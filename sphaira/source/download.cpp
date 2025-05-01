#include "download.hpp"
#include "log.hpp"
#include "defines.hpp"
#include "evman.hpp"
#include "fs.hpp"
#include <switch.h>
#include <cstring>
#include <cassert>
#include <vector>
#include <deque>
#include <mutex>
#include <curl/curl.h>
#include <yyjson.h>

namespace sphaira::curl {
namespace {

#define CURL_EASY_SETOPT_LOG(handle, opt, v) \
    if (auto r = curl_easy_setopt(handle, opt, v); r != CURLE_OK) { \
        log_write("curl_easy_setopt(%s, %s) msg: %s\n", #opt, #v, curl_easy_strerror(r)); \
    } \

#define CURL_SHARE_SETOPT_LOG(handle, opt, v) \
    if (auto r = curl_share_setopt(handle, opt, v); r != CURLSHE_OK) { \
        log_write("curl_share_setopt(%s, %s) msg: %s\n", #opt, #v, curl_share_strerror(r)); \
    } \

constexpr auto API_AGENT = "ITotalJustice";
constexpr u64 CHUNK_SIZE = 1024*1024;
constexpr auto MAX_THREADS = 4;
constexpr int THREAD_PRIO = 0x2C;
constexpr int THREAD_CORE = 1;

std::atomic_bool g_running{};
CURLSH* g_curl_share{};
Mutex g_mutex_share[CURL_LOCK_DATA_LAST]{};

struct DataStruct {
    std::vector<u8> data;
    s64 offset{};
    FsFile f{};
    s64 file_offset{};
};

auto generate_key_from_path(const fs::FsPath& path) -> std::string {
    const auto key = crc32Calculate(path.s, path.size());
    return std::to_string(key);
}

struct Cache {
    using Value = std::pair<std::string, std::string>;

    bool init() {
        mutexLock(&m_mutex);
        ON_SCOPE_EXIT(mutexUnlock(&m_mutex));

        if (m_json) {
            return true;
        }

        auto json_in = yyjson_read_file(JSON_PATH, YYJSON_READ_NOFLAG, nullptr, nullptr);
        if (json_in) {
            log_write("loading old json doc\n");
            m_json = yyjson_doc_mut_copy(json_in, nullptr);
            yyjson_doc_free(json_in);
            m_root = yyjson_mut_doc_get_root(m_json);
        } else {
            log_write("creating new json doc\n");
            m_json = yyjson_mut_doc_new(nullptr);
            m_root = yyjson_mut_obj(m_json);
            yyjson_mut_doc_set_root(m_json, m_root);
        }

        return m_json && m_root;
    }

    void exit() {
        mutexLock(&m_mutex);
        ON_SCOPE_EXIT(mutexUnlock(&m_mutex));

        if (!m_json) {
            return;
        }

        if (!yyjson_mut_write_file(JSON_PATH, m_json, YYJSON_WRITE_NOFLAG, nullptr, nullptr)) {
            log_write("failed to write etag json: %s\n", JSON_PATH.s);
        }

        yyjson_mut_doc_free(m_json);
        m_json = nullptr;
        m_root = nullptr;
    }

    void get(const fs::FsPath& path, curl::Header& header) {
        const auto [etag, last_modified] = get_internal(path);
        if (!etag.empty()) {
            header.m_map.emplace("if-none-match", etag);
        }

        if (!last_modified.empty()) {
            header.m_map.emplace("if-modified-since", last_modified);
        }
    }

    void set(const fs::FsPath& path, const curl::Header& value) {
        mutexLock(&m_mutex);
        ON_SCOPE_EXIT(mutexUnlock(&m_mutex));

        std::string etag_str;
        std::string last_modified_str;

        if (auto it = value.Find(ETAG_STR); it != value.m_map.end()) {
            etag_str = it->second;
        }
        if (auto it = value.Find(LAST_MODIFIED_STR); it != value.m_map.end()) {
            last_modified_str = it->second;
        }

        if (!etag_str.empty() || !last_modified_str.empty()) {
            set_internal(path, Value{etag_str, last_modified_str});
        }
    }

private:
    auto get_internal(const fs::FsPath& path) -> Value {
        if (!fs::FsNativeSd().FileExists(path)) {
            return {};
        }

        const auto kkey = generate_key_from_path(path);
        const auto it = m_cache.find(kkey);
        if (it != m_cache.end()) {
            return it->second;
        }

        auto hash_key = yyjson_mut_obj_getn(m_root, kkey.c_str(), kkey.length());
        if (!hash_key) {
            return {};
        }

        auto etag_key = yyjson_mut_obj_get(hash_key, ETAG_STR);
        auto last_modified_key = yyjson_mut_obj_get(hash_key, LAST_MODIFIED_STR);

        const auto etag_value = yyjson_mut_get_str(etag_key);
        const auto etag_value_len = yyjson_mut_get_len(etag_key);
        const auto last_modified_value = yyjson_mut_get_str(last_modified_key);
        const auto last_modified_value_len = yyjson_mut_get_len(last_modified_key);

        if ((!etag_value || !etag_value_len) && (!last_modified_value || !last_modified_value_len)) {
            return {};
        }

        std::string etag;
        std::string last_modified;
        if (etag_value && etag_value_len) {
            etag.assign(etag_value, etag_value_len);
        }
        if (last_modified_value && last_modified_value_len) {
            last_modified.assign(last_modified_value, last_modified_value_len);
        }

        const Value ret{etag, last_modified};
        m_cache.insert_or_assign(it, kkey, ret);
        return ret;
    }

    void set_internal(const fs::FsPath& path, const Value& value) {
        const auto kkey = generate_key_from_path(path);

        // check if we already have this entry
        const auto it = m_cache.find(kkey);
        if (it != m_cache.end() && it->second == value) {
            log_write("already has etag, not updating, path: %s key: %s\n", path.s, kkey.c_str());
            return;
        }

        if (it != m_cache.end()) {
            log_write("updating etag, path: %s key: %s\n", path.s, kkey.c_str());
        } else {
            log_write("setting new etag, path: %s key: %s\n", path.s, kkey.c_str());
        }

        // insert new entry into cache, this will never fail.
        const auto& [jkey, jvalue] = *m_cache.insert_or_assign(it, kkey, value);
        const auto& [etag, last_modified] = jvalue;

        // check if we need to add a new entry to root or simply update the value.
        auto hash_key = yyjson_mut_obj_getn(m_root, kkey.c_str(), kkey.length());
        if (!hash_key) {
            hash_key = yyjson_mut_obj_add_obj(m_json, m_root, jkey.c_str());
        }

        if (!hash_key) {
            log_write("failed to set new cache key obj, path: %s key: %s\n", path.s, jkey.c_str());
        } else {
            const auto update_entry = [this, &hash_key](const char* tag, const std::string& value) {
                if (value.empty()) {
                    // workaround for appstore accepting etags but not returning them.
                    yyjson_mut_obj_remove_str(hash_key, tag);
                    return true;
                } else {
                    auto key = yyjson_mut_obj_get(hash_key, tag);
                    if (!key) {
                        return yyjson_mut_obj_add_str(m_json, hash_key, tag, value.c_str());
                    } else {
                        return yyjson_mut_set_str(key, value.c_str());
                    }
                }
            };

            if (!update_entry("etag", etag)) {
                log_write("failed to set new etag, path: %s key: %s\n", path.s, jkey.c_str());
            }

            if (!update_entry("last-modified", last_modified)) {
                log_write("failed to set new last-modified, path: %s key: %s\n", path.s, jkey.c_str());
            }
        }
    }

    static constexpr inline fs::FsPath JSON_PATH{"/switch/sphaira/cache/cache.json"};
    static constexpr inline const char* ETAG_STR{"etag"};
    static constexpr inline const char* LAST_MODIFIED_STR{"last-modified"};

    Mutex m_mutex{};
    yyjson_mut_doc* m_json{};
    yyjson_mut_val* m_root{};
    std::unordered_map<std::string, Value> m_cache{};
};

struct ThreadEntry {
    auto Create() -> Result {
        m_curl = curl_easy_init();
        R_UNLESS(m_curl != nullptr, 0x1);

        ueventCreate(&m_uevent, true);
        R_TRY(threadCreate(&m_thread, ThreadFunc, this, nullptr, 1024*32, THREAD_PRIO, THREAD_CORE));
        R_TRY(threadStart(&m_thread));
        R_SUCCEED();
    }

    void Close() {
        ueventSignal(&m_uevent);
        threadWaitForExit(&m_thread);
        threadClose(&m_thread);
        if (m_curl) {
            curl_easy_cleanup(m_curl);
            m_curl = nullptr;
        }
    }

    auto InProgress() -> bool {
        return m_in_progress == true;
    }

    auto Setup(const Api& api) -> bool {
        assert(m_in_progress == false && "Setting up thread while active");
        mutexLock(&m_mutex);
        ON_SCOPE_EXIT(mutexUnlock(&m_mutex));

        if (m_in_progress) {
            return false;
        }
        m_api = api;
        m_in_progress = true;
        // log_write("started download :)\n");
        ueventSignal(&m_uevent);
        return true;
    }

    static void ThreadFunc(void* p);

    CURL* m_curl{};
    Thread m_thread{};
    Api m_api{};
    std::atomic_bool m_in_progress{};
    Mutex m_mutex{};
    UEvent m_uevent{};
};

struct ThreadQueueEntry {
    Api api;
    bool m_delete{};
};

struct ThreadQueue {
    std::deque<ThreadQueueEntry> m_entries;
    Thread m_thread;
    Mutex m_mutex{};
    UEvent m_uevent{};

    auto Create() -> Result {
        ueventCreate(&m_uevent, true);
        R_TRY(threadCreate(&m_thread, ThreadFunc, this, nullptr, 1024*32, THREAD_PRIO, THREAD_CORE));
        R_TRY(threadStart(&m_thread));
        R_SUCCEED();
    }

    void Close() {
        ueventSignal(&m_uevent);
        threadWaitForExit(&m_thread);
        threadClose(&m_thread);
    }

    auto Add(const Api& api) -> bool {
        if (api.GetUrl().empty() || api.GetPath().empty() || !api.GetOnComplete()) {
            return false;
        }

        mutexLock(&m_mutex);
        ON_SCOPE_EXIT(mutexUnlock(&m_mutex));

        switch (api.GetPriority()) {
            case Priority::Normal:
                m_entries.emplace_back(api);
                break;
            case Priority::High:
                m_entries.emplace_front(api);
                break;
        }

        ueventSignal(&m_uevent);
        return true;
    }

    static void ThreadFunc(void* p);
};

ThreadEntry g_threads[MAX_THREADS]{};
ThreadQueue g_thread_queue;
Cache g_cache;

void GetDownloadTempPath(fs::FsPath& buf) {
    static Mutex mutex{};
    static u64 count{};

    mutexLock(&mutex);
    const auto count_copy = count;
    count++;
    mutexUnlock(&mutex);

    std::snprintf(buf, sizeof(buf), "/switch/sphaira/cache/download_temp%lu", count_copy);
}

auto ProgressCallbackFunc1(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow) -> size_t {
    if (!g_running) {
        return 1;
    }

    svcSleepThread(YieldType_WithoutCoreMigration);
    return 0;
}

auto ProgressCallbackFunc2(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow) -> size_t {
    auto api = static_cast<Api*>(clientp);
    if (!g_running || api->GetToken().stop_requested()) {
        return 1;
    }

    // log_write("pcall called %u %u %u %u\n", dltotal, dlnow, ultotal, ulnow);
    if (!api->GetOnProgress()(dltotal, dlnow, ultotal, ulnow)) {
        return 1;
    }

    svcSleepThread(YieldType_WithoutCoreMigration);
    return 0;
}

auto WriteMemoryCallback(void *contents, size_t size, size_t num_files, void *userp) -> size_t {
    if (!g_running) {
        return 0;
    }

    auto data_struct = static_cast<DataStruct*>(userp);
    const auto realsize = size * num_files;

    // give it more memory
    if (data_struct->data.capacity() < data_struct->offset + realsize) {
        data_struct->data.reserve(data_struct->data.capacity() + CHUNK_SIZE);
    }

    data_struct->data.resize(data_struct->offset + realsize);
    std::memcpy(data_struct->data.data() + data_struct->offset, contents, realsize);

    data_struct->offset += realsize;

    svcSleepThread(YieldType_WithoutCoreMigration);

    return realsize;
}

auto WriteFileCallback(void *contents, size_t size, size_t num_files, void *userp) -> size_t {
    if (!g_running) {
        return 0;
    }

    auto data_struct = static_cast<DataStruct*>(userp);
    const auto realsize = size * num_files;

    // flush data if incomming data would overflow the buffer
    if (data_struct->offset && data_struct->data.size() < data_struct->offset + realsize) {
        if (R_FAILED(fsFileWrite(&data_struct->f, data_struct->file_offset, data_struct->data.data(), data_struct->offset, FsWriteOption_None))) {
            return 0;
        }

        data_struct->file_offset += data_struct->offset;
        data_struct->offset = 0;
    }

    // we have a huge chunk! write it directly to file
    if (data_struct->data.size() < realsize) {
        if (R_FAILED(fsFileWrite(&data_struct->f, data_struct->file_offset, contents, realsize, FsWriteOption_None))) {
            return 0;
        }

        data_struct->file_offset += realsize;
    } else {
        // buffer data until later
        std::memcpy(data_struct->data.data() + data_struct->offset, contents, realsize);
        data_struct->offset += realsize;
    }

    svcSleepThread(YieldType_WithoutCoreMigration);
    return realsize;
}

auto header_callback(char* b, size_t size, size_t nitems, void* userdata) -> size_t {
    auto header = static_cast<Header*>(userdata);
    const auto numbytes = size * nitems;

    if (b && numbytes) {
        const auto dilem = (const char*)memchr(b, ':', numbytes);
        if (dilem) {
            const int key_len = dilem - b;
            const int value_len = numbytes - key_len - 4; // "\r\n"
            if (key_len > 0 && value_len > 0) {
                const std::string key(b, key_len);
                const std::string value(dilem + 2, value_len);
                header->m_map.insert_or_assign(key, value);
            }
        }
    }

    return numbytes;
}

auto DownloadInternal(CURL* curl, const Api& e) -> ApiResult {
    // check if stop has been requested before starting download
    if (e.GetToken().stop_requested()) {
        return {};
    }

    fs::FsPath tmp_buf;
    const bool has_file = !e.GetPath().empty() && e.GetPath() != "";
    const bool has_post = !e.GetFields().empty() && e.GetFields() != "";

    DataStruct chunk;
    Header header_in = e.GetHeader();
    Header header_out;
    fs::FsNativeSd fs;

    if (has_file) {
        GetDownloadTempPath(tmp_buf);
        fs.CreateDirectoryRecursivelyWithPath(tmp_buf);

        if (auto rc = fs.CreateFile(tmp_buf, 0, 0); R_FAILED(rc) && rc != FsError_PathAlreadyExists) {
            log_write("failed to create file: %s\n", tmp_buf.s);
            return {};
        }

        if (R_FAILED(fs.OpenFile(tmp_buf, FsOpenMode_Write|FsOpenMode_Append, &chunk.f))) {
            log_write("failed to open file: %s\n", tmp_buf.s);
            return {};
        }

        // only add etag if the dst file still exists.
        if ((e.GetFlags() & Flag_Cache) && fs::FileExists(&fs.m_fs, e.GetPath())) {
            g_cache.get(e.GetPath(), header_in);
        }
    }

    // reserve the first chunk
    chunk.data.reserve(CHUNK_SIZE);

    curl_easy_reset(curl);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_URL, e.GetUrl().c_str());
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_USERAGENT, "TotalJustice");
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_FOLLOWLOCATION, 1L);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_FAILONERROR, 1L);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_SHARE, g_curl_share);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_BUFFERSIZE, 1024*512);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_HEADERFUNCTION, header_callback);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_HEADERDATA, &header_out);
    // enable all forms of compression supported by libcurl.
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_ACCEPT_ENCODING, "");

    if (has_post) {
        CURL_EASY_SETOPT_LOG(curl, CURLOPT_POSTFIELDS, e.GetFields().c_str());
        log_write("setting post field: %s\n", e.GetFields().c_str());
    }

    struct curl_slist* list = NULL;
    ON_SCOPE_EXIT(if (list) { curl_slist_free_all(list); } );

    for (const auto& [key, value] : header_in.m_map) {
        if (value.empty()) {
            continue;
        }

        // create header key value pair.
        const auto header_str = key + ": " + value;

        // try to append header chunk.
        auto temp = curl_slist_append(list, header_str.c_str());
        if (temp) {
            log_write("adding header: %s\n", header_str.c_str());
            list = temp;
        } else {
            log_write("failed to append header\n");
        }
    }

    if (list) {
        CURL_EASY_SETOPT_LOG(curl, CURLOPT_HTTPHEADER, list);
    }

    // progress calls.
    if (e.GetOnProgress()) {
        CURL_EASY_SETOPT_LOG(curl, CURLOPT_XFERINFODATA, &e);
        CURL_EASY_SETOPT_LOG(curl, CURLOPT_XFERINFOFUNCTION, ProgressCallbackFunc2);
    } else {
        CURL_EASY_SETOPT_LOG(curl, CURLOPT_XFERINFOFUNCTION, ProgressCallbackFunc1);
    }
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_NOPROGRESS, 0L);

    // write calls.
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_WRITEFUNCTION, has_file ? WriteFileCallback : WriteMemoryCallback);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_WRITEDATA, &chunk);

    // perform download and cleanup after and report the result.
    const auto res = curl_easy_perform(curl);
    bool success = res == CURLE_OK;

    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    if (has_file) {
        ON_SCOPE_EXIT( fs.DeleteFile(tmp_buf) );
        if (res == CURLE_OK && chunk.offset) {
            fsFileWrite(&chunk.f, chunk.file_offset, chunk.data.data(), chunk.offset, FsWriteOption_None);
        }

        fsFileClose(&chunk.f);

        if (res == CURLE_OK) {
            if (http_code == 304) {
                log_write("cached download: %s\n", e.GetUrl().c_str());
            } else {
                log_write("un-cached download: %s code: %lu\n", e.GetUrl().c_str(), http_code);
                if (e.GetFlags() & Flag_Cache) {
                    g_cache.set(e.GetPath(), header_out);
                }

                // enable to log received headers.
                #if 0
                log_write("\n\nLOGGING HEADER\n");
                    for (auto [a, b] : header_out.m_map) {
                        log_write("\t%s: %s\n", a.c_str(), b.c_str());
                    }
                log_write("\n\n");
                #endif

                fs.DeleteFile(e.GetPath());
                fs.CreateDirectoryRecursivelyWithPath(e.GetPath());
                if (R_FAILED(fs.RenameFile(tmp_buf, e.GetPath()))) {
                    success = false;
                }
            }
        }
        chunk.data.clear();
    } else {
        // empty data if we failed
        if (res != CURLE_OK) {
            chunk.data.clear();
        }
    }

    log_write("Downloaded %s %s\n", e.GetUrl().c_str(), curl_easy_strerror(res));
    return {success, http_code, header_out, chunk.data, e.GetPath()};
}

auto DownloadInternal(const Api& e) -> ApiResult {
    auto curl = curl_easy_init();
    if (!curl) {
        log_write("curl init failed\n");
        return {};
    }
    ON_SCOPE_EXIT(curl_easy_cleanup(curl));
    return DownloadInternal(curl, e);
}

void my_lock(CURL *handle, curl_lock_data data, curl_lock_access laccess, void *useptr) {
    mutexLock(&g_mutex_share[data]);
}

void my_unlock(CURL *handle, curl_lock_data data, void *useptr) {
    mutexUnlock(&g_mutex_share[data]);
}

void ThreadEntry::ThreadFunc(void* p) {
    auto data = static_cast<ThreadEntry*>(p);
    while (g_running) {
        auto rc = waitSingle(waiterForUEvent(&data->m_uevent), UINT64_MAX);
        // log_write("woke up\n");
        if (!g_running) {
            break;
        }

        if (R_FAILED(rc)) {
            continue;
        }

        const auto result = DownloadInternal(data->m_curl, data->m_api);
        if (g_running && data->m_api.GetOnComplete() && !data->m_api.GetToken().stop_requested()) {
            const DownloadEventData event_data{data->m_api.GetOnComplete(), result, data->m_api.GetToken()};
            evman::push(std::move(event_data), false);
        }

        data->m_in_progress = false;
        // notify the queue that there's a space free
        ueventSignal(&g_thread_queue.m_uevent);
    }
    log_write("exited download thread\n");
}

void ThreadQueue::ThreadFunc(void* p) {
    auto data = static_cast<ThreadQueue*>(p);
    while (g_running) {
        auto rc = waitSingle(waiterForUEvent(&data->m_uevent), UINT64_MAX);
        log_write("[thread queue] woke up\n");
        if (!g_running) {
            return;
        }
        if (R_FAILED(rc)) {
            continue;
        }

        mutexLock(&data->m_mutex);
        ON_SCOPE_EXIT(mutexUnlock(&data->m_mutex));
        if (data->m_entries.empty()) {
            continue;
        }

        // find the next avaliable thread
        u32 pop_count{};
        for (auto& entry : data->m_entries) {
            if (!g_running) {
                return;
            }

            bool keep_going{};

            for (auto& thread : g_threads) {
                if (!g_running) {
                    return;
                }

                if (!thread.InProgress()) {
                    thread.Setup(entry.api);
                    // log_write("[dl queue] starting download\n");
                    // mark entry for deletion
                    entry.m_delete = true;
                    pop_count++;
                    keep_going = true;
                    break;
                }
            }

            if (!keep_going) {
                break;
            }
        }

        // delete all entries marked for deletion
        for (u32 i = 0; i < pop_count; i++) {
            data->m_entries.pop_front();
        }
    }

    log_write("exited download thread queue\n");
}

} // namespace

auto Init() -> bool {
    if (CURLE_OK != curl_global_init(CURL_GLOBAL_DEFAULT)) {
        return false;
    }

    g_curl_share = curl_share_init();
    if (g_curl_share) {
        CURL_SHARE_SETOPT_LOG(g_curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_COOKIE);
        CURL_SHARE_SETOPT_LOG(g_curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
        CURL_SHARE_SETOPT_LOG(g_curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_SSL_SESSION);
        CURL_SHARE_SETOPT_LOG(g_curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_CONNECT);
        CURL_SHARE_SETOPT_LOG(g_curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_PSL);
        CURL_SHARE_SETOPT_LOG(g_curl_share, CURLSHOPT_LOCKFUNC, my_lock);
        CURL_SHARE_SETOPT_LOG(g_curl_share, CURLSHOPT_UNLOCKFUNC, my_unlock);
    }

    g_running = true;

    if (R_FAILED(g_thread_queue.Create())) {
        log_write("!failed to create download thread queue\n");
    }

    for (auto& entry : g_threads) {
        if (R_FAILED(entry.Create())) {
            log_write("!failed to create download thread\n");
        }
    }

    log_write("finished creating threads\n");

    if (!g_cache.init()) {
        log_write("failed to init json cache\n");
    }

    return true;
}

void Exit() {
    g_running = false;

    g_thread_queue.Close();

    for (auto& entry : g_threads) {
        entry.Close();
    }

    if (g_curl_share) {
        curl_share_cleanup(g_curl_share);
        g_curl_share = {};
    }

    curl_global_cleanup();
    g_cache.exit();
}

auto ToMemory(const Api& e) -> ApiResult {
    if (!e.GetPath().empty()) {
        return {};
    }
    return DownloadInternal(e);
}

auto ToFile(const Api& e) -> ApiResult {
    if (e.GetPath().empty()) {
        return {};
    }
    return DownloadInternal(e);
}

auto ToMemoryAsync(const Api& api) -> bool {
    return g_thread_queue.Add(api);
}

auto ToFileAsync(const Api& e) -> bool {
    return g_thread_queue.Add(e);
}

auto EscapeString(const std::string& str) -> std::string {
    std::string result;
    const auto s = curl_escape(str.data(), str.length());
    if (s) {
        result = s;
        curl_free(s);
    }
    return result;
}

} // namespace sphaira::curl
