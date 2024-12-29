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

void DownloadThread(void* p);
void DownloadThreadQueue(void* p);

#define USE_THREAD_QUEUE 1
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
    u64 offset{};
    FsFile f{};
    s64 file_offset{};
};

auto generate_key_from_path(const fs::FsPath& path) -> std::string {
    const auto key = crc32Calculate(path.s, path.size());
    return std::to_string(key);
}

struct CacheEntry {
    constexpr CacheEntry(const fs::FsPath& _path, const char* _header_key)
    : json_path{_path}
    , header_key{_header_key} {

    }

    bool init() {
        if (m_json) {
            return true;
        }

        // enable for testing etag is working.
        // fs::FsNativeSd().DeleteFile(json_path);

        auto json_in = yyjson_read_file(json_path, YYJSON_READ_NOFLAG, nullptr, nullptr);
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
        if (!m_json) {
            return;
        }

        if (!yyjson_mut_write_file(json_path, m_json, YYJSON_WRITE_NOFLAG, nullptr, nullptr)) {
            log_write("failed to write etag json: %s\n", json_path.s);
        }

        yyjson_mut_doc_free(m_json);
        m_json = nullptr;
        m_root = nullptr;
    }

    void set_internal(const fs::FsPath& path, const std::string& value) {
        const auto kkey = generate_key_from_path(path);

        // check if we already have this entry
        const auto it = m_cache.find(kkey);
        if (it != m_cache.end() && it->second == value) {
            log_write("already has etag, not updating, path: %s key: %s value: %s\n", path.s, kkey.c_str(), value.c_str());
            return;
        }

        if (it != m_cache.end()) {
            log_write("updating etag, path: %s old: %s new: %s\n", path.s, it->first.c_str(), it->second.c_str(), value.c_str());
        } else {
            log_write("setting new etag, path: %s key: %s value: %s\n", path.s, kkey.c_str(), value.c_str());
        }

        // insert new entry into cache, this will never fail.
        const auto& [jkey, jvalue] = *m_cache.insert_or_assign(it, kkey, value);

        // check if we need to add a new entry to root or simply update the value.
        auto etag_key = yyjson_mut_obj_getn(m_root, kkey.c_str(), kkey.length());
        if (!etag_key) {
            if (!yyjson_mut_obj_add_str(m_json, m_root, jkey.c_str(), jvalue.c_str())) {
                log_write("failed to set new etag key: %s\n", jkey.c_str());
            }
        } else {
            if (!yyjson_mut_set_strn(etag_key, jvalue.c_str(), jvalue.length())) {
                log_write("failed to update etag key: %s\n", jkey.c_str());
            }
        }
    }

    auto get(const fs::FsPath& path) -> std::string {
        if (!fs::FsNativeSd().FileExists(path)) {
            return {};
        }

        const auto kkey = generate_key_from_path(path);
        const auto it = m_cache.find(kkey);
        if (it != m_cache.end()) {
            return it->second;
        }

        auto etag_key = yyjson_mut_obj_getn(m_root, kkey.c_str(), kkey.length());
        R_UNLESS(etag_key, {});

        const auto val = yyjson_mut_get_str(etag_key);
        const auto val_len = yyjson_mut_get_len(etag_key);
        R_UNLESS(val && val_len, {});

        const std::string ret = {val, val_len};
        m_cache.insert_or_assign(it, kkey, ret);
        return ret;
    }

    void set(const fs::FsPath& path, const std::string& value) {
        set_internal(path, value);
    }

    void set(const fs::FsPath& path, const curl::Header& value) {
        if (auto it = value.Find(header_key); it != value.m_map.end()) {
            set_internal(path, it->second);
        }
    }

    const fs::FsPath json_path;
    const char* header_key;

    yyjson_mut_doc* m_json{};
    yyjson_mut_val* m_root{};
    std::unordered_map<std::string, std::string> m_cache{};
};

struct ThreadEntry {
    auto Create() -> Result {
        m_curl = curl_easy_init();
        R_UNLESS(m_curl != nullptr, 0x1);

        ueventCreate(&m_uevent, true);
        R_TRY(threadCreate(&m_thread, DownloadThread, this, nullptr, 1024*32, THREAD_PRIO, THREAD_CORE));
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
        R_TRY(threadCreate(&m_thread, DownloadThreadQueue, this, nullptr, 1024*32, THREAD_PRIO, THREAD_CORE));
        R_TRY(threadStart(&m_thread));
        R_SUCCEED();
    }

    void Close() {
        ueventSignal(&m_uevent);
        threadWaitForExit(&m_thread);
        threadClose(&m_thread);
    }

    auto Add(const Api& api) -> bool {
        mutexLock(&m_mutex);
        ON_SCOPE_EXIT(mutexUnlock(&m_mutex));

        ThreadQueueEntry entry{};
        entry.api = api;

        switch (api.m_prio) {
            case Priority::Normal:
                m_entries.emplace_back(entry);
                break;
            case Priority::High:
                m_entries.emplace_front(entry);
                break;
        }

        ueventSignal(&m_uevent);
        return true;
    }
};

ThreadEntry g_threads[MAX_THREADS]{};
ThreadQueue g_thread_queue;

CacheEntry g_etag{"/switch/sphaira/cache/etag.json", "etag"};
CacheEntry g_lmt{"/switch/sphaira/cache/lmt.json", "last-modified"};
Mutex g_cache_mutex;

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
    if (!g_running) {
        return 1;
    }

    // log_write("pcall called %u %u %u %u\n", dltotal, dlnow, ultotal, ulnow);
    auto callback = *static_cast<OnProgress*>(clientp);
    if (!callback(dltotal, dlnow, ultotal, ulnow)) {
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
    fs::FsPath tmp_buf;
    const bool has_file = !e.m_path.empty() && e.m_path != "";
    const bool has_post = !e.m_fields.m_str.empty() && e.m_fields.m_str != "";

    DataStruct chunk;
    Header header_out;
    fs::FsNativeSd fs;

    if (has_file) {
        GetDownloadTempPath(tmp_buf);
        fs.CreateDirectoryRecursivelyWithPath(tmp_buf, true);

        if (auto rc = fs.CreateFile(tmp_buf, 0, 0, true); R_FAILED(rc) && rc != FsError_PathAlreadyExists) {
            log_write("failed to create file: %s\n", tmp_buf);
            return {};
        }

        if (R_FAILED(fs.OpenFile(tmp_buf, FsOpenMode_Write|FsOpenMode_Append, &chunk.f))) {
            log_write("failed to open file: %s\n", tmp_buf);
            return {};
        }
    }

    // reserve the first chunk
    chunk.data.reserve(CHUNK_SIZE);

    curl_easy_reset(curl);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_URL, e.m_url.m_str.c_str());
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_USERAGENT, "TotalJustice");
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_FOLLOWLOCATION, 1L);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_FAILONERROR, 1L);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_SHARE, g_curl_share);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_BUFFERSIZE, 1024*512);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_HEADERFUNCTION, header_callback);
    CURL_EASY_SETOPT_LOG(curl, CURLOPT_HEADERDATA, &header_out);

    if (has_post) {
        CURL_EASY_SETOPT_LOG(curl, CURLOPT_POSTFIELDS, e.m_fields.m_str.c_str());
        log_write("setting post field: %s\n", e.m_fields.m_str.c_str());
    }

    struct curl_slist* list = NULL;
    ON_SCOPE_EXIT(if (list) { curl_slist_free_all(list); } );

    for (auto& [key, value] : e.m_header.m_map) {
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
    if (e.m_on_progress) {
        CURL_EASY_SETOPT_LOG(curl, CURLOPT_XFERINFODATA, &e.m_on_progress);
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
        ON_SCOPE_EXIT( fs.DeleteFile(tmp_buf, true) );
        if (res == CURLE_OK && chunk.offset) {
            fsFileWrite(&chunk.f, chunk.file_offset, chunk.data.data(), chunk.offset, FsWriteOption_None);
        }

        fsFileClose(&chunk.f);

        if (res == CURLE_OK && http_code != 304) {
            fs.DeleteFile(e.m_path, true);
            fs.CreateDirectoryRecursivelyWithPath(e.m_path, true);
            if (R_FAILED(fs.RenameFile(tmp_buf, e.m_path, true))) {
                success = false;
            }
        }
        chunk.data.clear();
    } else {
        // empty data if we failed
        if (res != CURLE_OK) {
            chunk.data.clear();
        }
    }

    log_write("Downloaded %s %s\n", e.m_url.m_str.c_str(), curl_easy_strerror(res));
    return {success, http_code, header_out, chunk.data, e.m_path};
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

void DownloadThread(void* p) {
    auto data = static_cast<ThreadEntry*>(p);
    while (g_running) {
        auto rc = waitSingle(waiterForUEvent(&data->m_uevent), UINT64_MAX);
        // log_write("woke up\n");
        if (!g_running) {
            return;
        }
        if (R_FAILED(rc)) {
            continue;
        }

        #if 1
        const auto result = DownloadInternal(data->m_curl, data->m_api);
        if (g_running) {
            const DownloadEventData event_data{data->m_api.m_on_complete, result};
            evman::push(std::move(event_data), false);
        } else {
            break;
        }
        #endif
        // mutexLock(&data->m_mutex);
        // ON_SCOPE_EXIT(mutexUnlock(&data->m_mutex));

        data->m_in_progress = false;
        // notify the queue that there's a space free
        ueventSignal(&g_thread_queue.m_uevent);
    }
    log_write("exited download thread\n");
}

void DownloadThreadQueue(void* p) {
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

            if (!g_running) {
                return;
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

void my_lock(CURL *handle, curl_lock_data data, curl_lock_access laccess, void *useptr) {
    mutexLock(&g_mutex_share[data]);
}

void my_unlock(CURL *handle, curl_lock_data data, void *useptr) {
    mutexUnlock(&g_mutex_share[data]);
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
}

auto ToMemory(const Api& e) -> ApiResult {
    if (!e.m_path.empty()) {
        return {};
    }
    return DownloadInternal(e);
}

auto ToFile(const Api& e) -> ApiResult {
    if (e.m_path.empty()) {
        return {};
    }
    return DownloadInternal(e);
}

auto ToMemoryAsync(const Api& api) -> bool {
    #if USE_THREAD_QUEUE
    return g_thread_queue.Add(api);
    #else
    // mutexLock(&g_thread_queue.m_mutex);
    // ON_SCOPE_EXIT(mutexUnlock(&g_thread_queue.m_mutex));

    for (auto& entry : g_threads) {
        if (!entry.InProgress()) {
            return entry.Setup(callback, url);
        }
    }

    log_write("failed to start download, no avaliable threads\n");
    return false;
    #endif
}

auto ToFileAsync(const Api& e) -> bool {
    #if USE_THREAD_QUEUE
    return g_thread_queue.Add(e);
    #else
    // mutexLock(&g_thread_queue.m_mutex);
    // ON_SCOPE_EXIT(mutexUnlock(&g_thread_queue.m_mutex));

    for (auto& entry : g_threads) {
        if (!entry.InProgress()) {
            return entry.Setup(callback, url, out);
        }
    }

    log_write("failed to start download, no avaliable threads\n");
    return false;
    #endif
}

namespace cache {

bool init() {
    mutexLock(&g_cache_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&g_cache_mutex));

    if (!g_etag.m_json) {
        R_UNLESS(g_etag.init(), false);
    }

    if (!g_lmt.m_json) {
        R_UNLESS(g_lmt.init(), false);
    }

    return true;
}

void exit() {
    mutexLock(&g_cache_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&g_cache_mutex));

    g_etag.exit();
    g_lmt.exit();
}

auto etag_get(const fs::FsPath& path) -> std::string {
    mutexLock(&g_cache_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&g_cache_mutex));
    return g_etag.get(path);
}

void etag_set(const fs::FsPath& path, const std::string& value) {
    mutexLock(&g_cache_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&g_cache_mutex));
    g_etag.set(path, value);
}

void etag_set(const fs::FsPath& path, const Header& value) {
    mutexLock(&g_cache_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&g_cache_mutex));
    g_etag.set(path, value);
}

auto lmt_get(const fs::FsPath& path) -> std::string {
    mutexLock(&g_cache_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&g_cache_mutex));
    return g_lmt.get(path);
}

void lmt_set(const fs::FsPath& path, const std::string& value) {
    mutexLock(&g_cache_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&g_cache_mutex));
    g_lmt.set(path, value);
}

void lmt_set(const fs::FsPath& path, const Header& value) {
    mutexLock(&g_cache_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&g_cache_mutex));
    g_lmt.set(path, value);
}

} // namespace cache
} // namespace sphaira::curl
