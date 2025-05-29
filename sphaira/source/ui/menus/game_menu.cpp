#include "app.hpp"
#include "log.hpp"
#include "fs.hpp"
#include "dumper.hpp"
#include "defines.hpp"
#include "i18n.hpp"

#include "ui/menus/game_menu.hpp"
#include "ui/sidebar.hpp"
#include "ui/error_box.hpp"
#include "ui/option_box.hpp"
#include "ui/progress_box.hpp"
#include "ui/popup_list.hpp"
#include "ui/nvg_util.hpp"

#include "yati/nx/ncm.hpp"
#include "yati/nx/nca.hpp"
#include "yati/nx/es.hpp"
#include "yati/container/base.hpp"
#include "yati/container/nsp.hpp"

#include <utility>
#include <cstring>
#include <algorithm>
#include <minIni.h>

namespace sphaira::ui::menu::game {
namespace {

constexpr int THREAD_PRIO = PRIO_PREEMPTIVE;
constexpr int THREAD_CORE = 1;

constexpr u32 ContentMetaTypeToContentFlag(u8 meta_type) {
    if (meta_type & 0x80) {
        return 1 << (meta_type - 0x80);
    }

    return 0;
}

enum ContentFlag {
    ContentFlag_Application = ContentMetaTypeToContentFlag(NcmContentMetaType_Application),
    ContentFlag_Patch = ContentMetaTypeToContentFlag(NcmContentMetaType_Patch),
    ContentFlag_AddOnContent = ContentMetaTypeToContentFlag(NcmContentMetaType_AddOnContent),
    ContentFlag_DataPatch = ContentMetaTypeToContentFlag(NcmContentMetaType_DataPatch),
    ContentFlag_All = ContentFlag_Application | ContentFlag_Patch | ContentFlag_AddOnContent | ContentFlag_DataPatch,
};

struct NcmEntry {
    const NcmStorageId storage_id;
    NcmContentStorage cs{};
    NcmContentMetaDatabase db{};

    void Open() {
        if (R_FAILED(ncmOpenContentMetaDatabase(std::addressof(db), storage_id))) {
            log_write("\tncmOpenContentMetaDatabase() failed. storage_id: %u\n", storage_id);
        } else {
            log_write("\tncmOpenContentMetaDatabase() success. storage_id: %u\n", storage_id);
        }

        if (R_FAILED(ncmOpenContentStorage(std::addressof(cs), storage_id))) {
            log_write("\tncmOpenContentStorage() failed. storage_id: %u\n", storage_id);
        } else {
            log_write("\tncmOpenContentStorage() success. storage_id: %u\n", storage_id);
        }
    }

    void Close() {
        ncmContentMetaDatabaseClose(std::addressof(db));
        ncmContentStorageClose(std::addressof(cs));

        db = {};
        cs = {};
    }
};

constinit NcmEntry ncm_entries[] = {
    // on memory, will become invalid on the gamecard being inserted / removed.
    { NcmStorageId_GameCard },
    // normal (save), will remain valid.
    { NcmStorageId_BuiltInUser },
    { NcmStorageId_SdCard },
};

auto& GetNcmEntry(u8 storage_id) {
    auto it = std::ranges::find_if(ncm_entries, [storage_id](auto& e){
        return storage_id == e.storage_id;
    });

    if (it == std::end(ncm_entries)) {
        log_write("unable to find valid ncm entry: %u\n", storage_id);
        return ncm_entries[0];
    }

    return *it;
}

auto& GetNcmCs(u8 storage_id) {
    return GetNcmEntry(storage_id).cs;
}

auto& GetNcmDb(u8 storage_id) {
    return GetNcmEntry(storage_id).db;
}

using MetaEntries = std::vector<NsApplicationContentMetaStatus>;

struct ContentInfoEntry {
    NsApplicationContentMetaStatus status{};
    std::vector<NcmContentInfo> content_infos{};
    std::vector<FsRightsId> rights_ids{};
};

struct TikEntry {
    FsRightsId id{};
    std::vector<u8> tik_data{};
    std::vector<u8> cert_data{};
};

struct NspEntry {
    // application name.
    std::string application_name{};
    // name of the nsp (name [id][v0][BASE].nsp).
    fs::FsPath path{};
    // tickets and cert data, will be empty if title key crypto isn't used.
    std::vector<TikEntry> tickets{};
    // all the collections for this nsp, such as nca's and tickets.
    std::vector<yati::container::CollectionEntry> collections{};
    // raw nsp data (header, file table and string table).
    std::vector<u8> nsp_data{};
    // size of the entier nsp.
    s64 nsp_size{};
    // copy of ncm cs, it is not closed.
    NcmContentStorage cs{};
    // copy of the icon, if invalid, it will use the default icon.
    int icon{};

    // todo: benchmark manual sdcard read and decryption vs ncm.
    Result Read(void* buf, s64 off, s64 size, u64* bytes_read) {
        if (off < nsp_data.size()) {
            *bytes_read = size = ClipSize(off, size, nsp_data.size());
            std::memcpy(buf, nsp_data.data() + off, size);
            R_SUCCEED();
        }

        // adjust offset.
        off -= nsp_data.size();

        for (const auto& collection : collections) {
            if (InRange(off, collection.offset, collection.size)) {
                // adjust offset relative to the collection.
                off -= collection.offset;
                *bytes_read = size = ClipSize(off, size, collection.size);

                if (collection.name.ends_with(".nca")) {
                    const auto id = ncm::GetContentIdFromStr(collection.name.c_str());
                    return ncmContentStorageReadContentIdFile(&cs, buf, size, &id, off);
                } else if (collection.name.ends_with(".tik") || collection.name.ends_with(".cert")) {
                    FsRightsId id;
                    keys::parse_hex_key(&id, collection.name.c_str());

                    const auto it = std::ranges::find_if(tickets, [&id](auto& e){
                        return !std::memcmp(&id, &e.id, sizeof(id));
                    });
                    R_UNLESS(it != tickets.end(), 0x1);

                    const auto& data = collection.name.ends_with(".tik") ? it->tik_data : it->cert_data;
                    std::memcpy(buf, data.data() + off, size);
                    R_SUCCEED();
                }
            }
        }

        log_write("did not find collection...\n");
        return 0x1;
    }

private:
    static auto InRange(s64 off, s64 offset, s64 size) -> bool {
        return off < offset + size && off >= offset;
    }

    static auto ClipSize(s64 off, s64 size, s64 file_size) -> s64 {
        return std::min(size, file_size - off);
    }
};

struct NspSource final : dump::BaseSource {
    NspSource(const std::vector<NspEntry>& entries) : m_entries{entries} {
        m_is_file_based_emummc = App::IsFileBaseEmummc();
    }

    Result Read(const std::string& path, void* buf, s64 off, s64 size, u64* bytes_read) override {
        const auto it = std::ranges::find_if(m_entries, [&path](auto& e){
            return path.find(e.path.s) != path.npos;
        });
        R_UNLESS(it != m_entries.end(), 0x1);

        const auto rc = it->Read(buf, off, size, bytes_read);
        if (m_is_file_based_emummc) {
            svcSleepThread(2e+6); // 2ms
        }
        return rc;
    }

    auto GetName(const std::string& path) const -> std::string {
        const auto it = std::ranges::find_if(m_entries, [&path](auto& e){
            return path.find(e.path.s) != path.npos;
        });

        if (it != m_entries.end()) {
            return it->application_name;
        }

        return {};
    }

    auto GetSize(const std::string& path) const -> s64 {
        const auto it = std::ranges::find_if(m_entries, [&path](auto& e){
            return path.find(e.path.s) != path.npos;
        });

        if (it != m_entries.end()) {
            return it->nsp_size;
        }

        return 0;
    }

    auto GetIcon(const std::string& path) const -> int override {
        const auto it = std::ranges::find_if(m_entries, [&path](auto& e){
            return path.find(e.path.s) != path.npos;
        });

        if (it != m_entries.end()) {
            return it->icon;
        }

        return App::GetDefaultImage();
    }

private:
    std::vector<NspEntry> m_entries{};
    bool m_is_file_based_emummc{};
};

Result Notify(Result rc, const std::string& error_message) {
    if (R_FAILED(rc)) {
        App::Push(std::make_shared<ui::ErrorBox>(rc,
            i18n::get(error_message)
        ));
    } else {
        App::Notify("Success"_i18n);
    }

    return rc;
}

Result GetMetaEntries(u64 id, MetaEntries& out, u32 flags = ContentFlag_All) {
    for (s32 i = 0; ; i++) {
        s32 count;
        NsApplicationContentMetaStatus status;
        R_TRY(nsListApplicationContentMetaStatus(id, i, &status, 1, &count));

        if (!count) {
            break;
        }

        if (flags & ContentMetaTypeToContentFlag(status.meta_type)) {
            out.emplace_back(status);
        }
    }

    R_SUCCEED();
}

Result GetMetaEntries(const Entry& e, MetaEntries& out, u32 flags = ContentFlag_All) {
    return GetMetaEntries(e.app_id, out, flags);
}

// also sets the status to error.
void FakeNacpEntry(ThreadResultData& e) {
    e.status = NacpLoadStatus::Error;
    // fake the nacp entry
    std::strcpy(e.lang.name, "Corrupted");
    std::strcpy(e.lang.author, "Corrupted");
    std::strcpy(e.display_version, "0.0.0");
    e.control.reset();
}

bool LoadControlImage(Entry& e) {
    if (!e.image && e.control) {
        TimeStamp ts;
        const auto jpeg_size = e.control_size - sizeof(NacpStruct);
        e.image = nvgCreateImageMem(App::GetVg(), 0, e.control->icon, jpeg_size);
        e.control.reset();
        log_write("\t\t[image load] time taken: %.2fs %zums\n", ts.GetSecondsD(), ts.GetMs());
        return true;
    }

    return false;
}

Result LoadControlManual(u64 id, ThreadResultData& data) {
    TimeStamp ts;

    MetaEntries entries;
    R_TRY(GetMetaEntries(id, entries));
    R_UNLESS(!entries.empty(), 0x1);

    const auto& ee = entries.back();
    if (ee.storageID != NcmStorageId_SdCard && ee.storageID != NcmStorageId_BuiltInUser && ee.storageID != NcmStorageId_GameCard) {
        return 0x1;
    }

    auto& db = GetNcmDb(ee.storageID);
    auto& cs = GetNcmCs(ee.storageID);

    NcmContentMetaKey key;
    R_TRY(ncmContentMetaDatabaseGetLatestContentMetaKey(&db, &key, ee.application_id));

    NcmContentId content_id;
    R_TRY(ncmContentMetaDatabaseGetContentIdByType(&db, &content_id, &key, NcmContentType_Control));

    u64 program_id;
    R_TRY(ncmContentStorageGetProgramId(&cs, &program_id, &content_id, FsContentAttributes_All));

    fs::FsPath path;
    R_TRY(ncmContentStorageGetPath(&cs, path, sizeof(path), &content_id));

    std::vector<u8> icon;
    R_TRY(nca::ParseControl(path, program_id, &data.control->nacp, sizeof(data.control->nacp), &icon));
    std::memcpy(data.control->icon, icon.data(), icon.size());

    data.control_size = sizeof(data.control->nacp) + icon.size();
    log_write("\t\t[manual control] time taken: %.2fs %zums\n", ts.GetSecondsD(), ts.GetMs());

    R_SUCCEED();
}

auto LoadControlEntry(u64 id) -> ThreadResultData {
    ThreadResultData data{};
    data.id = id;
    data.control = std::make_shared<NsApplicationControlData>();
    data.status = NacpLoadStatus::Error;

    bool manual_load = true;
    if (hosversionBefore(20,0,0)) {
        TimeStamp ts;
        if (R_SUCCEEDED(nsGetApplicationControlData(NsApplicationControlSource_CacheOnly, id, data.control.get(), sizeof(NsApplicationControlData), &data.control_size))) {
            manual_load = false;
            log_write("\t\t[ns control cache] time taken: %.2fs %zums\n", ts.GetSecondsD(), ts.GetMs());
        }
    }

    if (manual_load) {
        manual_load = R_SUCCEEDED(LoadControlManual(id, data));
    }

    Result rc{};
    if (!manual_load) {
        TimeStamp ts;
        rc = nsGetApplicationControlData(NsApplicationControlSource_Storage, id, data.control.get(), sizeof(NsApplicationControlData), &data.control_size);
        log_write("\t\t[ns control storage] time taken: %.2fs %zums\n", ts.GetSecondsD(), ts.GetMs());
    }

    if (R_SUCCEEDED(rc)) {
        NacpLanguageEntry* lang{};
        if (R_SUCCEEDED(rc = nsGetApplicationDesiredLanguage(&data.control->nacp, &lang)) && lang) {
            data.lang = *lang;
            std::memcpy(data.display_version, data.control->nacp.display_version, sizeof(data.display_version));
            data.status = NacpLoadStatus::Loaded;
        }
    }

    if (R_FAILED(rc)) {
        FakeNacpEntry(data);
    }

    return data;
}

void LoadResultIntoEntry(Entry& e, const ThreadResultData& result) {
    e.status = result.status;
    e.control = result.control;
    e.control_size = result.control_size;
    std::memcpy(e.display_version, result.display_version, sizeof(result.display_version));
    e.lang = result.lang;
    e.status = result.status;
}

void LoadControlEntry(Entry& e, bool force_image_load = false) {
    if (e.status == NacpLoadStatus::None) {
        const auto result = LoadControlEntry(e.app_id);
        LoadResultIntoEntry(e, result);
    }

    if (force_image_load && e.status == NacpLoadStatus::Loaded) {
        LoadControlImage(e);
    }
}

// taken from nxdumptool.
void utilsReplaceIllegalCharacters(char *str, bool ascii_only)
{
    static const char g_illegalFileSystemChars[] = "\\/:*?\"<>|";

    size_t str_size = 0, cur_pos = 0;

    if (!str || !(str_size = strlen(str))) return;

    u8 *ptr1 = (u8*)str, *ptr2 = ptr1;
    ssize_t units = 0;
    u32 code = 0;
    bool repl = false;

    while(cur_pos < str_size)
    {
        units = decode_utf8(&code, ptr1);
        if (units < 0) break;

        if (code < 0x20 || (!ascii_only && code == 0x7F) || (ascii_only && code >= 0x7F) || \
            (units == 1 && memchr(g_illegalFileSystemChars, (int)code, std::size(g_illegalFileSystemChars))))
        {
            if (!repl)
            {
                *ptr2++ = '_';
                repl = true;
            }
        } else {
            if (ptr2 != ptr1) memmove(ptr2, ptr1, (size_t)units);
            ptr2 += units;
            repl = false;
        }

        ptr1 += units;
        cur_pos += (size_t)units;
    }

    *ptr2 = '\0';
}

auto isRightsIdValid(FsRightsId id) -> bool {
    FsRightsId empty_id{};
    return 0 != std::memcmp(std::addressof(id), std::addressof(empty_id), sizeof(id));
}

struct HashStr {
    char str[0x21];
};

HashStr hexIdToStr(auto id) {
    HashStr str{};
    const auto id_lower = std::byteswap(*(u64*)id.c);
    const auto id_upper = std::byteswap(*(u64*)(id.c + 0x8));
    std::snprintf(str.str, 0x21, "%016lx%016lx", id_lower, id_upper);
    return str;
}

auto BuildNspPath(const Entry& e, const NsApplicationContentMetaStatus& status) -> fs::FsPath {
    fs::FsPath name_buf = e.GetName();
    utilsReplaceIllegalCharacters(name_buf, true);

    char version[sizeof(NacpStruct::display_version) + 1]{};
    if (status.meta_type == NcmContentMetaType_Patch) {
        std::snprintf(version, sizeof(version), "%s ", e.GetDisplayVersion());
    }

    fs::FsPath path;
    if (App::GetApp()->m_dump_app_folder.Get()) {
        std::snprintf(path, sizeof(path), "%s/%s %s[%016lX][v%u][%s].nsp", name_buf.s, name_buf.s, version, status.application_id, status.version, ncm::GetMetaTypeShortStr(status.meta_type));
    } else {
        std::snprintf(path, sizeof(path), "%s %s[%016lX][v%u][%s].nsp", name_buf.s, version, status.application_id, status.version, ncm::GetMetaTypeShortStr(status.meta_type));
    }

    return path;
}

Result BuildContentEntry(const NsApplicationContentMetaStatus& status, ContentInfoEntry& out) {
    auto& cs = GetNcmCs(status.storageID);
    auto& db = GetNcmDb(status.storageID);
    const auto app_id = ncm::GetAppId(status.meta_type, status.application_id);

    auto id_min = status.application_id;
    auto id_max = status.application_id;
    // workaround N bug where they don't check the full range in the ID filter.
    // https://github.com/Atmosphere-NX/Atmosphere/blob/1d3f3c6e56b994b544fc8cd330c400205d166159/libraries/libstratosphere/source/ncm/ncm_on_memory_content_meta_database_impl.cpp#L22
    if (status.storageID == NcmStorageId_None || status.storageID == NcmStorageId_GameCard) {
        id_min -= 1;
        id_max += 1;
    }

    s32 meta_total;
    s32 meta_entries_written;
    NcmContentMetaKey key;
    R_TRY(ncmContentMetaDatabaseList(std::addressof(db), std::addressof(meta_total), std::addressof(meta_entries_written), std::addressof(key), 1, (NcmContentMetaType)status.meta_type, app_id, id_min, id_max, NcmContentInstallType_Full));
    log_write("ncmContentMetaDatabaseList(): AppId: %016lX Id: %016lX total: %d written: %d storageID: %u key.id %016lX\n", app_id, status.application_id, meta_total, meta_entries_written, status.storageID, key.id);
    R_UNLESS(meta_total == 1, 0x1);
    R_UNLESS(meta_entries_written == 1, 0x1);

    std::vector<NcmContentInfo> cnmt_infos;
    for (s32 i = 0; ; i++) {
        s32 entries_written;
        NcmContentInfo info_out;
        R_TRY(ncmContentMetaDatabaseListContentInfo(std::addressof(db), std::addressof(entries_written), std::addressof(info_out), 1, std::addressof(key), i));

        if (!entries_written) {
            break;
        }

        // check if we need to fetch tickets.
        NcmRightsId ncm_rights_id;
        R_TRY(ncmContentStorageGetRightsIdFromContentId(std::addressof(cs), std::addressof(ncm_rights_id), std::addressof(info_out.content_id), FsContentAttributes_All));

        const auto rights_id = ncm_rights_id.rights_id;
        if (isRightsIdValid(rights_id)) {
            const auto it = std::ranges::find_if(out.rights_ids, [&rights_id](auto& e){
                return !std::memcmp(&e, &rights_id, sizeof(rights_id));
            });

            if (it == out.rights_ids.end()) {
                out.rights_ids.emplace_back(rights_id);
            }
        }

        if (info_out.content_type == NcmContentType_Meta) {
            cnmt_infos.emplace_back(info_out);
        } else {
            out.content_infos.emplace_back(info_out);
        }
    }

    // append cnmt at the end of the list, following StandardNSP spec.
    out.content_infos.insert_range(out.content_infos.end(), cnmt_infos);
    out.status = status;
    R_SUCCEED();
}

Result BuildNspEntry(const Entry& e, const ContentInfoEntry& info, NspEntry& out) {
    out.application_name = e.GetName();
    out.path = BuildNspPath(e, info.status);
    s64 offset{};

    for (auto& rights_id : info.rights_ids) {
        TikEntry entry{rights_id};
        log_write("rights id is valid, fetching common ticket and cert\n");

        u64 tik_size;
        u64 cert_size;
        R_TRY(es::GetCommonTicketAndCertificateSize(&tik_size, &cert_size, &rights_id));
        log_write("got tik_size: %zu cert_size: %zu\n", tik_size, cert_size);

        entry.tik_data.resize(tik_size);
        entry.cert_data.resize(cert_size);
        R_TRY(es::GetCommonTicketAndCertificateData(&tik_size, &cert_size, entry.tik_data.data(), entry.tik_data.size(), entry.cert_data.data(), entry.cert_data.size(), &rights_id));
        log_write("got tik_data: %zu cert_data: %zu\n", tik_size, cert_size);

        char tik_name[0x200];
        std::snprintf(tik_name, sizeof(tik_name), "%s%s", hexIdToStr(rights_id).str, ".tik");

        char cert_name[0x200];
        std::snprintf(cert_name, sizeof(cert_name), "%s%s", hexIdToStr(rights_id).str, ".cert");

        out.collections.emplace_back(tik_name, offset, entry.tik_data.size());
        offset += entry.tik_data.size();

        out.collections.emplace_back(cert_name, offset, entry.cert_data.size());
        offset += entry.cert_data.size();

        out.tickets.emplace_back(entry);
    }

    for (auto& e : info.content_infos) {
        char nca_name[0x200];
        std::snprintf(nca_name, sizeof(nca_name), "%s%s", hexIdToStr(e.content_id).str, e.content_type == NcmContentType_Meta ? ".cnmt.nca" : ".nca");

        u64 size;
        ncmContentInfoSizeToU64(std::addressof(e), std::addressof(size));

        out.collections.emplace_back(nca_name, offset, size);
        offset += size;
    }

    out.nsp_data = yati::container::Nsp::Build(out.collections, out.nsp_size);
    out.cs = GetNcmCs(info.status.storageID);

    R_SUCCEED();
}

Result BuildNspEntries(Entry& e, u32 flags, std::vector<NspEntry>& out) {
    LoadControlEntry(e);

    MetaEntries meta_entries;
    R_TRY(GetMetaEntries(e, meta_entries, flags));

    for (const auto& status : meta_entries) {
        ContentInfoEntry info;
        R_TRY(BuildContentEntry(status, info));

        NspEntry nsp;
        R_TRY(BuildNspEntry(e, info, nsp));
        out.emplace_back(nsp).icon = e.image;
    }

    R_UNLESS(!out.empty(), 0x1);
    R_SUCCEED();
}

void FreeEntry(NVGcontext* vg, Entry& e) {
    nvgDeleteImage(vg, e.image);
    e.image = 0;
}

void LaunchEntry(const Entry& e) {
    const auto rc = appletRequestLaunchApplication(e.app_id, nullptr);
    Notify(rc, "Failed to launch application");
}

void ThreadFunc(void* user) {
    auto data = static_cast<ThreadData*>(user);

    while (data->IsRunning()) {
        data->Run();
    }
}

} // namespace

ThreadData::ThreadData() {
    ueventCreate(&m_uevent, true);
    mutexInit(&m_mutex_id);
    mutexInit(&m_mutex_result);
    m_running = true;
}

auto ThreadData::IsRunning() const -> bool {
    return m_running;
}

void ThreadData::Run() {
    while (IsRunning()) {
        const auto waiter = waiterForUEvent(&m_uevent);
        waitSingle(waiter, UINT64_MAX);

        if (!IsRunning()) {
            return;
        }

        std::vector<u64> ids;
        {
            mutexLock(&m_mutex_id);
            ON_SCOPE_EXIT(mutexUnlock(&m_mutex_id));
            std::swap(ids, m_ids);
        }

        for (u64 i = 0; i < std::size(ids); i++) {
            if (!IsRunning()) {
                return;
            }

            // sleep after every other entry loaded.
            svcSleepThread(2e+6); // 2ms

            const auto result = LoadControlEntry(ids[i]);
            mutexLock(&m_mutex_result);
            ON_SCOPE_EXIT(mutexUnlock(&m_mutex_result));
            m_result.emplace_back(result);
        }
    }
}

void ThreadData::Close() {
    m_running = false;
    ueventSignal(&m_uevent);
}

void ThreadData::Push(u64 id) {
    mutexLock(&m_mutex_id);
    ON_SCOPE_EXIT(mutexUnlock(&m_mutex_id));

    const auto it = std::ranges::find(m_ids, id);
    if (it == m_ids.end()) {
        m_ids.emplace_back(id);
        ueventSignal(&m_uevent);
    }
}

void ThreadData::Push(std::span<const Entry> entries) {
    for (auto& e : entries) {
        Push(e.app_id);
    }
}

void ThreadData::Pop(std::vector<ThreadResultData>& out) {
    mutexLock(&m_mutex_result);
    ON_SCOPE_EXIT(mutexUnlock(&m_mutex_result));

    std::swap(out, m_result);
    m_result.clear();
}

Menu::Menu(u32 flags) : grid::Menu{"Games"_i18n, flags} {
    this->SetActions(
        std::make_pair(Button::L3, Action{[this](){
            if (m_entries.empty()) {
                return;
            }

            m_entries[m_index].selected ^= 1;

            if (m_entries[m_index].selected) {
                m_selected_count++;
            } else {
                m_selected_count--;
            }
        }}),
        std::make_pair(Button::R3, Action{[this](){
            if (m_entries.empty()) {
                return;
            }

            if (m_selected_count == m_entries.size()) {
                ClearSelection();
            } else {
                m_selected_count = m_entries.size();
                for (auto& e : m_entries) {
                    e.selected = true;
                }
            }
        }}),
        std::make_pair(Button::B, Action{"Back"_i18n, [this](){
            SetPop();
        }}),
        std::make_pair(Button::A, Action{"Launch"_i18n, [this](){
            if (m_entries.empty()) {
                return;
            }
            LaunchEntry(m_entries[m_index]);
        }}),
        std::make_pair(Button::X, Action{"Options"_i18n, [this](){
            auto options = std::make_shared<Sidebar>("Game Options"_i18n, Sidebar::Side::RIGHT);
            ON_SCOPE_EXIT(App::Push(options));

            if (m_entries.size()) {
                options->Add(std::make_shared<SidebarEntryCallback>("Sort By"_i18n, [this](){
                    auto options = std::make_shared<Sidebar>("Sort Options"_i18n, Sidebar::Side::RIGHT);
                    ON_SCOPE_EXIT(App::Push(options));

                    SidebarEntryArray::Items sort_items;
                    sort_items.push_back("Updated"_i18n);

                    SidebarEntryArray::Items order_items;
                    order_items.push_back("Descending"_i18n);
                    order_items.push_back("Ascending"_i18n);

                    SidebarEntryArray::Items layout_items;
                    layout_items.push_back("List"_i18n);
                    layout_items.push_back("Icon"_i18n);
                    layout_items.push_back("Grid"_i18n);

                    options->Add(std::make_shared<SidebarEntryArray>("Sort"_i18n, sort_items, [this](s64& index_out){
                        m_sort.Set(index_out);
                        SortAndFindLastFile(false);
                    }, m_sort.Get()));

                    options->Add(std::make_shared<SidebarEntryArray>("Order"_i18n, order_items, [this](s64& index_out){
                        m_order.Set(index_out);
                        SortAndFindLastFile(false);
                    }, m_order.Get()));

                    options->Add(std::make_shared<SidebarEntryArray>("Layout"_i18n, layout_items, [this](s64& index_out){
                        m_layout.Set(index_out);
                        OnLayoutChange();
                    }, m_layout.Get()));

                    options->Add(std::make_shared<SidebarEntryBool>("Hide forwarders"_i18n, m_hide_forwarders.Get(), [this](bool& v_out){
                        m_hide_forwarders.Set(v_out);
                        m_dirty = true;
                    }));
                }));

                #if 0
                options->Add(std::make_shared<SidebarEntryCallback>("Info"_i18n, [this](){

                }));
                #endif

                options->Add(std::make_shared<SidebarEntryCallback>("Launch random game"_i18n, [this](){
                    const auto random_index = randomGet64() % std::size(m_entries);
                    auto& e = m_entries[random_index];
                    LoadControlEntry(e, true);

                    App::Push(std::make_shared<OptionBox>(
                        "Launch "_i18n + e.GetName(),
                        "Back"_i18n, "Launch"_i18n, 1, [this, &e](auto op_index){
                            if (op_index && *op_index) {
                                LaunchEntry(e);
                            }
                        }, e.image
                    ));
                }));

                options->Add(std::make_shared<SidebarEntryCallback>("List meta records"_i18n, [this](){
                    MetaEntries meta_entries;
                    const auto rc = GetMetaEntries(m_entries[m_index], meta_entries);
                    if (R_FAILED(rc)) {
                        App::Push(std::make_shared<ui::ErrorBox>(rc,
                            i18n::get("Failed to list application meta entries")
                        ));
                        return;
                    }

                    if (meta_entries.empty()) {
                        App::Notify("No meta entries found...\n"_i18n);
                        return;
                    }

                    PopupList::Items items;
                    for (auto& e : meta_entries) {
                        char buf[256];
                        std::snprintf(buf, sizeof(buf), "Type: %s Storage: %s [%016lX][v%u]", ncm::GetMetaTypeStr(e.meta_type), ncm::GetStorageIdStr(e.storageID), e.application_id, e.version);
                        items.emplace_back(buf);
                    }

                    App::Push(std::make_shared<PopupList>(
                        "Entries"_i18n, items, [this, meta_entries](auto op_index){
                            #if 0
                            if (op_index) {
                                const auto& e = meta_entries[*op_index];
                            }
                            #endif
                        }
                    ));
                }));

                options->Add(std::make_shared<SidebarEntryCallback>("Dump"_i18n, [this](){
                    auto options = std::make_shared<Sidebar>("Select content to dump"_i18n, Sidebar::Side::RIGHT);
                    ON_SCOPE_EXIT(App::Push(options));

                    options->Add(std::make_shared<SidebarEntryCallback>("Dump All"_i18n, [this](){
                        DumpGames(ContentFlag_All);
                    }, true));
                    options->Add(std::make_shared<SidebarEntryCallback>("Dump Application"_i18n, [this](){
                        DumpGames(ContentFlag_Application);
                    }, true));
                    options->Add(std::make_shared<SidebarEntryCallback>("Dump Patch"_i18n, [this](){
                        DumpGames(ContentFlag_Patch);
                    }, true));
                    options->Add(std::make_shared<SidebarEntryCallback>("Dump AddOnContent"_i18n, [this](){
                        DumpGames(ContentFlag_AddOnContent);
                    }, true));
                    options->Add(std::make_shared<SidebarEntryCallback>("Dump DataPatch"_i18n, [this](){
                        DumpGames(ContentFlag_DataPatch);
                    }, true));
                }, true));

                options->Add(std::make_shared<SidebarEntryCallback>("Dump options"_i18n, [this](){
                    App::DisplayDumpOptions(false);
                }));

                // completely deletes the application record and all data.
                options->Add(std::make_shared<SidebarEntryCallback>("Delete"_i18n, [this](){
                    const auto buf = "Are you sure you want to delete "_i18n + m_entries[m_index].GetName() + "?";
                    App::Push(std::make_shared<OptionBox>(
                        buf,
                        "Back"_i18n, "Delete"_i18n, 0, [this](auto op_index){
                            if (op_index && *op_index) {
                                DeleteGames();
                            }
                        }, m_entries[m_index].image
                    ));
                }, true));
            }
        }})
    );

    OnLayoutChange();

    nsInitialize();
    es::Initialize();

    for (auto& e : ncm_entries) {
        e.Open();
    }

    threadCreate(&m_thread, ThreadFunc, &m_thread_data, nullptr, 1024*32, THREAD_PRIO, THREAD_CORE);
    svcSetThreadCoreMask(m_thread.handle, THREAD_CORE, THREAD_AFFINITY_DEFAULT(THREAD_CORE));
    threadStart(&m_thread);
}

Menu::~Menu() {
    m_thread_data.Close();

    for (auto& e : ncm_entries) {
        e.Close();
    }

    FreeEntries();
    nsExit();
    es::Exit();

    threadWaitForExit(&m_thread);
    threadClose(&m_thread);
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    if (m_dirty) {
        App::Notify("Updating application record list"_i18n);
        SortAndFindLastFile(true);
    }

    MenuBase::Update(controller, touch);
    m_list->OnUpdate(controller, touch, m_index, m_entries.size(), [this](bool touch, auto i) {
        if (touch && m_index == i) {
            FireAction(Button::A);
        } else {
            App::PlaySoundEffect(SoundEffect_Focus);
            SetIndex(i);
        }
    });
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    // max images per frame, in order to not hit io / gpu too hard.
    const int image_load_max = 2;
    int image_load_count = 0;

    std::vector<ThreadResultData> data;
    m_thread_data.Pop(data);

    for (const auto& d : data) {
        const auto it = std::ranges::find_if(m_entries, [&d](auto& e) {
            return e.app_id == d.id;
        });

        if (it != m_entries.end()) {
            LoadResultIntoEntry(*it, d);
        }
    }

    m_list->Draw(vg, theme, m_entries.size(), [this, &image_load_count](auto* vg, auto* theme, auto v, auto pos) {
        const auto& [x, y, w, h] = v;
        auto& e = m_entries[pos];

        if (e.status == NacpLoadStatus::None) {
            m_thread_data.Push(e.app_id);
            e.status = NacpLoadStatus::Progress;
        }

        // lazy load image
        if (image_load_count < image_load_max) {
            if (LoadControlImage(e)) {
                image_load_count++;
            }
        }

        const auto selected = pos == m_index;
        DrawEntry(vg, theme, m_layout.Get(), v, selected, e.image, e.GetName(), e.GetAuthor(), e.GetDisplayVersion());

        if (e.selected) {
            gfx::drawRect(vg, v, theme->GetColour(ThemeEntryID_FOCUS), 5);
            gfx::drawText(vg, x + w / 2, y + h / 2, 24.f, "\uE14B", nullptr, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_SELECTED));
        }
    });
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();
    if (m_entries.empty()) {
        ScanHomebrew();
    }
}

void Menu::SetIndex(s64 index) {
    m_index = index;
    if (!m_index) {
        m_list->SetYoff(0);
    }

    char title_id[33];
    std::snprintf(title_id, sizeof(title_id), "%016lX", m_entries[m_index].app_id);
    SetTitleSubHeading(title_id);
    this->SetSubHeading(std::to_string(m_index + 1) + " / " + std::to_string(m_entries.size()));
}

void Menu::ScanHomebrew() {
    constexpr auto ENTRY_CHUNK_COUNT = 1000;
    const auto hide_forwarders = m_hide_forwarders.Get();
    TimeStamp ts;

    appletSetCpuBoostMode(ApmCpuBoostMode_FastLoad);
    ON_SCOPE_EXIT(appletSetCpuBoostMode(ApmCpuBoostMode_Normal));

    FreeEntries();
    m_entries.reserve(ENTRY_CHUNK_COUNT);

    std::vector<NsApplicationRecord> record_list(ENTRY_CHUNK_COUNT);
    s32 offset{};
    while (true) {
        s32 record_count{};
        if (R_FAILED(nsListApplicationRecord(record_list.data(), record_list.size(), offset, &record_count))) {
            log_write("failed to list application records at offset: %d\n", offset);
        }

        // finished parsing all entries.
        if (!record_count) {
            break;
        }

        for (s32 i = 0; i < record_count; i++) {
            const auto& e = record_list[i];
            #if 0
            u8 unk_x09 = e.unk_x09;
            u64 unk_x0a;// = e.unk_x0a;
            u8 unk_x10 = e.unk_x10;
            u64 unk_x11;// = e.unk_x11;
            memcpy(&unk_x0a, e.unk_x0a, sizeof(e.unk_x0a));
            memcpy(&unk_x11, e.unk_x11, sizeof(e.unk_x11));
            log_write("ID: %016lx got type: %u unk_x09: %u unk_x0a: %zu unk_x10: %u unk_x11: %zu\n", e.application_id, e.type,
                unk_x09,
                unk_x0a,
                unk_x10,
                unk_x11
            );
            #endif
            if (hide_forwarders && (e.application_id & 0x0500000000000000) == 0x0500000000000000) {
                continue;
            }

            m_entries.emplace_back(e.application_id);
        }

        offset += record_count;
    }

    m_is_reversed = false;
    m_dirty = false;
    log_write("games found: %zu time_taken: %.2f seconds %zu ms %zu ns\n", m_entries.size(), ts.GetSecondsD(), ts.GetMs(), ts.GetNs());
    this->Sort();
    SetIndex(0);
    ClearSelection();
}

void Menu::Sort() {
    // const auto sort = m_sort.Get();
    const auto order = m_order.Get();

    if (order == OrderType_Ascending) {
        if (!m_is_reversed) {
            std::ranges::reverse(m_entries);
            m_is_reversed = true;
        }
    } else {
        if (m_is_reversed) {
            std::ranges::reverse(m_entries);
            m_is_reversed = false;
        }
    }
}

void Menu::SortAndFindLastFile(bool scan) {
    const auto app_id = m_entries[m_index].app_id;
    if (scan) {
        ScanHomebrew();
    } else {
        Sort();
    }
    SetIndex(0);

    s64 index = -1;
    for (u64 i = 0; i < m_entries.size(); i++) {
        if (app_id == m_entries[i].app_id) {
            index = i;
            break;
        }
    }

    if (index >= 0) {
        const auto row = m_list->GetRow();
        const auto page = m_list->GetPage();
        // guesstimate where the position is
        if (index >= page) {
            m_list->SetYoff((((index - page) + row) / row) * m_list->GetMaxY());
        } else {
            m_list->SetYoff(0);
        }
        SetIndex(index);
    }
}

void Menu::FreeEntries() {
    auto vg = App::GetVg();

    for (auto&p : m_entries) {
        FreeEntry(vg, p);
    }

    m_entries.clear();
}

void Menu::OnLayoutChange() {
    m_index = 0;
    grid::Menu::OnLayoutChange(m_list, m_layout.Get());
}

void Menu::DeleteGames() {
    App::Push(std::make_shared<ProgressBox>(0, "Deleting"_i18n, "", [this](auto pbox) -> Result {
        auto targets = GetSelectedEntries();

        for (s64 i = 0; i < std::size(targets); i++) {
            auto& e = targets[i];

            LoadControlEntry(e);
            pbox->SetTitle(e.GetName());
            pbox->UpdateTransfer(i + 1, std::size(targets));
            R_TRY(nsDeleteApplicationCompletely(e.app_id));
        }

        R_SUCCEED();
    }, [this](Result rc){
        App::PushErrorBox(rc, "Delete failed!"_i18n);

        ClearSelection();
        m_dirty = true;

        if (R_SUCCEEDED(rc)) {
            App::Notify("Delete successfull!"_i18n);
        }
    }));
}

void Menu::DumpGames(u32 flags) {
    auto targets = GetSelectedEntries();

    std::vector<NspEntry> nsp_entries;
    for (auto& e : targets) {
        BuildNspEntries(e, flags, nsp_entries);
    }

    std::vector<fs::FsPath> paths;
    for (auto& e : nsp_entries) {
        paths.emplace_back(fs::AppendPath("/dumps/NSP", e.path));
    }

    auto source = std::make_shared<NspSource>(nsp_entries);
    dump::Dump(source, paths, [this](Result rc){
        ClearSelection();
    });
}

} // namespace sphaira::ui::menu::game
