#include "haze_helper.hpp"

#include "app.hpp"
#include "fs.hpp"
#include "log.hpp"
#include "evman.hpp"
#include "i18n.hpp"

#include <algorithm>
#include <haze.h>

namespace sphaira::libhaze {
namespace {

struct InstallSharedData {
    Mutex mutex;
    std::string current_file;

    void* user;
    OnInstallStart on_start;
    OnInstallWrite on_write;
    OnInstallClose on_close;

    bool in_progress;
    bool enabled;
};

constexpr int THREAD_PRIO = 0x20;
constexpr int THREAD_CORE = 2;
std::atomic_bool g_should_exit = false;
bool g_is_running{false};
Mutex g_mutex{};

InstallSharedData g_shared_data{};

const char* SUPPORTED_EXT[] = {
    ".nsp", ".xci", ".nsz", ".xcz",
};

// ive given up with good names.
void on_thing() {
    log_write("[MTP] doing on_thing\n");
    SCOPED_MUTEX(&g_shared_data.mutex);
    log_write("[MTP] locked on_thing\n");

    if (!g_shared_data.in_progress) {
        if (!g_shared_data.current_file.empty()) {
            log_write("[MTP] pushing new file data\n");
            if (!g_shared_data.on_start || !g_shared_data.on_start(g_shared_data.current_file.c_str())) {
                g_shared_data.current_file.clear();
            } else {
                log_write("[MTP] success on new file push\n");
                g_shared_data.in_progress = true;
            }
        }
    }
}

struct FsProxyBase : haze::FileSystemProxyImpl {
    FsProxyBase(const char* name, const char* display_name) : m_name{name}, m_display_name{display_name} {

    }

    auto FixPath(const char* base, const char* path) const {
        fs::FsPath buf;
        const auto len = std::strlen(GetName());

        if (len && !strncasecmp(path, GetName(), len)) {
            std::snprintf(buf, sizeof(buf), "%s/%s", base, path + len);
        } else {
            std::snprintf(buf, sizeof(buf), "%s/%s", base, path);
            // std::strcpy(buf, path);
        }

        log_write("[FixPath] %s -> %s\n", path, buf.s);
        return buf;
    }

    const char* GetName() const override {
        return m_name.c_str();
    }
    const char* GetDisplayName() const override {
        return m_display_name.c_str();
    }

protected:
    const std::string m_name;
    const std::string m_display_name;
};

struct FsProxy final : FsProxyBase {
    using File = fs::File;
    using Dir = fs::Dir;
    using DirEntry = FsDirectoryEntry;

    FsProxy(std::unique_ptr<fs::Fs>&& fs, const char* name, const char* display_name)
    : FsProxyBase{name, display_name}
    , m_fs{std::forward<decltype(fs)>(fs)} {
    }

    ~FsProxy() {
        if (m_fs->IsNative()) {
            auto fs = (fs::FsNative*)m_fs.get();
            fsFsCommit(&fs->m_fs);
        }
    }

    auto FixPath(const char* path) const {
        return FsProxyBase::FixPath(m_fs->Root(), path);
    }

    // TODO: impl this for stdio
    Result GetTotalSpace(const char *path, s64 *out) override {
        if (m_fs->IsNative()) {
            auto fs = (fs::FsNative*)m_fs.get();
            return fsFsGetTotalSpace(&fs->m_fs, FixPath(path), out);
        }

        // todo: use statvfs.
        // then fallback to 256gb if not available.
        *out = 1024ULL * 1024ULL * 1024ULL * 256ULL;
        R_SUCCEED();
    }

    Result GetFreeSpace(const char *path, s64 *out) override {
        if (m_fs->IsNative()) {
            auto fs = (fs::FsNative*)m_fs.get();
            return fsFsGetFreeSpace(&fs->m_fs, FixPath(path), out);
        }

        // todo: use statvfs.
        // then fallback to 256gb if not available.
        *out = 1024ULL * 1024ULL * 1024ULL * 256ULL;
        R_SUCCEED();
    }

    Result GetEntryType(const char *path, haze::FileAttrType *out_entry_type) override {
        FsDirEntryType type;
        R_TRY(m_fs->GetEntryType(FixPath(path), &type));
        *out_entry_type = (type == FsDirEntryType_Dir) ? haze::FileAttrType_DIR : haze::FileAttrType_FILE;
        R_SUCCEED();
    }

    Result GetEntryAttributes(const char *path, haze::FileAttr *out) override {
        FsDirEntryType type;
        R_TRY(m_fs->GetEntryType(FixPath(path), &type));

        if (type == FsDirEntryType_File) {
            out->type = haze::FileAttrType_FILE;

            // it doesn't matter if this fails.
            s64 size{};
            FsTimeStampRaw timestamp{};
            R_TRY(m_fs->FileGetSizeAndTimestamp(FixPath(path), &timestamp, &size));

            out->size = size;
            if (timestamp.is_valid) {
                out->ctime = timestamp.created;
                out->mtime = timestamp.modified;
            }
        } else {
            out->type = haze::FileAttrType_DIR;
        }

        if (IsReadOnly()) {
            out->flag |= haze::FileAttrFlag_READ_ONLY;
        }

        R_SUCCEED();
    }

    Result CreateFile(const char* path, s64 size) override {
        log_write("[HAZE] CreateFile(%s)\n", path);
        return m_fs->CreateFile(FixPath(path), 0, 0);
    }

    Result DeleteFile(const char* path) override {
        log_write("[HAZE] DeleteFile(%s)\n", path);
        return m_fs->DeleteFile(FixPath(path));
    }

    Result RenameFile(const char *old_path, const char *new_path) override {
        log_write("[HAZE] RenameFile(%s -> %s)\n", old_path, new_path);
        return m_fs->RenameFile(FixPath(old_path), FixPath(new_path));
    }

    Result OpenFile(const char *path, haze::FileOpenMode mode, haze::File *out_file) override {
        log_write("[HAZE] OpenFile(%s)\n", path);

        u32 flags = FsOpenMode_Read;
        if (mode == haze::FileOpenMode_WRITE) {
            flags = FsOpenMode_Write | FsOpenMode_Append;
        }

        auto f = new File();
        const auto rc = m_fs->OpenFile(FixPath(path), flags, f);
        if (R_FAILED(rc)) {
            log_write("[HAZE] OpenFile(%s) failed: 0x%X\n", path, rc);
            delete f;
            return rc;
        }


        out_file->impl = f;
        R_SUCCEED();
    }

    Result GetFileSize(haze::File *file, s64 *out_size) override {
        auto f = static_cast<File*>(file->impl);
        return f->GetSize(out_size);
    }

    Result SetFileSize(haze::File *file, s64 size) override {
        auto f = static_cast<File*>(file->impl);
        return f->SetSize(size);
    }

    Result ReadFile(haze::File *file, s64 off, void *buf, u64 read_size, u64 *out_bytes_read) override {
        auto f = static_cast<File*>(file->impl);
        return f->Read(off, buf, read_size, FsReadOption_None, out_bytes_read);
    }

    Result WriteFile(haze::File *file, s64 off, const void *buf, u64 write_size) override {
        auto f = static_cast<File*>(file->impl);
        return f->Write(off, buf, write_size, FsWriteOption_None);
    }

    void CloseFile(haze::File *file) override {
        auto f = static_cast<File*>(file->impl);
        if (f) {
            delete f;
            file->impl = nullptr;
        }
    }

    Result CreateDirectory(const char* path) override {
        return m_fs->CreateDirectory(FixPath(path));
    }

    Result DeleteDirectoryRecursively(const char* path) override {
        return m_fs->DeleteDirectoryRecursively(FixPath(path));
    }

    Result RenameDirectory(const char *old_path, const char *new_path) override {
        return m_fs->RenameDirectory(FixPath(old_path), FixPath(new_path));
    }

    Result OpenDirectory(const char *path, haze::Dir *out_dir) override {
        auto dir = new Dir();
        const auto rc = m_fs->OpenDirectory(FixPath(path), FsDirOpenMode_ReadDirs | FsDirOpenMode_ReadFiles | FsDirOpenMode_NoFileSize, dir);
        if (R_FAILED(rc)) {
            log_write("[HAZE] OpenDirectory(%s) failed: 0x%X\n", path, rc);
            delete dir;
            return rc;
        }

        out_dir->impl = dir;
        R_SUCCEED();
    }

    Result ReadDirectory(haze::Dir *d, s64 *out_total_entries, size_t max_entries, haze::DirEntry *buf) override {
        auto dir = static_cast<Dir*>(d->impl);

        std::vector<FsDirectoryEntry> entries(max_entries);
        R_TRY(dir->Read(out_total_entries, entries.size(), entries.data()));

        for (s64 i = 0; i < *out_total_entries; i++) {
            std::strcpy(buf[i].name, entries[i].name);
        }

        R_SUCCEED();
    }

    Result GetDirectoryEntryCount(haze::Dir *d, s64 *out_count) override {
        auto dir = static_cast<Dir*>(d->impl);
        return dir->GetEntryCount(out_count);
    }

    void CloseDirectory(haze::Dir *d) override {
        auto dir = static_cast<Dir*>(d->impl);
        if (dir) {
            delete dir;
            d->impl = nullptr;
        }
    }

private:
    std::unique_ptr<fs::Fs> m_fs{};
};

// fake fs that allows for files to create r/w on the root.
// folders are not yet supported.
struct FsProxyVfs : FsProxyBase {
    struct File {
        u64 index{};
        haze::FileOpenMode mode{};
    };

    struct Dir {
        u64 pos{};
    };

    using FsProxyBase::FsProxyBase;
    virtual ~FsProxyVfs() = default;

    auto FixPath(const char* path) const {
        return FsProxyBase::FixPath("", path);
    }

    auto GetFileName(const char* s) -> const char* {
        const auto file_name = std::strrchr(s, '/');
        if (!file_name || file_name[1] == '\0') {
            return nullptr;
        }
        return file_name + 1;
    }

    virtual Result GetEntryType(const char *path, haze::FileAttrType *out_entry_type) {
        if (FixPath(path) == "/") {
            *out_entry_type = haze::FileAttrType_DIR;
            R_SUCCEED();
        } else {
            const auto file_name = GetFileName(path);
            R_UNLESS(file_name, FsError_PathNotFound);

            const auto it = std::ranges::find_if(m_entries, [file_name](auto& e){
                return !strcasecmp(file_name, e.name);
            });
            R_UNLESS(it != m_entries.end(), FsError_PathNotFound);

            *out_entry_type = haze::FileAttrType_FILE;
            R_SUCCEED();
        }
    }

    virtual Result CreateFile(const char* path, s64 size) {
        const auto file_name = GetFileName(path);
        R_UNLESS(file_name, FsError_PathNotFound);

        const auto it = std::ranges::find_if(m_entries, [file_name](auto& e){
            return !strcasecmp(file_name, e.name);
        });
        R_UNLESS(it == m_entries.end(), FsError_PathAlreadyExists);

        FsDirectoryEntry entry{};
        std::strcpy(entry.name, file_name);
        entry.type = FsDirEntryType_File;
        entry.file_size = size;

        m_entries.emplace_back(entry);
        R_SUCCEED();
    }

    virtual Result DeleteFile(const char* path) {
        const auto file_name = GetFileName(path);
        R_UNLESS(file_name, FsError_PathNotFound);

        const auto it = std::ranges::find_if(m_entries, [file_name](auto& e){
            return !strcasecmp(file_name, e.name);
        });
        R_UNLESS(it != m_entries.end(), FsError_PathNotFound);

        m_entries.erase(it);
        R_SUCCEED();
    }

    virtual Result RenameFile(const char *old_path, const char *new_path) {
        const auto file_name = GetFileName(old_path);
        R_UNLESS(file_name, FsError_PathNotFound);

        const auto it = std::ranges::find_if(m_entries, [file_name](auto& e){
            return !strcasecmp(file_name, e.name);
        });
        R_UNLESS(it != m_entries.end(), FsError_PathNotFound);

        const auto file_name_new = GetFileName(new_path);
        R_UNLESS(file_name_new, FsError_PathNotFound);

        const auto new_it = std::ranges::find_if(m_entries, [file_name_new](auto& e){
            return !strcasecmp(file_name_new, e.name);
        });
        R_UNLESS(new_it == m_entries.end(), FsError_PathAlreadyExists);

        std::strcpy(it->name, file_name_new);
        R_SUCCEED();
    }

    virtual Result OpenFile(const char *path, haze::FileOpenMode mode, haze::File *out_file) {
        const auto file_name = GetFileName(path);
        R_UNLESS(file_name, FsError_PathNotFound);

        const auto it = std::ranges::find_if(m_entries, [file_name](auto& e){
            return !strcasecmp(file_name, e.name);
        });
        R_UNLESS(it != m_entries.end(), FsError_PathNotFound);

        auto f = new File();
        f->index = std::distance(m_entries.begin(), it);
        f->mode = mode;
        out_file->impl = f;
        R_SUCCEED();
    }

    virtual Result GetFileSize(haze::File *file, s64 *out_size) {
        auto f = static_cast<File*>(file->impl);
        *out_size = m_entries[f->index].file_size;
        R_SUCCEED();
    }

    virtual Result SetFileSize(haze::File *file, s64 size) {
        auto f = static_cast<File*>(file->impl);
        m_entries[f->index].file_size = size;
        R_SUCCEED();
    }

    virtual Result ReadFile(haze::File *file, s64 off, void *buf, u64 read_size, u64 *out_bytes_read) {
        // stub for now as it may confuse users who think that the returned file is valid.
        // the code below can be used to benchmark mtp reads.
        R_THROW(FsError_NotImplemented);
    }

    virtual Result WriteFile(haze::File *file, s64 off, const void *buf, u64 write_size) {
        auto f = static_cast<File*>(file->impl);
        auto& e = m_entries[f->index];
        e.file_size = std::max<s64>(e.file_size, off + write_size);
        R_SUCCEED();
    }

    virtual void CloseFile(haze::File *file) {
        auto f = static_cast<File*>(file->impl);
        if (f) {
            delete f;
            file->impl = nullptr;
        }
    }

    Result CreateDirectory(const char* path) override {
        R_THROW(FsError_NotImplemented);
    }

    Result DeleteDirectoryRecursively(const char* path) override {
        R_THROW(FsError_NotImplemented);
    }

    Result RenameDirectory(const char *old_path, const char *new_path) override {
        R_THROW(FsError_NotImplemented);
    }

    Result OpenDirectory(const char *path, haze::Dir *out_dir) override {
        auto dir = new Dir();
        out_dir->impl = dir;
        R_SUCCEED();
    }

    Result ReadDirectory(haze::Dir *d, s64 *out_total_entries, size_t max_entries, haze::DirEntry *buf) override {
        auto dir = static_cast<Dir*>(d->impl);

        max_entries = std::min<s64>(m_entries.size() - dir->pos, max_entries);

        for (size_t i = 0; i < max_entries; i++) {
            std::strcpy(buf[i].name, m_entries[dir->pos + i].name);
        }

        dir->pos += max_entries;
        *out_total_entries = max_entries;
        R_SUCCEED();
    }

    Result GetDirectoryEntryCount(haze::Dir *d, s64 *out_count) override {
        *out_count = m_entries.size();
        R_SUCCEED();
    }

    void CloseDirectory(haze::Dir *d) override {
        auto dir = static_cast<Dir*>(d->impl);
        if (dir) {
            delete dir;
            d->impl = nullptr;
        }
    }

protected:
    std::vector<FsDirectoryEntry> m_entries;
};

struct FsDevNullProxy final : FsProxyVfs {
    using FsProxyVfs::FsProxyVfs;

    Result GetTotalSpace(const char *path, s64 *out) override {
        *out = 1024ULL * 1024ULL * 1024ULL * 256ULL;
        R_SUCCEED();
    }

    Result GetFreeSpace(const char *path, s64 *out) override {
        *out = 1024ULL * 1024ULL * 1024ULL * 256ULL;
        R_SUCCEED();
    }
};

struct FsInstallProxy final : FsProxyVfs {
    using FsProxyVfs::FsProxyVfs;

    Result FailedIfNotEnabled() {
        SCOPED_MUTEX(&g_shared_data.mutex);
        if (!g_shared_data.enabled) {
            App::Notify("Please launch MTP install menu before trying to install"_i18n);
            R_THROW(FsError_NotImplemented);
        }
        R_SUCCEED();
    }

    Result IsValidFileType(const char* name) {
        const char* ext = std::strrchr(name, '.');
        if (!ext) {
            R_THROW(FsError_NotImplemented);
        }

        bool found = false;
        for (size_t i = 0; i < std::size(SUPPORTED_EXT); i++) {
            if (!strcasecmp(ext, SUPPORTED_EXT[i])) {
                found = true;
                break;
            }
        }

        if (!found) {
            R_THROW(FsError_NotImplemented);
        }

        R_SUCCEED();
    }

    Result GetTotalSpace(const char *path, s64 *out) override {
        if (App::GetApp()->m_install_sd.Get()) {
            return fs::FsNativeContentStorage(FsContentStorageId_SdCard).GetTotalSpace("/", out);
        } else {
            return fs::FsNativeContentStorage(FsContentStorageId_User).GetTotalSpace("/", out);
        }
    }

    Result GetFreeSpace(const char *path, s64 *out) override {
        if (App::GetApp()->m_install_sd.Get()) {
            return fs::FsNativeContentStorage(FsContentStorageId_SdCard).GetFreeSpace("/", out);
        } else {
            return fs::FsNativeContentStorage(FsContentStorageId_User).GetFreeSpace("/", out);
        }
    }

    Result GetEntryType(const char *path, haze::FileAttrType *out_entry_type) override {
        R_TRY(FsProxyVfs::GetEntryType(path, out_entry_type));
        if (*out_entry_type == haze::FileAttrType_FILE) {
            R_TRY(FailedIfNotEnabled());
        }
        R_SUCCEED();
    }

    Result CreateFile(const char* path, s64 size) override {
        R_TRY(FailedIfNotEnabled());
        R_TRY(IsValidFileType(path));
        R_TRY(FsProxyVfs::CreateFile(path, size));
        R_SUCCEED();
    }

    Result OpenFile(const char *path, haze::FileOpenMode mode, haze::File *out_file) override {
        R_TRY(FailedIfNotEnabled());
        R_TRY(IsValidFileType(path));
        R_TRY(FsProxyVfs::OpenFile(path, mode, out_file));
        log_write("[MTP] done file open: %s mode: 0x%X\n", path, mode);

        if (mode == haze::FileOpenMode_WRITE) {
            auto f = static_cast<File*>(out_file->impl);
            const auto& e = m_entries[f->index];

            // check if we already have this file queued.
            log_write("[MTP] checking if empty\n");
            R_UNLESS(g_shared_data.current_file.empty(), FsError_NotImplemented);
            log_write("[MTP] is empty\n");
            g_shared_data.current_file = e.name;
            on_thing();
        }

        log_write("[MTP] got file: %s\n", path);
        R_SUCCEED();
    }

    Result WriteFile(haze::File *file, s64 off, const void *buf, u64 write_size) override {
        SCOPED_MUTEX(&g_shared_data.mutex);
        if (!g_shared_data.enabled) {
            log_write("[MTP] failing as not enabled\n");
            R_THROW(FsError_NotImplemented);
        }

        if (!g_shared_data.on_write || !g_shared_data.on_write(buf, write_size)) {
            log_write("[MTP] failing as not written\n");
            R_THROW(FsError_NotImplemented);
        }

        R_TRY(FsProxyVfs::WriteFile(file, off, buf, write_size));
        R_SUCCEED();
    }

    void CloseFile(haze::File *file) override {
        auto f = static_cast<File*>(file->impl);
        if (!f) {
            return;
        }

        bool update{};
        {
            SCOPED_MUTEX(&g_shared_data.mutex);
            if (f->mode == haze::FileOpenMode_WRITE) {
                log_write("[MTP] closing current file\n");
                if (g_shared_data.on_close) {
                    g_shared_data.on_close();
                }

                g_shared_data.in_progress = false;
                g_shared_data.current_file.clear();
                update = true;
            }
        }

        if (update) {
            on_thing();
        }

        FsProxyVfs::CloseFile(file);
    }
};

haze::FsEntries g_fs_entries{};

void haze_callback(const haze::CallbackData *data) {
    #if 0
    auto& e = *data;

    switch (e.type) {
        case haze::CallbackType_OpenSession: log_write("[LIBHAZE] Opening Session\n"); break;
        case haze::CallbackType_CloseSession: log_write("[LIBHAZE] Closing Session\n"); break;

        case haze::CallbackType_CreateFile: log_write("[LIBHAZE] Creating File: %s\n", e.file.filename); break;
        case haze::CallbackType_DeleteFile: log_write("[LIBHAZE] Deleting File: %s\n", e.file.filename); break;

        case haze::CallbackType_RenameFile: log_write("[LIBHAZE] Rename File: %s -> %s\n", e.rename.filename, e.rename.newname); break;
        case haze::CallbackType_RenameFolder: log_write("[LIBHAZE] Rename Folder: %s -> %s\n", e.rename.filename, e.rename.newname); break;

        case haze::CallbackType_CreateFolder: log_write("[LIBHAZE] Creating Folder: %s\n", e.file.filename); break;
        case haze::CallbackType_DeleteFolder: log_write("[LIBHAZE] Deleting Folder: %s\n", e.file.filename); break;

        case haze::CallbackType_ReadBegin: log_write("[LIBHAZE] Reading File Begin: %s \n", e.file.filename); break;
        case haze::CallbackType_ReadProgress: log_write("\t[LIBHAZE] Reading File: offset: %lld size: %lld\n", e.progress.offset, e.progress.size); break;
        case haze::CallbackType_ReadEnd: log_write("[LIBHAZE] Reading File Finished: %s\n", e.file.filename); break;

        case haze::CallbackType_WriteBegin: log_write("[LIBHAZE] Writing File Begin: %s \n", e.file.filename); break;
        case haze::CallbackType_WriteProgress: log_write("\t[LIBHAZE] Writing File: offset: %lld size: %lld\n", e.progress.offset, e.progress.size); break;
        case haze::CallbackType_WriteEnd: log_write("[LIBHAZE] Writing File Finished: %s\n", e.file.filename); break;
    }
    #endif

    App::NotifyFlashLed();
}

} // namespace

bool Init() {
    SCOPED_MUTEX(&g_mutex);
    if (g_is_running) {
        log_write("[MTP] already enabled, cannot open\n");
        return false;
    }

    // add default mount of the sd card.
    g_fs_entries.emplace_back(std::make_shared<FsProxy>(std::make_unique<fs::FsNativeSd>(), "", "microSD card"));

    if (App::GetApp()->m_mtp_show_album.Get()) {
        g_fs_entries.emplace_back(std::make_shared<FsProxy>(std::make_unique<fs::FsNativeImage>(FsImageDirectoryId_Sd), "Album", "Album (Image SD)"));
    }

    if (App::GetApp()->m_mtp_show_content_sd.Get()) {
        g_fs_entries.emplace_back(std::make_shared<FsProxy>(std::make_unique<fs::FsNativeContentStorage>(FsContentStorageId_SdCard), "ContentsM", "Contents (microSD card)"));
    }

    if (App::GetApp()->m_mtp_show_content_system.Get()) {
        g_fs_entries.emplace_back(std::make_shared<FsProxy>(std::make_unique<fs::FsNativeContentStorage>(FsContentStorageId_System), "ContentsS", "Contents (System)"));
    }

    if (App::GetApp()->m_mtp_show_content_user.Get()) {
        g_fs_entries.emplace_back(std::make_shared<FsProxy>(std::make_unique<fs::FsNativeContentStorage>(FsContentStorageId_User), "ContentsU", "Contents (User)"));
    }

    if (App::GetApp()->m_mtp_show_games.Get()) {
        g_fs_entries.emplace_back(std::make_shared<FsProxy>(std::make_unique<fs::FsStdio>(true, "games:/"), "Games", "Games"));
    }

    if (App::GetApp()->m_mtp_show_install.Get()) {
        g_fs_entries.emplace_back(std::make_shared<FsInstallProxy>("install", "Install (NSP, XCI, NSZ, XCZ)"));
    }

    if (App::GetApp()->m_mtp_show_mounts.Get()) {
        g_fs_entries.emplace_back(std::make_shared<FsProxy>(std::make_unique<fs::FsStdio>(true, "mounts:/"), "Mounts", "Mounts"));
    }

    if (App::GetApp()->m_mtp_show_speedtest.Get()) {
        g_fs_entries.emplace_back(std::make_shared<FsDevNullProxy>("DevNull", "DevNull (Speed Test)"));
    }

    g_should_exit = false;
    if (!haze::Initialize(haze_callback, g_fs_entries, App::GetApp()->m_mtp_vid.Get(), App::GetApp()->m_mtp_pid.Get())) {
        return false;
    }

    log_write("[MTP] started\n");
    return g_is_running = true;
}

bool IsInit() {
    SCOPED_MUTEX(&g_mutex);
    return g_is_running;
}

void Exit() {
    SCOPED_MUTEX(&g_mutex);
    if (!g_is_running) {
        return;
    }

    haze::Exit();
    g_is_running = false;
    g_should_exit = true;
    g_fs_entries.clear();

    log_write("[MTP] exitied\n");
}

void InitInstallMode(const OnInstallStart& on_start, const OnInstallWrite& on_write, const OnInstallClose& on_close) {
    SCOPED_MUTEX(&g_shared_data.mutex);
    g_shared_data.on_start = on_start;
    g_shared_data.on_write = on_write;
    g_shared_data.on_close = on_close;
    g_shared_data.enabled = true;
}

void DisableInstallMode() {
    SCOPED_MUTEX(&g_shared_data.mutex);
    g_shared_data.enabled = false;
}

} // namespace sphaira::libhaze
