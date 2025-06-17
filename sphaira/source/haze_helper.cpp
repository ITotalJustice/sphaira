#include "haze_helper.hpp"

#include "app.hpp"
#include "fs.hpp"
#include "log.hpp"
#include "evman.hpp"
#include "i18n.hpp"

#include <algorithm>
#include <haze.h>

namespace sphaira::haze {
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

constexpr int THREAD_PRIO = PRIO_PREEMPTIVE;
constexpr int THREAD_CORE = 2;
volatile bool g_should_exit = false;
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

struct FsProxyBase : ::haze::FileSystemProxyImpl {
    FsProxyBase(const char* name, const char* display_name) : m_name{name}, m_display_name{display_name} {

    }

    auto FixPath(const char* path) const {
        fs::FsPath buf;
        const auto len = std::strlen(GetName());

        if (len && !strncasecmp(path + 1, GetName(), len)) {
            std::snprintf(buf, sizeof(buf), "/%s", path + 1 + len);
        } else {
            std::strcpy(buf, path);
        }

        // log_write("[FixPath] %s -> %s\n", path, buf.s);
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
    FsProxy(std::shared_ptr<fs::Fs> fs, const char* name, const char* display_name) : FsProxyBase{name, display_name} {
        m_fs = fs;
    }

    ~FsProxy() {
        if (m_fs->IsNative()) {
            auto fs = (fs::FsNative*)m_fs.get();
            fsFsCommit(&fs->m_fs);
        }
    }

    // TODO: impl this for stdio
    Result GetTotalSpace(const char *path, s64 *out) override {
        if (m_fs->IsNative()) {
            auto fs = (fs::FsNative*)m_fs.get();
            return fsFsGetTotalSpace(&fs->m_fs, FixPath(path), out);
        }
        *out = 1024ULL * 1024ULL * 1024ULL * 256ULL;
        R_SUCCEED();
    }
    Result GetFreeSpace(const char *path, s64 *out) override {
        if (m_fs->IsNative()) {
            auto fs = (fs::FsNative*)m_fs.get();
            return fsFsGetFreeSpace(&fs->m_fs, FixPath(path), out);
        }
        *out = 1024ULL * 1024ULL * 1024ULL * 256ULL;
        R_SUCCEED();
    }
    Result GetEntryType(const char *path, FsDirEntryType *out_entry_type) override {
        const auto rc = m_fs->GetEntryType(FixPath(path), out_entry_type);
        log_write("[HAZE] GetEntryType(%s) 0x%X\n", path, rc);
        return rc;
    }
    Result CreateFile(const char* path, s64 size, u32 option) override {
        log_write("[HAZE] CreateFile(%s)\n", path);
        return m_fs->CreateFile(FixPath(path), size, option);
    }
    Result DeleteFile(const char* path) override {
        log_write("[HAZE] DeleteFile(%s)\n", path);
        return m_fs->DeleteFile(FixPath(path));
    }
    Result RenameFile(const char *old_path, const char *new_path) override {
        log_write("[HAZE] RenameFile(%s -> %s)\n", old_path, new_path);
        return m_fs->RenameFile(FixPath(old_path), FixPath(new_path));
    }
    Result OpenFile(const char *path, u32 mode, FsFile *out_file) override {
        log_write("[HAZE] OpenFile(%s)\n", path);
        auto fptr = new fs::File();
        const auto rc = m_fs->OpenFile(FixPath(path), mode, fptr);

        if (R_SUCCEEDED(rc)) {
            std::memcpy(&out_file->s, &fptr, sizeof(fptr));
        } else {
            delete fptr;
        }

        return rc;
    }
    Result GetFileSize(FsFile *file, s64 *out_size) override {
        log_write("[HAZE] GetFileSize()\n");
        fs::File* f;
        std::memcpy(&f, &file->s, sizeof(f));
        return f->GetSize(out_size);
    }
    Result SetFileSize(FsFile *file, s64 size) override {
        log_write("[HAZE] SetFileSize()\n");
        fs::File* f;
        std::memcpy(&f, &file->s, sizeof(f));
        return f->SetSize(size);
    }
    Result ReadFile(FsFile *file, s64 off, void *buf, u64 read_size, u32 option, u64 *out_bytes_read) override {
        log_write("[HAZE] ReadFile()\n");
        fs::File* f;
        std::memcpy(&f, &file->s, sizeof(f));
        return f->Read(off, buf, read_size, option, out_bytes_read);
    }
    Result WriteFile(FsFile *file, s64 off, const void *buf, u64 write_size, u32 option) override {
        log_write("[HAZE] WriteFile()\n");
        fs::File* f;
        std::memcpy(&f, &file->s, sizeof(f));
        return f->Write(off, buf, write_size, option);
    }
    void CloseFile(FsFile *file) override {
        log_write("[HAZE] CloseFile()\n");
        fs::File* f;
        std::memcpy(&f, &file->s, sizeof(f));
        if (f) {
            delete f;
        }
        std::memset(file, 0, sizeof(*file));
    }

    Result CreateDirectory(const char* path) override {
        log_write("[HAZE] DeleteFile(%s)\n", path);
        return m_fs->CreateDirectory(FixPath(path));
    }
    Result DeleteDirectoryRecursively(const char* path) override {
        log_write("[HAZE] DeleteDirectoryRecursively(%s)\n", path);
        return m_fs->DeleteDirectoryRecursively(FixPath(path));
    }
    Result RenameDirectory(const char *old_path, const char *new_path) override {
        log_write("[HAZE] RenameDirectory(%s -> %s)\n", old_path, new_path);
        return m_fs->RenameDirectory(FixPath(old_path), FixPath(new_path));
    }
    Result OpenDirectory(const char *path, u32 mode, FsDir *out_dir) override {
        auto fptr = new fs::Dir();
        const auto rc = m_fs->OpenDirectory(FixPath(path), mode, fptr);

        if (R_SUCCEEDED(rc)) {
            std::memcpy(&out_dir->s, &fptr, sizeof(fptr));
        } else {
            delete fptr;
        }

        log_write("[HAZE] OpenDirectory(%s) 0x%X\n", path, rc);
        return rc;
    }
    Result ReadDirectory(FsDir *d, s64 *out_total_entries, size_t max_entries, FsDirectoryEntry *buf) override {
        fs::Dir* f;
        std::memcpy(&f, &d->s, sizeof(f));
        const auto rc = f->Read(out_total_entries, max_entries, buf);
        log_write("[HAZE] ReadDirectory(%zd) 0x%X\n", *out_total_entries, rc);
        return rc;
    }
    Result GetDirectoryEntryCount(FsDir *d, s64 *out_count) override {
        fs::Dir* f;
        std::memcpy(&f, &d->s, sizeof(f));
        const auto rc = f->GetEntryCount(out_count);
        log_write("[HAZE] GetDirectoryEntryCount(%zd) 0x%X\n", *out_count, rc);
        return rc;
    }
    void CloseDirectory(FsDir *d) override {
        log_write("[HAZE] CloseDirectory()\n");
        fs::Dir* f;
        std::memcpy(&f, &d->s, sizeof(f));
        if (f) {
            delete f;
        }
        std::memset(d, 0, sizeof(*d));
    }

private:
    std::shared_ptr<fs::Fs> m_fs{};
};

// fake fs that allows for files to create r/w on the root.
// folders are not yet supported.
struct FsProxyVfs : FsProxyBase {
    using FsProxyBase::FsProxyBase;
    virtual ~FsProxyVfs() = default;

    auto GetFileName(const char* s) -> const char* {
        const auto file_name = std::strrchr(s, '/');
        if (!file_name || file_name[1] == '\0') {
            return nullptr;
        }
        return file_name + 1;
    }

    virtual Result GetEntryType(const char *path, FsDirEntryType *out_entry_type) {
        if (FixPath(path) == "/") {
            *out_entry_type = FsDirEntryType_Dir;
            R_SUCCEED();
        } else {
            const auto file_name = GetFileName(path);
            R_UNLESS(file_name, FsError_PathNotFound);

            const auto it = std::ranges::find_if(m_entries, [file_name](auto& e){
                return !strcasecmp(file_name, e.name);
            });
            R_UNLESS(it != m_entries.end(), FsError_PathNotFound);

            *out_entry_type = FsDirEntryType_File;
            R_SUCCEED();
        }
    }
    virtual Result CreateFile(const char* path, s64 size, u32 option) {
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
    virtual Result OpenFile(const char *path, u32 mode, FsFile *out_file) {
        const auto file_name = GetFileName(path);
        R_UNLESS(file_name, FsError_PathNotFound);

        const auto it = std::ranges::find_if(m_entries, [file_name](auto& e){
            return !strcasecmp(file_name, e.name);
        });
        R_UNLESS(it != m_entries.end(), FsError_PathNotFound);

        out_file->s.object_id = std::distance(m_entries.begin(), it);
        out_file->s.own_handle = mode;
        R_SUCCEED();
    }
    virtual Result GetFileSize(FsFile *file, s64 *out_size) {
        auto& e = m_entries[file->s.object_id];
        *out_size = e.file_size;
        R_SUCCEED();
    }
    virtual Result SetFileSize(FsFile *file, s64 size) {
        auto& e = m_entries[file->s.object_id];
        e.file_size = size;
        R_SUCCEED();
    }
    virtual Result ReadFile(FsFile *file, s64 off, void *buf, u64 read_size, u32 option, u64 *out_bytes_read) {
        // stub for now as it may confuse users who think that the returned file is valid.
        // the code below can be used to benchmark mtp reads.
        R_THROW(FsError_NotImplemented);
        // auto& e = m_entries[file->s.object_id];
        // read_size = std::min<s64>(e.file_size - off, read_size);
        // std::memset(buf, 0, read_size);
        // *out_bytes_read = read_size;
        // R_SUCCEED();
    }
    virtual Result WriteFile(FsFile *file, s64 off, const void *buf, u64 write_size, u32 option) {
        auto& e = m_entries[file->s.object_id];
        e.file_size = std::max<s64>(e.file_size, off + write_size);
        R_SUCCEED();
    }
    virtual void CloseFile(FsFile *file) {
        std::memset(file, 0, sizeof(*file));
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
    Result OpenDirectory(const char *path, u32 mode, FsDir *out_dir) override {
        std::memset(out_dir, 0, sizeof(*out_dir));
        R_SUCCEED();
    }
    Result ReadDirectory(FsDir *d, s64 *out_total_entries, size_t max_entries, FsDirectoryEntry *buf) override {
        max_entries = std::min<s64>(m_entries.size()- d->s.object_id, max_entries);
        std::memcpy(buf, m_entries.data() + d->s.object_id, max_entries * sizeof(*buf));
        d->s.object_id += max_entries;
        *out_total_entries = max_entries;
        R_SUCCEED();
    }
    Result GetDirectoryEntryCount(FsDir *d, s64 *out_count) override {
        *out_count = m_entries.size();
        R_SUCCEED();
    }
    void CloseDirectory(FsDir *d) override {
        std::memset(d, 0, sizeof(*d));
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

    Result GetEntryType(const char *path, FsDirEntryType *out_entry_type) override {
        R_TRY(FsProxyVfs::GetEntryType(path, out_entry_type));
        if (*out_entry_type == FsDirEntryType_File) {
            R_TRY(FailedIfNotEnabled());
        }
        R_SUCCEED();
    }
    Result CreateFile(const char* path, s64 size, u32 option) override {
        R_TRY(FailedIfNotEnabled());
        R_TRY(IsValidFileType(path));
        R_TRY(FsProxyVfs::CreateFile(path, size, option));
        R_SUCCEED();
    }
    Result OpenFile(const char *path, u32 mode, FsFile *out_file) override {
        R_TRY(FailedIfNotEnabled());
        R_TRY(IsValidFileType(path));
        R_TRY(FsProxyVfs::OpenFile(path, mode, out_file));
        log_write("[MTP] done file open: %s mode: 0x%X\n", path, mode);

        if (mode & FsOpenMode_Write) {
            const auto& e = m_entries[out_file->s.object_id];

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
    Result WriteFile(FsFile *file, s64 off, const void *buf, u64 write_size, u32 option) override {
        SCOPED_MUTEX(&g_shared_data.mutex);
        if (!g_shared_data.enabled) {
            log_write("[MTP] failing as not enabled\n");
            R_THROW(FsError_NotImplemented);
        }

        if (!g_shared_data.on_write || !g_shared_data.on_write(buf, write_size)) {
            log_write("[MTP] failing as not written\n");
            R_THROW(FsError_NotImplemented);
        }

        R_TRY(FsProxyVfs::WriteFile(file, off, buf, write_size, option));
        R_SUCCEED();
    }
    void CloseFile(FsFile *file) override {
        bool update{};
        {
            SCOPED_MUTEX(&g_shared_data.mutex);
            if (file->s.own_handle & FsOpenMode_Write) {
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

::haze::FsEntries g_fs_entries{};

void haze_callback(const ::haze::CallbackData *data) {
    auto& e = *data;

    switch (e.type) {
        case ::haze::CallbackType_OpenSession: log_write("[LIBHAZE] Opening Session\n"); break;
        case ::haze::CallbackType_CloseSession: log_write("[LIBHAZE] Closing Session\n"); break;

        case ::haze::CallbackType_CreateFile: log_write("[LIBHAZE] Creating File: %s\n", e.file.filename); break;
        case ::haze::CallbackType_DeleteFile: log_write("[LIBHAZE] Deleting File: %s\n", e.file.filename); break;

        case ::haze::CallbackType_RenameFile: log_write("[LIBHAZE] Rename File: %s -> %s\n", e.rename.filename, e.rename.newname); break;
        case ::haze::CallbackType_RenameFolder: log_write("[LIBHAZE] Rename Folder: %s -> %s\n", e.rename.filename, e.rename.newname); break;

        case ::haze::CallbackType_CreateFolder: log_write("[LIBHAZE] Creating Folder: %s\n", e.file.filename); break;
        case ::haze::CallbackType_DeleteFolder: log_write("[LIBHAZE] Deleting Folder: %s\n", e.file.filename); break;

        case ::haze::CallbackType_ReadBegin: log_write("[LIBHAZE] Reading File Begin: %s \n", e.file.filename); break;
        case ::haze::CallbackType_ReadProgress: log_write("\t[LIBHAZE] Reading File: offset: %lld size: %lld\n", e.progress.offset, e.progress.size); break;
        case ::haze::CallbackType_ReadEnd: log_write("[LIBHAZE] Reading File Finished: %s\n", e.file.filename); break;

        case ::haze::CallbackType_WriteBegin: log_write("[LIBHAZE] Writing File Begin: %s \n", e.file.filename); break;
        case ::haze::CallbackType_WriteProgress: log_write("\t[LIBHAZE] Writing File: offset: %lld size: %lld\n", e.progress.offset, e.progress.size); break;
        case ::haze::CallbackType_WriteEnd: log_write("[LIBHAZE] Writing File Finished: %s\n", e.file.filename); break;
    }

    App::NotifyFlashLed();
}

} // namespace

bool Init() {
    SCOPED_MUTEX(&g_mutex);
    if (g_is_running) {
        log_write("[MTP] already enabled, cannot open\n");
        return false;
    }

    g_fs_entries.emplace_back(std::make_shared<FsProxy>(std::make_shared<fs::FsNativeSd>(), "", "microSD card"));
    g_fs_entries.emplace_back(std::make_shared<FsProxy>(std::make_shared<fs::FsNativeImage>(FsImageDirectoryId_Nand), "image_nand", "Image nand"));
    g_fs_entries.emplace_back(std::make_shared<FsProxy>(std::make_shared<fs::FsNativeImage>(FsImageDirectoryId_Sd), "image_sd", "Image sd"));
    g_fs_entries.emplace_back(std::make_shared<FsDevNullProxy>("DevNull", "DevNull (Speed Test)"));
    g_fs_entries.emplace_back(std::make_shared<FsInstallProxy>("install", "Install (NSP, XCI, NSZ, XCZ)"));

    g_should_exit = false;
    if (!::haze::Initialize(haze_callback, THREAD_PRIO, THREAD_CORE, g_fs_entries)) {
        return false;
    }

    log_write("[MTP] started\n");
    return g_is_running = true;
}

void Exit() {
    SCOPED_MUTEX(&g_mutex);
    if (!g_is_running) {
        return;
    }

    ::haze::Exit();
    g_is_running = false;
    g_should_exit = true;
    g_fs_entries.clear();

    log_write("[MTP] exitied\n");
}

void InitInstallMode(OnInstallStart on_start, OnInstallWrite on_write, OnInstallClose on_close) {
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

} // namespace sphaira::haze
