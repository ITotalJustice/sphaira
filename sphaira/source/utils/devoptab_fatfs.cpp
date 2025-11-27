#include "utils/devoptab_common.hpp"
#include "utils/profile.hpp"

#include "log.hpp"
#include "defines.hpp"

#include <fcntl.h>

#include <string>
#include <vector>
#include <memory>
#include <cstring>
#include <sys/stat.h>
#include <ff.h>

namespace sphaira::devoptab {
namespace {

enum BisMountType {
    BisMountType_PRODINFOF,
    BisMountType_SAFE,
    BisMountType_USER,
    BisMountType_SYSTEM,
};

struct FatStorageEntry {
    FsStorage storage;
    std::unique_ptr<common::LruBufferedData> buffered;
    FATFS fs;
};

struct BisMountEntry {
    const FsBisPartitionId id;
    const char* volume_name;
    const char* mount_name;
};

constexpr BisMountEntry BIS_MOUNT_ENTRIES[] {
    [BisMountType_PRODINFOF] = { FsBisPartitionId_CalibrationFile, "PRODINFOF", "PRODINFOF:/" },
    [BisMountType_SAFE] = { FsBisPartitionId_SafeMode, "SAFE", "SAFE:/" },
    [BisMountType_USER] = { FsBisPartitionId_User, "USER", "USER:/" },
    [BisMountType_SYSTEM] = { FsBisPartitionId_System, "SYSTEM", "SYSTEM:/" },
};
static_assert(std::size(BIS_MOUNT_ENTRIES) == FF_VOLUMES);

FatStorageEntry g_fat_storage[FF_VOLUMES];

// todo: replace with off+size and have the data be in another struct
// in order to be more lcache efficient.
struct FsStorageSource final : yati::source::Base {
    FsStorageSource(FsStorage* s) : m_s{*s} {

    }

    Result Read(void* buf, s64 off, s64 size, u64* bytes_read) override {
        R_TRY(fsStorageRead(&m_s, off, buf, size));
        *bytes_read = size;
        R_SUCCEED();
    }

    Result GetSize(s64* size) {
        return fsStorageGetSize(&m_s, size);
    }

private:
    FsStorage m_s;
};

struct File {
    FIL* files;
    u32 file_count;
    size_t off;
    char path[PATH_MAX];
};

struct Dir {
    FDIR dir;
    char path[PATH_MAX];
};

struct Device final : common::MountDevice {
    Device(BisMountType type, const common::MountConfig& _config)
    : MountDevice{_config}
    , m_type{type} {

    }

    ~Device();

private:
    bool fix_path(const char* str, char* out, bool strip_leading_slash = false) override {
        std::strcpy(out, str);
        return true;
    }

    bool Mount() override;
    int devoptab_open(void *fileStruct, const char *path, int flags, int mode) override;
    int devoptab_close(void *fd) override;
    ssize_t devoptab_read(void *fd, char *ptr, size_t len) override;
    ssize_t devoptab_seek(void *fd, off_t pos, int dir) override;
    int devoptab_fstat(void *fd, struct stat *st) override;
    int devoptab_diropen(void* fd, const char *path) override;
    int devoptab_dirreset(void* fd) override;
    int devoptab_dirnext(void* fd, char *filename, struct stat *filestat) override;
    int devoptab_dirclose(void* fd) override;
    int devoptab_lstat(const char *path, struct stat *st) override;

private:
    const BisMountType m_type;
    bool mounted{};
};

auto is_archive(BYTE attr) -> bool {
    const auto archive_attr = AM_DIR | AM_ARC;
    return (attr & archive_attr) == archive_attr;
}

u64 get_size_from_files(const File* file) {
    u64 size = 0;
    for (u32 i = 0; i < file->file_count; i++) {
        size += f_size(&file->files[i]);
    }
    return size;
}

FIL* get_current_file(File* file) {
    auto off = file->off;
    for (u32 i = 0; i < file->file_count; i++) {
        auto fil = &file->files[i];
        if (off <= f_size(fil)) {
            return fil;
        }
        off -= f_size(fil);
    }
    return NULL;
}

// adjusts current file pos and sets the rest of files to 0.
void set_current_file_pos(File* file) {
    s64 off = file->off;
    for (u32 i = 0; i < file->file_count; i++) {
        auto fil = &file->files[i];
        if (off >= 0 && off < f_size(fil)) {
            f_lseek(fil, off);
        } else {
            f_rewind(fil);
        }
        off -= f_size(fil);
    }
}

void fill_stat(const char* path, const FILINFO* fno, struct stat *st) {
    std::memset(st, 0, sizeof(*st));

    st->st_nlink = 1;

    struct tm tm{};
    tm.tm_sec = (fno->ftime & 0x1F) << 1;
    tm.tm_min = (fno->ftime >> 5) & 0x3F;
    tm.tm_hour = (fno->ftime >> 11);
    tm.tm_mday = (fno->fdate & 0x1F);
    tm.tm_mon = ((fno->fdate >> 5) & 0xF) - 1;
    tm.tm_year = (fno->fdate >> 9) + 80;

    st->st_atime = std::mktime(&tm);
    st->st_mtime = st->st_atime;
    st->st_ctime = st->st_atime;

    // fake file.
    if (path && is_archive(fno->fattrib)) {
        st->st_size = 0;
        char file_path[256];
        for (u16 i = 0; i < 256; i++) {
            std::snprintf(file_path, sizeof(file_path), "%s/%02u", path, i);
            FILINFO file_info{};
            if (FR_OK != f_stat(file_path, &file_info)) {
                break;
            }

            st->st_size += file_info.fsize;
        }

        st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    } else if (fno->fattrib & AM_DIR) {
        st->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
    } else {
        st->st_size = fno->fsize;
        st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    }
}

Device::~Device() {
    if (mounted) {
        auto& fat = g_fat_storage[m_type];
        f_unmount(BIS_MOUNT_ENTRIES[m_type].mount_name);
        fat.buffered.reset();
        fsStorageClose(&fat.storage);
    }
}

bool Device::Mount() {
    if (mounted) {
        return true;
    }

    auto& fat = g_fat_storage[m_type];

    if (!serviceIsActive(&fat.storage.s)) {
        const auto res = fsOpenBisStorage(&fat.storage, BIS_MOUNT_ENTRIES[m_type].id);
        if (R_FAILED(res)) {
            log_write("[FATFS] fsOpenBisStorage(%d) failed: 0x%x\n", BIS_MOUNT_ENTRIES[m_type].id, res);
            return false;
        }
    } else {
        log_write("[FATFS] Storage for %s already opened\n", BIS_MOUNT_ENTRIES[m_type].mount_name);
    }

    if (!fat.buffered) {
        auto source = std::make_shared<FsStorageSource>(&fat.storage);

        s64 size;
        if (R_FAILED(source->GetSize(&size))) {
            log_write("[FATFS] Failed to get size of storage source\n");
            return false;
        }

        fat.buffered = std::make_unique<common::LruBufferedData>(source, size);
        if (!fat.buffered) {
            log_write("[FATFS] Failed to create LruBufferedData\n");
            return false;
        }
    }

    if (FR_OK != f_mount(&fat.fs, BIS_MOUNT_ENTRIES[m_type].mount_name, 1)) {
        log_write("[FATFS] f_mount(%s) failed\n", BIS_MOUNT_ENTRIES[m_type].mount_name);
        return false;
    }

    log_write("[FATFS] Mounted %s at %s\n", BIS_MOUNT_ENTRIES[m_type].volume_name, BIS_MOUNT_ENTRIES[m_type].mount_name);
    return mounted = true;
}

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);

    // todo: init array
    // todo: handle dir.
    FIL fil{};
    if (FR_OK == f_open(&fil, path, FA_READ)) {
        file->files = (FIL*)std::malloc(sizeof(*file->files));
        if (!file->files) {
            return -ENOMEM;
        }

        file->file_count = 1;
        std::memcpy(file->files, &fil, sizeof(*file->files));
        // todo: check what error code is returned here.
    } else {
        FILINFO info{};
        if (FR_OK != f_stat(path, &info)) {
            return -ENOENT;
        }

        if (!(info.fattrib & AM_ARC)) {
            return -ENOENT;
        }

        char file_path[256];
        for (u16 i = 0; i < 256; i++) {
            std::memset(&fil, 0, sizeof(fil));
            std::snprintf(file_path, sizeof(file_path), "%s/%02u", path, i);

            if (FR_OK != f_open(&fil, file_path, FA_READ)) {
                break;
            }

            file->files = (FIL*)std::realloc(file->files, (i + 1) * sizeof(*file->files));
            if (!file->files) {
                return -ENOMEM;
            }

            std::memcpy(&file->files[i], &fil, sizeof(fil));
            file->file_count++;
        }
    }

    if (!file->files) {
        return -ENOENT;
    }

    std::snprintf(file->path, sizeof(file->path), "%s", path);
    return 0;
}

int Device::devoptab_close(void *fd) {
    auto file = static_cast<File*>(fd);

    for (u32 i = 0; i < file->file_count; i++) {
        f_close(&file->files[i]);
    }

    std::free(file->files);
    return 0;
}

ssize_t Device::devoptab_read(void *fd, char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);
    UINT total_bytes_read = 0;

    while (len) {
        UINT bytes_read;
        auto fil = get_current_file(file);
        if (!fil) {
            log_write("[FATFS] failed to get fil\n");
            return -EIO;
        }

        if (FR_OK != f_read(fil, ptr, len, &bytes_read)) {
            return -EIO;
        }

        if (!bytes_read) {
            break;
        }

        len -= bytes_read;
        file->off += bytes_read;
        total_bytes_read += bytes_read;
    }

    return total_bytes_read;
}

ssize_t Device::devoptab_seek(void *fd, off_t pos, int dir) {
    auto file = static_cast<File*>(fd);
    const auto size = get_size_from_files(file);

    if (dir == SEEK_CUR) {
        pos += file->off;
    } else if (dir == SEEK_END) {
        pos = size;
    }

    file->off = std::clamp<u64>(pos, 0, size);
    set_current_file_pos(file);

    return file->off;
}

int Device::devoptab_fstat(void *fd, struct stat *st) {
    auto file = static_cast<File*>(fd);

    /* Only fill the attr and size field, leaving the timestamp blank. */
    FILINFO info{};
    info.fsize = get_size_from_files(file);

    /* Fill stat info. */
    fill_stat(nullptr, &info, st);

    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    auto dir = static_cast<Dir*>(fd);

    log_write("[FATFS] diropen: %s\n", path);
    if (FR_OK != f_opendir(&dir->dir, path)) {
        log_write("[FATFS] f_opendir(%s) failed\n", path);
        return -ENOENT;
    }

    log_write("[FATFS] Opened dir: %s\n", path);
    return 0;
}

int Device::devoptab_dirreset(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    if (FR_OK != f_rewinddir(&dir->dir)) {
        return -EIO;
    }

    return 0;
}

int Device::devoptab_dirnext(void* fd, char *filename, struct stat *filestat) {
    auto dir = static_cast<Dir*>(fd);
    FILINFO fno{};

    if (FR_OK != f_readdir(&dir->dir, &fno)) {
        return -EIO;
    }

    if (!fno.fname[0]) {
        return -EIO;
    }

    std::strcpy(filename, fno.fname);
    fill_stat(dir->path, &fno, filestat);

    return 0;
}

int Device::devoptab_dirclose(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    if (FR_OK != f_closedir(&dir->dir)) {
        return -EIO;
    }

    return 0;
}

int Device::devoptab_lstat(const char *path, struct stat *st) {
    FILINFO fno;
    if (FR_OK != f_stat(path, &fno)) {
        return -ENOENT;
    }

    fill_stat(path, &fno, st);
    return 0;
}

} // namespace

Result MountFatfsAll() {
    for (u32 i = 0; i < FF_VOLUMES; i++) {
        const auto& bis = BIS_MOUNT_ENTRIES[i];

        common::MountConfig config{};
        config.read_only = true;
        config.dump_hidden = true;

        if (!common::MountNetworkDevice2(
            std::make_unique<Device>((BisMountType)i, config),
            config,
            sizeof(File), sizeof(Dir),
            bis.volume_name, bis.mount_name
        )) {
            log_write("[FATFS] Failed to mount %s\n", bis.volume_name);
        }
    }

    R_SUCCEED();
}

} // namespace sphaira::devoptab

extern "C" {

const char* VolumeStr[] {
    sphaira::devoptab::BIS_MOUNT_ENTRIES[0].volume_name,
    sphaira::devoptab::BIS_MOUNT_ENTRIES[1].volume_name,
    sphaira::devoptab::BIS_MOUNT_ENTRIES[2].volume_name,
    sphaira::devoptab::BIS_MOUNT_ENTRIES[3].volume_name,
};

Result fatfs_read(u8 num, void* dst, u64 offset, u64 size) {
    auto& fat = sphaira::devoptab::g_fat_storage[num];
    return fat.buffered->Read2(dst, offset, size);
}

// libusbhsfs also defines these, so only define if not using it.
#ifndef ENABLE_LIBUSBHSFS
void* ff_memalloc (UINT msize) {
    return std::malloc(msize);
}

void ff_memfree (void* mblock) {
    std::free(mblock);
}
#endif // ENABLE_LIBUSBHSFS

} // extern "C"
