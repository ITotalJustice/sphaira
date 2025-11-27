
#include "utils/devoptab.hpp"
#include "utils/devoptab_common.hpp"
#include "defines.hpp"
#include "log.hpp"

#include "yati/nx/nxdumptool/defines.h"
#include "yati/nx/nxdumptool/core/save.h"

#include <cstring>
#include <cerrno>
#include <array>
#include <memory>
#include <algorithm>

namespace sphaira::devoptab {
namespace {

struct File {
    save_fs_list_entry_t entry;
    allocation_table_storage_ctx_t storage;
    size_t off;
};

struct DirNext {
    u32 next_directory;
    u32 next_file;
};

struct Dir {
    save_fs_list_entry_t entry;
    u32 next_directory;
    u32 next_file;
};

struct Device final : common::MountDevice {
    Device(save_ctx_t* _ctx, const common::MountConfig& _config)
    : MountDevice{_config}
    , ctx{_ctx} {
        file_table = &ctx->save_filesystem_core.file_table;
    }

    ~Device() {
        save_close_savefile(&this->ctx);
    }

private:
    bool Mount() override { return true; }
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
    save_ctx_t* ctx;
    hierarchical_save_file_table_ctx_t* file_table;
};

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);

    if (!save_hierarchical_file_table_get_file_entry_by_path(this->file_table, path, &file->entry)) {
        return -ENOENT;
    }

    if (!save_open_fat_storage(&this->ctx->save_filesystem_core, &file->storage, file->entry.value.save_file_info.start_block)) {
        return -ENOENT;
    }

    return 0;
}

int Device::devoptab_close(void *fd) {
    auto file = static_cast<File*>(fd);
    std::memset(file, 0, sizeof(*file));

    return 0;
}

ssize_t Device::devoptab_read(void *fd, char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);
    len = std::min(len, file->entry.value.save_file_info.length - file->off);

    if (!len) {
        return 0;
    }

    // todo: maybe eof here?
    const auto bytes_read = save_allocation_table_storage_read(&file->storage, ptr, file->off, len);
    if (!bytes_read) {
        return -ENOENT;
    }

    file->off += bytes_read;
    return bytes_read;
}

ssize_t Device::devoptab_seek(void *fd, off_t pos, int dir) {
    auto file = static_cast<File*>(fd);

    if (dir == SEEK_CUR) {
        pos += file->off;
    } else if (dir == SEEK_END) {
        pos = file->entry.value.save_file_info.length;
    }

    return file->off = std::clamp<u64>(pos, 0, file->entry.value.save_file_info.length);
}

int Device::devoptab_fstat(void *fd, struct stat *st) {
    auto file = static_cast<File*>(fd);

    st->st_nlink = 1;
    st->st_size = file->entry.value.save_file_info.length;
    st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    auto dir = static_cast<Dir*>(fd);

    if (!std::strcmp(path, "/")) {
        save_entry_key_t key{};
        const auto idx = save_fs_list_get_index_from_key(&this->file_table->directory_table, &key, NULL);
        if (idx == 0xFFFFFFFF) {
            return -ENOENT;
        }

        if (!save_fs_list_get_value(&this->file_table->directory_table, idx, &dir->entry)) {
            return -ENOENT;
        }
    } else if (!save_hierarchical_directory_table_get_file_entry_by_path(this->file_table, path, &dir->entry)) {
        return -ENOENT;
    }

    dir->next_file = dir->entry.value.save_find_position.next_file;
    dir->next_directory = dir->entry.value.save_find_position.next_directory;

    return 0;
}

int Device::devoptab_dirreset(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    dir->next_file = dir->entry.value.save_find_position.next_file;
    dir->next_directory = dir->entry.value.save_find_position.next_directory;

    return 0;
}

int Device::devoptab_dirnext(void* fd, char *filename, struct stat *filestat) {
    auto dir = static_cast<Dir*>(fd);
    save_fs_list_entry_t entry{};

    if (dir->next_directory) {
        // todo: use save_allocation_table_storage_read for faster reads
        if (!save_fs_list_get_value(&this->file_table->directory_table, dir->next_directory, &entry)) {
            return -ENOENT;
        }

        filestat->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
        dir->next_directory = entry.value.next_sibling;
    }
    else if (dir->next_file) {
        // todo: use save_allocation_table_storage_read for faster reads
        if (!save_fs_list_get_value(&this->file_table->file_table, dir->next_file, &entry)) {
            return -ENOENT;
        }

        filestat->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        dir->next_file = entry.value.next_sibling;
    }
    else {
        return -ENOENT;
    }

    filestat->st_nlink = 1;
    std::strcpy(filename, entry.name);

    return 0;
}

int Device::devoptab_dirclose(void* fd) {
    auto dir = static_cast<Dir*>(fd);
    std::memset(dir, 0, sizeof(*dir));

    return 0;
}

int Device::devoptab_lstat(const char *path, struct stat *st) {
    st->st_nlink = 1;

    save_fs_list_entry_t entry{};

    // NOTE: this is very slow.
    if (save_hierarchical_file_table_get_file_entry_by_path(this->file_table, path, &entry)) {
        st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        st->st_size = entry.value.save_file_info.length;
    } else if (save_hierarchical_directory_table_get_file_entry_by_path(this->file_table, path, &entry)) {
        st->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
    } else {
        return -ENOENT;
    }

    return 0;
}

} // namespace

Result MountSaveSystem(u64 id, fs::FsPath& out_path) {
    static Mutex mutex{};
    SCOPED_MUTEX(&mutex);

    fs::FsPath path{};
    std::snprintf(path, sizeof(path), "SYSTEM:/save/%016lx", id);

    auto ctx = save_open_savefile(path, 0);
    if (!ctx) {
        R_THROW(0x1);
    }

    if (!common::MountReadOnlyIndexDevice(
        [&ctx](const common::MountConfig& config) {
            return std::make_unique<Device>(ctx, config);
        },
        sizeof(File), sizeof(Dir),
        "SAVE", out_path
    )) {
        log_write("[SAVE] Failed to mount %s\n", path.s);
        R_THROW(0x1);
    }

    R_SUCCEED();
}

} // namespace sphaira::devoptab
