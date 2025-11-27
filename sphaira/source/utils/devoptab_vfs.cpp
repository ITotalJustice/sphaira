#include "utils/devoptab_common.hpp"
#include "defines.hpp"
#include "log.hpp"

#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <string>
#include <cstring>

namespace sphaira::devoptab {
namespace {

struct Device final : common::MountDevice {
    Device(const common::MountConfig& _config)
    : common::MountDevice{_config}
    , m_root{config.url} {
    }

private:
    bool fix_path(const char* str, char* out, bool strip_leading_slash = false) override {
        char temp[PATH_MAX]{};
        if (!common::fix_path(str, temp, false)) {
            return false;
        }

        std::snprintf(out, PATH_MAX, "%s/%s", m_root.c_str(), temp);
        log_write("[VFS] fixed path: %s -> %s\n", str, out);
        return true;
    }

    bool Mount() override;
    int devoptab_open(void *fileStruct, const char *path, int flags, int mode) override;
    int devoptab_close(void *fd) override;
    ssize_t devoptab_read(void *fd, char *ptr, size_t len) override;
    ssize_t devoptab_write(void *fd, const char *ptr, size_t len) override;
    ssize_t devoptab_seek(void *fd, off_t pos, int dir) override;
    int devoptab_fstat(void *fd, struct stat *st) override;
    int devoptab_unlink(const char *path) override;
    int devoptab_rename(const char *oldName, const char *newName) override;
    int devoptab_mkdir(const char *path, int mode) override;
    int devoptab_rmdir(const char *path) override;
    int devoptab_diropen(void* fd, const char *path) override;
    int devoptab_dirreset(void* fd) override;
    int devoptab_dirnext(void* fd, char *filename, struct stat *filestat) override;
    int devoptab_dirclose(void* fd) override;
    int devoptab_lstat(const char *path, struct stat *st) override;
    int devoptab_ftruncate(void *fd, off_t len) override;
    int devoptab_statvfs(const char *path, struct statvfs *buf) override;
    int devoptab_fsync(void *fd) override;
    int devoptab_utimes(const char *path, const struct timeval times[2]) override;

private:
    const std::string m_root{};
    bool mounted{};
};

struct File {
    int fd;
};

struct Dir {
    DIR* dir;
};

bool Device::Mount() {
    if (mounted) {
        return true;
    }

    log_write("[VFS] Mounting %s\n", this->config.url.c_str());

    if (m_root.empty()) {
        log_write("[VFS] Empty root path\n");
        return false;
    }

    log_write("[VFS] Mounted %s\n", this->config.url.c_str());
    return mounted = true;
}

int return_errno(int err = EIO) {
    return errno ? -errno : -err;
}

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);

    const auto ret = open(path, flags, mode);
    if (ret < 0) {
        return return_errno();
    }

    file->fd = ret;
    return 0;
}

int Device::devoptab_close(void *fd) {
    auto file = static_cast<File*>(fd);

    close(file->fd);
    return 0;
}

ssize_t Device::devoptab_read(void *fd, char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);

    const auto ret = read(file->fd, ptr, len);
    if (ret < 0) {
        return return_errno();
    }

    return ret;
}

ssize_t Device::devoptab_write(void *fd, const char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);

    const auto ret = write(file->fd, ptr, len);
    if (ret < 0) {
        return return_errno();
    }

    return ret;
}

ssize_t Device::devoptab_seek(void *fd, off_t pos, int dir) {
    auto file = static_cast<File*>(fd);

    return lseek(file->fd, pos, dir);
}

int Device::devoptab_fstat(void *fd, struct stat *st) {
    auto file = static_cast<File*>(fd);

    const auto ret = fstat(file->fd, st);
    if (ret < 0) {
        return return_errno();
    }

    return 0;
}

int Device::devoptab_unlink(const char *path) {
    const auto ret = unlink(path);
    if (ret < 0) {
        return return_errno();
    }

    return 0;
}

int Device::devoptab_rename(const char *oldName, const char *newName) {
    const auto ret = rename(oldName, newName);
    if (ret < 0) {
        return return_errno();
    }

    return 0;
}

int Device::devoptab_mkdir(const char *path, int mode) {
    const auto ret = mkdir(path, mode);
    if (ret < 0) {
        return return_errno();
    }

    return 0;
}

int Device::devoptab_rmdir(const char *path) {
    const auto ret = rmdir(path);
    if (ret < 0) {
        return return_errno();
    }

    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    auto dir = static_cast<Dir*>(fd);

    auto ret = opendir(path);
    if (!ret) {
        return return_errno();
    }

    dir->dir = ret;
    return 0;
}

int Device::devoptab_dirreset(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    rewinddir(dir->dir);
    return 0;
}

int Device::devoptab_dirnext(void* fd, char *filename, struct stat *filestat) {
    auto dir = static_cast<Dir*>(fd);

    const auto entry = readdir(dir->dir);
    if (!entry) {
        return return_errno(ENOENT);
    }

    filestat->st_ino = entry->d_ino;
    filestat->st_mode = entry->d_type << 12; // DT_* to S_IF*
    filestat->st_nlink = 1; // unknown

    std::strncpy(filename, entry->d_name, NAME_MAX);
    filename[NAME_MAX - 1] = '\0';

    return 0;
}

int Device::devoptab_dirclose(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    closedir(dir->dir);
    return 0;
}

int Device::devoptab_lstat(const char *path, struct stat *st) {
    const auto ret = lstat(path, st);
    if (ret < 0) {
        return return_errno();
    }

    return 0;
}

int Device::devoptab_ftruncate(void *fd, off_t len) {
    auto file = static_cast<File*>(fd);

    const auto ret = ftruncate(file->fd, len);
    if (ret < 0) {
        return return_errno();
    }

    return 0;
}

int Device::devoptab_statvfs(const char *path, struct statvfs *buf) {
    const auto ret = statvfs(path, buf);
    if (ret < 0) {
        return return_errno();
    }

    return 0;
}

int Device::devoptab_fsync(void *fd) {
    auto file = static_cast<File*>(fd);

    const auto ret = fsync(file->fd);
    if (ret < 0) {
        return return_errno();
    }

    return 0;
}

int Device::devoptab_utimes(const char *path, const struct timeval times[2]) {
    const auto ret = utimes(path, times);
    if (ret < 0) {
        return return_errno();
    }

    return 0;
}

} // namespace

Result MountVfsAll() {
    return common::MountNetworkDevice([](const common::MountConfig& cfg) {
            return std::make_unique<Device>(cfg);
        },
        sizeof(File), sizeof(Dir),
        "VFS"
    );
}

} // namespace sphaira::devoptab
