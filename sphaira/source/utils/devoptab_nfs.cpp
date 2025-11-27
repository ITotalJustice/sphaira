#include "utils/devoptab_common.hpp"
#include "defines.hpp"
#include "log.hpp"

#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <string>
#include <cstring>
#include <libnfs.h>
#include <minIni.h>

namespace sphaira::devoptab {
namespace {

struct Device final : common::MountDevice {
    using MountDevice::MountDevice;
    ~Device();

private:
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
    nfs_context* nfs{};
    bool mounted{};
};

struct File {
    nfsfh* fd;
};

struct Dir {
    nfsdir* dir;
};

Device::~Device() {
    if (nfs) {
        if (mounted) {
            nfs_umount(nfs);
        }

        nfs_destroy_context(nfs);
    }
}

bool Device::Mount() {
    if (mounted) {
        return true;
    }

    log_write("[NFS] Mounting %s\n", this->config.url.c_str());

    if (!nfs) {
        nfs = nfs_init_context();
        if (!nfs) {
            log_write("[NFS] nfs_init_context() failed\n");
            return false;
        }

        const auto uid = this->config.extra.find("uid");
        if (uid != this->config.extra.end()) {
            const auto uid_val = ini_parse_getl(uid->second.c_str(), -1);
            if (uid_val < 0) {
                log_write("[NFS] Invalid uid value: %s\n", uid->second.c_str());
            } else {
                log_write("[NFS] Setting uid: %ld\n", uid_val);
                nfs_set_uid(nfs, uid_val);
            }
        }

        const auto gid = this->config.extra.find("gid");
        if (gid != this->config.extra.end()) {
            const auto gid_val = ini_parse_getl(gid->second.c_str(), -1);
            if (gid_val < 0) {
                log_write("[NFS] Invalid gid value: %s\n", gid->second.c_str());
            } else {
                log_write("[NFS] Setting gid: %ld\n", gid_val);
                nfs_set_gid(nfs, gid_val);
            }
        }

        const auto version = this->config.extra.find("version");
        if (version != this->config.extra.end()) {
            const auto version_val = ini_parse_getl(version->second.c_str(), -1);
            if (version_val != 3 && version_val != 4) {
                log_write("[NFS] Invalid version value: %s\n", version->second.c_str());
            } else {
                log_write("[NFS] Setting version: %ld\n", version_val);
                nfs_set_version(nfs, version_val);
            }
        }

        if (this->config.timeout > 0) {
            nfs_set_timeout(nfs, this->config.timeout);
            nfs_set_readonly(nfs, this->config.read_only);
        }
        // nfs_set_mountport(nfs, url->port);
    }

    // fix the url if needed.
    auto url = this->config.url;
    if (!url.starts_with("nfs://")) {
        log_write("[NFS] Prepending nfs:// to url: %s\n", url.c_str());
        url = "nfs://" + url;
    }

    auto nfs_url = nfs_parse_url_full(nfs, url.c_str());
    if (!nfs_url) {
        log_write("[NFS] nfs_parse_url() failed for url: %s\n", url.c_str());
        return false;
    }
    ON_SCOPE_EXIT(nfs_destroy_url(nfs_url));

    const auto ret = nfs_mount(nfs, nfs_url->server, nfs_url->path);
    if (ret) {
        log_write("[NFS] nfs_mount() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return false;
    }

    log_write("[NFS] Mounted %s\n", this->config.url.c_str());
    return mounted = true;
}

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);

    const auto ret = nfs_open(nfs, path, flags, &file->fd);
    if (ret) {
        log_write("[NFS] nfs_open() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_close(void *fd) {
    auto file = static_cast<File*>(fd);

    nfs_close(nfs, file->fd);
    return 0;
}

ssize_t Device::devoptab_read(void *fd, char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);

    // todo: uncomment this when it's fixed upstream.
    #if 0
    const auto ret = nfs_read(nfs, file->fd, ptr, len);
    if (ret < 0) {
        log_write("[NFS] nfs_read() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return ret;

    #else
    // work around for bug upsteam.
    const auto max_read = nfs_get_readmax(nfs);
    size_t bytes_read = 0;

    while (bytes_read < len) {
        const auto to_read = std::min<size_t>(len - bytes_read, max_read);
        const auto ret = nfs_read(nfs, file->fd, ptr, to_read);

        if (ret < 0) {
            log_write("[NFS] nfs_read() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
            return ret;
        }

        ptr += ret;
        bytes_read += ret;

        if (ret < to_read) {
            break;
        }
    }

    return bytes_read;
    #endif
}

ssize_t Device::devoptab_write(void *fd, const char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);

    // unlike read, writing the max size seems to work fine.
    const auto max_write = nfs_get_writemax(nfs);
    size_t written = 0;

    while (written < len) {
        const auto to_write = std::min<size_t>(len - written, max_write);
        const auto ret = nfs_write(nfs, file->fd, ptr, to_write);

        if (ret < 0) {
            log_write("[NFS] nfs_write() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
            return ret;
        }

        ptr += ret;
        written += ret;

        if (ret < to_write) {
            break;
        }
    }

    return written;
}

ssize_t Device::devoptab_seek(void *fd, off_t pos, int dir) {
    auto file = static_cast<File*>(fd);

    u64 current_offset = 0;
    const auto ret = nfs_lseek(nfs, file->fd, pos, dir, &current_offset);
    if (ret < 0) {
        log_write("[NFS] nfs_lseek() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return current_offset;
}

int Device::devoptab_fstat(void *fd, struct stat *st) {
    auto file = static_cast<File*>(fd);

    const auto ret = nfs_fstat(nfs, file->fd, st);
    if (ret) {
        log_write("[NFS] nfs_fstat() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_unlink(const char *path) {
    const auto ret = nfs_unlink(nfs, path);
    if (ret) {
        log_write("[NFS] nfs_unlink() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_rename(const char *oldName, const char *newName) {
    const auto ret = nfs_rename(nfs, oldName, newName);
    if (ret) {
        log_write("[NFS] nfs_rename() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_mkdir(const char *path, int mode) {
    const auto ret = nfs_mkdir(nfs, path);
    if (ret) {
        log_write("[NFS] nfs_mkdir() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_rmdir(const char *path) {
    const auto ret = nfs_rmdir(nfs, path);
    if (ret) {
        log_write("[NFS] nfs_rmdir() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    auto dir = static_cast<Dir*>(fd);

    const auto ret = nfs_opendir(nfs, path, &dir->dir);
    if (ret) {
        log_write("[NFS] nfs_opendir() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_dirreset(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    nfs_rewinddir(nfs, dir->dir);
    return 0;
}

int Device::devoptab_dirnext(void* fd, char *filename, struct stat *filestat) {
    auto dir = static_cast<Dir*>(fd);

    const auto entry = nfs_readdir(nfs, dir->dir);
    if (!entry) {
        return -ENOENT;
    }

    std::strncpy(filename, entry->name, NAME_MAX);
    filename[NAME_MAX - 1] = '\0';

    // not everything is needed, however we may as well fill it all in.
    filestat->st_dev = entry->dev;
    filestat->st_ino = entry->inode;
    filestat->st_mode = entry->mode;
    filestat->st_nlink = entry->nlink;
    filestat->st_uid = entry->uid;
    filestat->st_gid = entry->gid;
    filestat->st_size = entry->size;
    filestat->st_atime = entry->atime.tv_sec;
    filestat->st_mtime = entry->mtime.tv_sec;
    filestat->st_ctime = entry->ctime.tv_sec;
    filestat->st_blksize = entry->blksize;
    filestat->st_blocks = entry->blocks;

    return 0;
}

int Device::devoptab_dirclose(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    nfs_closedir(nfs, dir->dir);
    return 0;
}

int Device::devoptab_lstat(const char *path, struct stat *st) {
    const auto ret = nfs_stat(nfs, path, st);
    if (ret) {
        log_write("[NFS] nfs_stat() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_ftruncate(void *fd, off_t len) {
    auto file = static_cast<File*>(fd);

    const auto ret = nfs_ftruncate(nfs, file->fd, len);
    if (ret) {
        log_write("[NFS] nfs_ftruncate() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_statvfs(const char *path, struct statvfs *buf) {
    const auto ret = nfs_statvfs(nfs, path, buf);
    if (ret) {
        log_write("[NFS] nfs_statvfs() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_fsync(void *fd) {
    auto file = static_cast<File*>(fd);

    const auto ret = nfs_fsync(nfs, file->fd);
    if (ret) {
        log_write("[NFS] nfs_fsync() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_utimes(const char *path, const struct timeval times[2]) {
    // todo: nfs should accept const times, pr the fix.
    struct timeval times_copy[2];
    std::memcpy(times_copy, times, sizeof(times_copy));

    const auto ret = nfs_utimes(nfs, path, times_copy);
    if (ret) {
        log_write("[NFS] nfs_utimes() failed: %s errno: %s\n", nfs_get_error(nfs), std::strerror(-ret));
        return ret;
    }

    return 0;
}

} // namespace

Result MountNfsAll() {
    return common::MountNetworkDevice([](const common::MountConfig& cfg) {
            return std::make_unique<Device>(cfg);
        },
        sizeof(File), sizeof(Dir),
        "NFS"
    );
}

} // namespace sphaira::devoptab
