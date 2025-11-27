#include "utils/devoptab_common.hpp"
#include "defines.hpp"
#include "log.hpp"

#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <string>
#include <cstring>
#include <smb2/smb2.h>
#include <smb2/libsmb2.h>
#include <minIni.h>

namespace sphaira::devoptab {
namespace {

struct Device final : common::MountDevice {
    using MountDevice::MountDevice;
    ~Device();

private:
    bool fix_path(const char* str, char* out, bool strip_leading_slash = false) override {
        return common::fix_path(str, out, true);
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

private:
    smb2_context* smb2{};
    bool mounted{};
};

struct File {
    smb2fh* fd;
};

struct Dir {
    smb2dir* dir;
};

void fill_stat(struct stat* st, const smb2_stat_64* smb2_st) {
    if (smb2_st->smb2_type == SMB2_TYPE_FILE) {
        st->st_mode = S_IFREG;
    } else if (smb2_st->smb2_type == SMB2_TYPE_DIRECTORY) {
        st->st_mode = S_IFDIR;
    } else if (smb2_st->smb2_type == SMB2_TYPE_LINK) {
        st->st_mode = S_IFLNK;
    } else {
        log_write("[SMB2] Unknown file type: %u\n", smb2_st->smb2_type);
        st->st_mode = S_IFCHR; // will be skipped by stdio readdir wrapper.
    }

    st->st_ino = smb2_st->smb2_ino;
    st->st_nlink = smb2_st->smb2_nlink;
    st->st_size = smb2_st->smb2_size;
    st->st_atime = smb2_st->smb2_atime;
    st->st_mtime = smb2_st->smb2_mtime;
    st->st_ctime = smb2_st->smb2_ctime;
}

Device::~Device() {
    if (this->smb2) {
        if (this->mounted) {
            smb2_disconnect_share(this->smb2);
        }

        smb2_destroy_context(this->smb2);
    }
}

bool Device::Mount() {
    if (mounted) {
        return true;
    }

    if (!this->smb2) {
        this->smb2 = smb2_init_context();
        if (!this->smb2) {
            log_write("[SMB2] smb2_init_context() failed\n");
            return false;
        }

        smb2_set_security_mode(this->smb2, SMB2_NEGOTIATE_SIGNING_ENABLED);

        if (!this->config.user.empty()) {
            smb2_set_user(this->smb2, this->config.user.c_str());
        }

        if (!this->config.pass.empty()) {
            smb2_set_password(this->smb2, this->config.pass.c_str());
        }

        const auto domain = this->config.extra.find("domain");
        if (domain != this->config.extra.end()) {
            smb2_set_domain(this->smb2, domain->second.c_str());
        }

        const auto workstation = this->config.extra.find("workstation");
        if (workstation != this->config.extra.end()) {
            smb2_set_workstation(this->smb2, workstation->second.c_str());
        }

        if (config.timeout > 0) {
            smb2_set_timeout(this->smb2, this->config.timeout);
        }
    }

    // due to a bug in old sphira, i incorrectly prepended the url with smb:// rather than smb2://
    auto url = this->config.url;
    if (!url.ends_with('/')) {
        url += '/';
    }

    auto smb2_url = smb2_parse_url(this->smb2, url.c_str());
    if (!smb2_url) {
        log_write("[SMB2] smb2_parse_url() failed: %s\n", smb2_get_error(this->smb2));
        return false;
    }
    ON_SCOPE_EXIT(smb2_destroy_url(smb2_url));

    const auto ret = smb2_connect_share(this->smb2, smb2_url->server, smb2_url->share, smb2_url->user);
    if (ret) {
        log_write("[SMB2] smb2_connect_share() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
        return false;
    }

    this->mounted = true;
    return true;
}

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);

    file->fd = smb2_open(this->smb2, path, flags);
    if (!file->fd) {
        log_write("[SMB2] smb2_open() failed: %s\n", smb2_get_error(this->smb2));
        return -EIO;
    }

    return 0;
}

int Device::devoptab_close(void *fd) {
    auto file = static_cast<File*>(fd);

    smb2_close(this->smb2, file->fd);
    return 0;
}

ssize_t Device::devoptab_read(void *fd, char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);

    const auto max_read = smb2_get_max_read_size(this->smb2);
    size_t bytes_read = 0;

    while (bytes_read < len) {
        const auto to_read = std::min<size_t>(len - bytes_read, max_read);
        const auto ret = smb2_read(this->smb2, file->fd, (u8*)ptr, to_read);

        if (ret < 0) {
            log_write("[SMB2] smb2_read() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
            return ret;
        }

        ptr += ret;
        bytes_read += ret;

        if (ret < to_read) {
            break;
        }
    }

    return bytes_read;
}

ssize_t Device::devoptab_write(void *fd, const char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);

    const auto max_write = smb2_get_max_write_size(this->smb2);
    size_t written = 0;

    while (written < len) {
        const auto to_write = std::min<size_t>(len - written, max_write);
        const auto ret = smb2_write(this->smb2, file->fd, (const u8*)ptr, to_write);

        if (ret < 0) {
            log_write("[SMB2] smb2_write() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
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
    const auto ret = smb2_lseek(this->smb2, file->fd, pos, dir, &current_offset);
    if (ret < 0) {
        log_write("[SMB2] smb2_lseek() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
        return ret;
    }

    return current_offset;
}

int Device::devoptab_fstat(void *fd, struct stat *st) {
    auto file = static_cast<File*>(fd);

    smb2_stat_64 smb2_st{};
    const auto ret = smb2_fstat(this->smb2, file->fd, &smb2_st);
    if (ret < 0) {
        log_write("[SMB2] smb2_fstat() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
        return ret;
    }

    fill_stat(st, &smb2_st);
    return 0;
}

int Device::devoptab_unlink(const char *path) {
    const auto ret = smb2_unlink(this->smb2, path);
    if (ret) {
        log_write("[SMB2] smb2_unlink() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_rename(const char *oldName, const char *newName) {
    const auto ret = smb2_rename(this->smb2, oldName, newName);
    if (ret) {
        log_write("[SMB2] smb2_rename() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_mkdir(const char *path, int mode) {
    const auto ret = smb2_mkdir(this->smb2, path);
    if (ret) {
        log_write("[SMB2] smb2_mkdir() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_rmdir(const char *path) {
    const auto ret = smb2_rmdir(this->smb2, path);
    if (ret) {
        log_write("[SMB2] smb2_rmdir() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    auto dir = static_cast<Dir*>(fd);

    dir->dir = smb2_opendir(this->smb2, path);
    if (!dir->dir) {
        log_write("[SMB2] smb2_opendir() failed: %s\n", smb2_get_error(this->smb2));
        return -EIO;
    }

    return 0;
}

int Device::devoptab_dirreset(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    smb2_rewinddir(this->smb2, dir->dir);
    return 0;
}

int Device::devoptab_dirnext(void* fd, char *filename, struct stat *filestat) {
    auto dir = static_cast<Dir*>(fd);

    if (!dir->dir) {
        return EINVAL;
    }

    const auto entry = smb2_readdir(this->smb2, dir->dir);
    if (!entry) {
        return -ENOENT;
    }

    std::strncpy(filename, entry->name, NAME_MAX);
    filename[NAME_MAX - 1] = '\0';
    fill_stat(filestat, &entry->st);

    return 0;
}

int Device::devoptab_dirclose(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    smb2_closedir(this->smb2, dir->dir);
    return 0;
}

int Device::devoptab_lstat(const char *path, struct stat *st) {
    smb2_stat_64 smb2_st{};
    const auto ret = smb2_stat(this->smb2, path, &smb2_st);
    if (ret) {
        log_write("[SMB2] smb2_stat() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
        return ret;
    }

    fill_stat(st, &smb2_st);
    return 0;
}

int Device::devoptab_ftruncate(void *fd, off_t len) {
    auto file = static_cast<File*>(fd);

    const auto ret = smb2_ftruncate(this->smb2, file->fd, len);
    if (ret) {
        log_write("[SMB2] smb2_ftruncate() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_statvfs(const char *path, struct statvfs *buf) {
    struct smb2_statvfs smb2_st{};
    const auto ret = smb2_statvfs(this->smb2, path, &smb2_st);
    if (ret) {
        log_write("[SMB2] smb2_statvfs() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
        return ret;
    }

    buf->f_bsize   = smb2_st.f_bsize;
    buf->f_frsize  = smb2_st.f_frsize;
    buf->f_blocks  = smb2_st.f_blocks;
    buf->f_bfree   = smb2_st.f_bfree;
    buf->f_bavail  = smb2_st.f_bavail;
    buf->f_files   = smb2_st.f_files;
    buf->f_ffree   = smb2_st.f_ffree;
    buf->f_favail  = smb2_st.f_favail;
    buf->f_fsid    = smb2_st.f_fsid;
    buf->f_flag    = smb2_st.f_flag;
    buf->f_namemax = smb2_st.f_namemax;

    return 0;
}

int Device::devoptab_fsync(void *fd) {
    auto file = static_cast<File*>(fd);

    const auto ret = smb2_fsync(this->smb2, file->fd);
    if (ret) {
        log_write("[SMB2] smb2_fsync() failed: %s errno: %s\n", smb2_get_error(this->smb2), std::strerror(-ret));
        return ret;
    }

    return 0;
}

} // namespace

Result MountSmb2All() {
    return common::MountNetworkDevice([](const common::MountConfig& cfg) {
            return std::make_unique<Device>(cfg);
        },
        sizeof(File), sizeof(Dir),
        "SMB"
    );
}

} // namespace sphaira::devoptab
