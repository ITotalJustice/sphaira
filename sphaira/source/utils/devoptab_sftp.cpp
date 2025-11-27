// NOTE (09/09/2025): do not use as it is unusably slow, even on local network.
// the issue isn't the ssh protocol (although it is slow). haven't looked into libssh2 yet
// it could be how they handle blocking. CPU usage is 0%, so its not that.

// NOTE (09/09/2025): its just reads that as super slow, which is even more strange!
// writes are very fast (for sftp), maxing switch wifi. what is going on???

// NOTE (09/09/2025): the issue was that fread was buffering, causing double reads.
// it would read the first 4mb, then read another 1kb.
// disabling buffering fixed the issue, and i have disabled buffering by default.
// buffering is now enabled only when requested.
#include "utils/devoptab_common.hpp"
#include "utils/profile.hpp"
#include "defines.hpp"
#include "log.hpp"

#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <string>
#include <cstring>

#include <libssh2.h>
#include <libssh2_sftp.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>

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

private:
    LIBSSH2_SESSION* m_session{};
    LIBSSH2_SFTP* m_sftp_session{};
    int m_socket{};
    bool m_is_ssh2_init{}; // set if libssh2_init() was successful.
    bool m_is_handshake_done{}; // set if handshake was successful.
    bool m_is_auth_done{}; // set if auth was successful.
    bool mounted{};
};

struct File {
    LIBSSH2_SFTP_HANDLE* fd{};
};

struct Dir {
    LIBSSH2_SFTP_HANDLE* fd{};
};

int convert_flags_to_sftp(int flags) {
    int sftp_flags = 0;

    if ((flags & O_ACCMODE) == O_RDONLY) {
        sftp_flags |= LIBSSH2_FXF_READ;
    } else if ((flags & O_ACCMODE) == O_WRONLY) {
        sftp_flags |= LIBSSH2_FXF_WRITE;
    } else if ((flags & O_ACCMODE) == O_RDWR) {
        sftp_flags |= LIBSSH2_FXF_READ | LIBSSH2_FXF_WRITE;
    }

    if (flags & O_CREAT) {
        sftp_flags |= LIBSSH2_FXF_CREAT;
    }
    if (flags & O_TRUNC) {
        sftp_flags |= LIBSSH2_FXF_TRUNC;
    }
    if (flags & O_APPEND) {
        sftp_flags |= LIBSSH2_FXF_APPEND;
    }
    if (flags & O_EXCL) {
        sftp_flags |= LIBSSH2_FXF_EXCL;
    }

    return sftp_flags;
}

int convert_mode_to_sftp(int mode) {
    int sftp_mode = 0;

    // permission bits.
    sftp_mode |= (mode & S_IRUSR) ? LIBSSH2_SFTP_S_IRUSR : 0;
    sftp_mode |= (mode & S_IWUSR) ? LIBSSH2_SFTP_S_IWUSR : 0;
    sftp_mode |= (mode & S_IXUSR) ? LIBSSH2_SFTP_S_IXUSR : 0;

    sftp_mode |= (mode & S_IRGRP) ? LIBSSH2_SFTP_S_IRGRP : 0;
    sftp_mode |= (mode & S_IWGRP) ? LIBSSH2_SFTP_S_IWGRP : 0;
    sftp_mode |= (mode & S_IXGRP) ? LIBSSH2_SFTP_S_IXGRP : 0;

    sftp_mode |= (mode & S_IROTH) ? LIBSSH2_SFTP_S_IROTH : 0;
    sftp_mode |= (mode & S_IWOTH) ? LIBSSH2_SFTP_S_IWOTH : 0;
    sftp_mode |= (mode & S_IXOTH) ? LIBSSH2_SFTP_S_IXOTH : 0;

    // file type bits.
    if (S_ISREG(mode)) {
        sftp_mode |= LIBSSH2_SFTP_S_IFREG;
    } else if (S_ISDIR(mode)) {
        sftp_mode |= LIBSSH2_SFTP_S_IFDIR;
    } else if (S_ISCHR(mode)) {
        sftp_mode |= LIBSSH2_SFTP_S_IFCHR;
    } else if (S_ISBLK(mode)) {
        sftp_mode |= LIBSSH2_SFTP_S_IFBLK;
    } else if (S_ISFIFO(mode)) {
        sftp_mode |= LIBSSH2_SFTP_S_IFIFO;
    } else if (S_ISLNK(mode)) {
        sftp_mode |= LIBSSH2_SFTP_S_IFLNK;
    } else if (S_ISSOCK(mode)) {
        sftp_mode |= LIBSSH2_SFTP_S_IFSOCK;
    }

    return sftp_mode;
}

void fill_stat(struct stat* st, const LIBSSH2_SFTP_ATTRIBUTES* attrs) {
    if (attrs->flags & LIBSSH2_SFTP_ATTR_PERMISSIONS) {
        st->st_mode = attrs->permissions;
    }

    if (attrs->flags & LIBSSH2_SFTP_ATTR_SIZE) {
        st->st_size = attrs->filesize;
    }

    if (attrs->flags & LIBSSH2_SFTP_ATTR_UIDGID) {
        st->st_uid = attrs->uid;
        st->st_gid = attrs->gid;
    }

    if (attrs->flags & LIBSSH2_SFTP_ATTR_ACMODTIME) {
        st->st_atime = attrs->atime;
        st->st_mtime = attrs->mtime;
        st->st_ctime = attrs->mtime; // no ctime available, use mtime.
    }

    st->st_nlink = 1;
}

Device::~Device() {
    if (m_sftp_session) {
        libssh2_sftp_shutdown(m_sftp_session);
    }

    if (m_session) {
        libssh2_session_disconnect(m_session, "Normal Shutdown");
        libssh2_session_free(m_session);
    }

    if (m_socket > 0) {
        shutdown(m_socket, SHUT_RDWR);
        close(m_socket);
    }

    if (m_is_ssh2_init) {
        libssh2_exit();
    }
}

bool Device::Mount() {
    if (mounted) {
        return true;
    }

    log_write("[SFTP] Mounting %s version: %s\n", this->config.url.c_str(), LIBSSH2_VERSION);

    if (!m_socket) {
        // connect the socket.
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        addrinfo* res{};
        const auto port = this->config.port > 0 ? this->config.port : 22;
        const auto port_str = std::to_string(port);
        auto ret = getaddrinfo(this->config.url.c_str(), port_str.c_str(), &hints, &res);
        if (ret != 0) {
            log_write("[SFTP] getaddrinfo() failed: %s\n", gai_strerror(ret));
            return false;
        }
        ON_SCOPE_EXIT(freeaddrinfo(res));

        for (auto addr = res; addr != nullptr; addr = addr->ai_next) {
            m_socket = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
            if (m_socket < 0) {
                log_write("[SFTP] socket() failed: %s\n", std::strerror(errno));
                continue;
            }

            ret = connect(m_socket, addr->ai_addr, addr->ai_addrlen);
            if (ret < 0) {
                log_write("[SFTP] connect() failed: %s\n", std::strerror(errno));
                close(m_socket);
                m_socket = -1;
                continue;
            }

            break;
        }

        if (m_socket < 0) {
            log_write("[SFTP] Failed to connect to %s:%ld\n", this->config.url.c_str(), port);
            return false;
        }

        log_write("[SFTP] Connected to %s:%ld\n", this->config.url.c_str(), port);
    }

    if (!m_is_ssh2_init) {
        auto ret = libssh2_init(0);
        if (ret != 0) {
            log_write("[SFTP] libssh2_init() failed: %d\n", ret);
            return false;
        }

        m_is_ssh2_init = true;
    }

    if (!m_session) {
        m_session = libssh2_session_init();
        if (!m_session) {
            log_write("[SFTP] libssh2_session_init() failed\n");
            return false;
        }

        libssh2_session_set_blocking(m_session, 1);
        libssh2_session_flag(m_session, LIBSSH2_FLAG_COMPRESS, 1);

        if (this->config.timeout > 0) {
            libssh2_session_set_timeout(m_session, this->config.timeout);
            // dkp libssh2 is too old for this.
            #if LIBSSH2_VERSION_NUM >= 0x010B00
            libssh2_session_set_read_timeout(m_session, this->config.timeout);
            #endif
        }
    }

    if (this->config.user.empty() || this->config.pass.empty()) {
        log_write("[SFTP] Missing username or password\n");
        return false;
    }

    if (!m_is_handshake_done) {
        const auto ret = libssh2_session_handshake(m_session, m_socket);
        if (ret) {
            log_write("[SFTP] libssh2_session_handshake() failed: %d\n", ret);
            return false;
        }

        m_is_handshake_done = true;
    }

    if (!m_is_auth_done) {
        const auto userauthlist = libssh2_userauth_list(m_session, this->config.user.c_str(), this->config.user.length());
        if (!userauthlist) {
            log_write("[SFTP] libssh2_userauth_list() failed\n");
            return false;
        }

        // just handle user/pass auth for now, pub/priv key is a bit overkill.
        if (std::strstr(userauthlist, "password")) {
            auto ret = libssh2_userauth_password(m_session, this->config.user.c_str(), this->config.pass.c_str());
            if (ret) {
                log_write("[SFTP] Password auth failed: %d\n", ret);
                return false;
            }
        } else {
            log_write("[SFTP] No supported auth methods found\n");
            return false;
        }

        m_is_auth_done = true;
    }

    if (!m_sftp_session) {
        m_sftp_session = libssh2_sftp_init(m_session);
        if (!m_sftp_session) {
            log_write("[SFTP] libssh2_sftp_init() failed\n");
            return false;
        }
    }

    log_write("[SFTP] Mounted %s\n", this->config.url.c_str());
    return mounted = true;
}

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);

    file->fd = libssh2_sftp_open(m_sftp_session, path, convert_flags_to_sftp(flags), convert_mode_to_sftp(mode));
    if (!file->fd) {
        log_write("[SFTP] libssh2_sftp_open() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        return -EIO;
    }

    return 0;
}

int Device::devoptab_close(void *fd) {
    auto file = static_cast<File*>(fd);

    libssh2_sftp_close(file->fd);
    return 0;
}

ssize_t Device::devoptab_read(void *fd, char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);

    // enable if sftp read is slow again.
    #if 0
    char name[256]{};
    std::snprintf(name, sizeof(name), "SFTP read %zu bytes", len);
    SCOPED_TIMESTAMP(name);
    #endif

    const auto ret = libssh2_sftp_read(file->fd, ptr, len);
    if (ret < 0) {
        log_write("[SFTP] libssh2_sftp_read() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        return -EIO;
    }

    return ret;
}

ssize_t Device::devoptab_write(void *fd, const char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);

    const auto ret = libssh2_sftp_write(file->fd, ptr, len);
    if (ret < 0) {
        log_write("[SFTP] libssh2_sftp_write() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        return -EIO;
    }

    return ret;
}

ssize_t Device::devoptab_seek(void *fd, off_t pos, int dir) {
    auto file = static_cast<File*>(fd);
    const auto current_pos = libssh2_sftp_tell64(file->fd);

    if (dir == SEEK_CUR) {
        pos += current_pos;
    } else if (dir == SEEK_END) {
        LIBSSH2_SFTP_ATTRIBUTES attrs{};
        auto ret = libssh2_sftp_fstat(file->fd, &attrs);
        if (ret || !(attrs.flags & LIBSSH2_SFTP_ATTR_SIZE)) {
            log_write("[SFTP] libssh2_sftp_fstat() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        } else {
            pos = attrs.filesize;
        }
    }

    // libssh2 already does this internally, but handle just in case this changes.
    if (pos == current_pos) {
        return pos;
    }

    log_write("[SFTP] Seeking to %ld dir: %d old: %llu\n", pos, dir, current_pos);
    libssh2_sftp_seek64(file->fd, pos);
    return libssh2_sftp_tell64(file->fd);
}

int Device::devoptab_fstat(void *fd, struct stat *st) {
    auto file = static_cast<File*>(fd);

    LIBSSH2_SFTP_ATTRIBUTES attrs{};
    const auto ret = libssh2_sftp_fstat(file->fd, &attrs);
    if (ret) {
        log_write("[SFTP] libssh2_sftp_fstat() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        return -EIO;
    }

    fill_stat(st, &attrs);
    return 0;
}

int Device::devoptab_unlink(const char *path) {
    const auto ret = libssh2_sftp_unlink(m_sftp_session, path);
    if (ret) {
        log_write("[SFTP] libssh2_sftp_unlink() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        return -EIO;
    }

    return 0;
}

int Device::devoptab_rename(const char *oldName, const char *newName) {
    const auto ret = libssh2_sftp_rename(m_sftp_session, oldName, newName);
    if (ret) {
        log_write("[SFTP] libssh2_sftp_rename() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        return -EIO;
    }

    return 0;
}

int Device::devoptab_mkdir(const char *path, int mode) {
    const auto ret = libssh2_sftp_mkdir(m_sftp_session, path, mode);
    if (ret) {
        log_write("[SFTP] libssh2_sftp_mkdir() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        return -EIO;
    }

    return 0;
}

int Device::devoptab_rmdir(const char *path) {
    const auto ret = libssh2_sftp_rmdir(m_sftp_session, path);
    if (ret) {
        log_write("[SFTP] libssh2_sftp_rmdir() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        return -EIO;
    }

    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    auto dir = static_cast<Dir*>(fd);

    dir->fd = libssh2_sftp_opendir(m_sftp_session, path);
    if (!dir->fd) {
        log_write("[SFTP] libssh2_sftp_opendir() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        return -EIO;
    }

    return 0;
}

int Device::devoptab_dirreset(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    libssh2_sftp_rewind(dir->fd);
    return 0;
}

int Device::devoptab_dirnext(void* fd, char *filename, struct stat *filestat) {
    auto dir = static_cast<Dir*>(fd);

    LIBSSH2_SFTP_ATTRIBUTES attrs{};
    const auto ret = libssh2_sftp_readdir(dir->fd, filename, NAME_MAX, &attrs);
    if (ret <= 0) {
        return -ENOENT;
    }

    fill_stat(filestat, &attrs);
    return 0;
}

int Device::devoptab_dirclose(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    libssh2_sftp_closedir(dir->fd);
    return 0;
}

int Device::devoptab_lstat(const char *path, struct stat *st) {
    LIBSSH2_SFTP_ATTRIBUTES attrs{};
    const auto ret = libssh2_sftp_stat(m_sftp_session, path, &attrs);
    if (ret) {
        log_write("[SFTP] libssh2_sftp_stat() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        return -EIO;
    }

    fill_stat(st, &attrs);
    return 0;
}

#if 1
int Device::devoptab_ftruncate(void *fd, off_t len) {
    // stubbed.
    return 0;
}
#endif

int Device::devoptab_statvfs(const char *path, struct statvfs *buf) {
    LIBSSH2_SFTP_STATVFS sftp_st{};
    const auto ret = libssh2_sftp_statvfs(m_sftp_session, path, std::strlen(path), &sftp_st);
    if (ret) {
        log_write("[SFTP] libssh2_sftp_statvfs() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        return -EIO;
    }

    buf->f_bsize = sftp_st.f_bsize;
    buf->f_frsize = sftp_st.f_frsize;
    buf->f_blocks = sftp_st.f_blocks;
    buf->f_bfree = sftp_st.f_bfree;
    buf->f_bavail = sftp_st.f_bavail;
    buf->f_files = sftp_st.f_files;
    buf->f_ffree = sftp_st.f_ffree;
    buf->f_favail = sftp_st.f_favail;
    buf->f_fsid = sftp_st.f_fsid;
    buf->f_flag = sftp_st.f_flag;
    buf->f_namemax = sftp_st.f_namemax;

    return 0;
}

int Device::devoptab_fsync(void *fd) {
    auto file = static_cast<File*>(fd);

    const auto ret = libssh2_sftp_fsync(file->fd);
    if (ret) {
        log_write("[SFTP] libssh2_sftp_fsync() failed: %ld\n", libssh2_sftp_last_error(m_sftp_session));
        return -EIO;
    }

    return 0;
}

} // namespace

Result MountSftpAll() {
    return common::MountNetworkDevice([](const common::MountConfig& cfg) {
            return std::make_unique<Device>(cfg);
        },
        sizeof(File), sizeof(Dir),
        "SFTP"
    );
}

} // namespace sphaira::devoptab
