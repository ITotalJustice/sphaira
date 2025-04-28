#include "ftpsrv_helper.hpp"

#include "app.hpp"
#include "fs.hpp"
#include "log.hpp"

#include <mutex>
#include <algorithm>
#include <minIni.h>
#include <ftpsrv.h>
#include <ftpsrv_vfs.h>
#include <nx/vfs_nx.h>
#include <nx/utils.h>

namespace sphaira::ftpsrv {
namespace {

struct InstallSharedData {
    std::mutex mutex;

    std::deque<std::string> queued_files;

    void* user;
    OnInstallStart on_start;
    OnInstallWrite on_write;
    OnInstallClose on_close;

    bool in_progress;
    bool enabled;
};

const char* INI_PATH = "/config/ftpsrv/config.ini";
FtpSrvConfig g_ftpsrv_config = {0};
volatile bool g_should_exit = false;
bool g_is_running{false};
Thread g_thread;
std::mutex g_mutex{};
InstallSharedData g_shared_data{};

void ftp_log_callback(enum FTP_API_LOG_TYPE type, const char* msg) {
    sphaira::App::NotifyFlashLed();
}

void ftp_progress_callback(void) {
    sphaira::App::NotifyFlashLed();
}

const char* SUPPORTED_EXT[] = {
    ".nsp", ".xci", ".nsz", ".xcz",
};

struct VfsUserData {
    char* path;
    int valid;
};

// ive given up with good names.
void on_thing() {
    log_write("[FTP] doing on_thing\n");
    std::scoped_lock lock{g_shared_data.mutex};
    log_write("[FTP] locked on_thing\n");

    if (!g_shared_data.in_progress) {
        if (!g_shared_data.queued_files.empty()) {
            log_write("[FTP] pushing new file data\n");
            if (!g_shared_data.on_start || !g_shared_data.on_start(g_shared_data.user, g_shared_data.queued_files[0].c_str())) {
                g_shared_data.queued_files.clear();
            } else {
                log_write("[FTP] success on new file push\n");
                g_shared_data.in_progress = true;
            }
        }
    }
}

int vfs_install_open(void* user, const char* path, enum FtpVfsOpenMode mode) {
    {
        std::scoped_lock lock{g_shared_data.mutex};
        auto data = static_cast<VfsUserData*>(user);
        data->valid = 0;

        if (mode != FtpVfsOpenMode_WRITE) {
            errno = EACCES;
            return -1;
        }

        if (!g_shared_data.enabled) {
            errno = EACCES;
            return -1;
        }

        const char* ext = strrchr(path, '.');
        if (!ext) {
            errno = EACCES;
            return -1;
        }

        bool found = false;
        for (size_t i = 0; i < std::size(SUPPORTED_EXT); i++) {
            if (!strcasecmp(ext, SUPPORTED_EXT[i])) {
                found = true;
                break;
            }
        }

        if (!found) {
            errno = EINVAL;
            return -1;
        }

        // check if we already have this file queued.
        auto it = std::find(g_shared_data.queued_files.cbegin(), g_shared_data.queued_files.cend(), path);
        if (it != g_shared_data.queued_files.cend()) {
            errno = EEXIST;
            return -1;
        }

        g_shared_data.queued_files.push_back(path);
        data->path = strdup(path);
        data->valid = true;
    }

    on_thing();
    log_write("[FTP] got file: %s\n", path);
    return 0;
}

int vfs_install_read(void* user, void* buf, size_t size) {
    errno = EACCES;
    return -1;
}

int vfs_install_write(void* user, const void* buf, size_t size) {
    std::scoped_lock lock{g_shared_data.mutex};
    if (!g_shared_data.enabled) {
        errno = EACCES;
        return -1;
    }

    auto data = static_cast<VfsUserData*>(user);
    if (!data->valid) {
        errno = EACCES;
        return -1;
    }

    if (!g_shared_data.on_write || !g_shared_data.on_write(g_shared_data.user, buf, size)) {
        errno = EIO;
        return -1;
    }

    return size;
}

int vfs_install_seek(void* user, const void* buf, size_t size, size_t off) {
    errno = ESPIPE;
    return -1;
}

int vfs_install_isfile_open(void* user) {
    std::scoped_lock lock{g_shared_data.mutex};
    auto data = static_cast<VfsUserData*>(user);
    return data->valid;
}

int vfs_install_isfile_ready(void* user) {
    std::scoped_lock lock{g_shared_data.mutex};
    auto data = static_cast<VfsUserData*>(user);
    const auto ready = !g_shared_data.queued_files.empty() && data->path == g_shared_data.queued_files[0];
    return ready;
}

int vfs_install_close(void* user) {
    {
        log_write("[FTP] closing file\n");
        std::scoped_lock lock{g_shared_data.mutex};
        auto data = static_cast<VfsUserData*>(user);
        if (data->valid) {
            log_write("[FTP] closing valid file\n");

            auto it = std::find(g_shared_data.queued_files.cbegin(), g_shared_data.queued_files.cend(), data->path);
            if (it != g_shared_data.queued_files.cend()) {
                if (it == g_shared_data.queued_files.cbegin()) {
                    log_write("[FTP] closing current file\n");
                    if (g_shared_data.on_close) {
                        g_shared_data.on_close(g_shared_data.user);
                    }

                    g_shared_data.in_progress = false;
                } else {
                    log_write("[FTP] closing other file...\n");
                }

                g_shared_data.queued_files.erase(it);
            } else {
                log_write("[FTP] could not find file in queue...\n");
            }

            if (data->path) {
                free(data->path);
            }

            data->valid = 0;
        }

        memset(data, 0, sizeof(*data));
    }

    on_thing();
    return 0;
}

int vfs_install_opendir(void* user, const char* path) {
    return 0;
}

const char* vfs_install_readdir(void* user, void* user_entry) {
    return NULL;
}

int vfs_install_dirlstat(void* user, const void* user_entry, const char* path, struct stat* st) {
    st->st_nlink = 1;
    st->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
    return 0;
}

int vfs_install_isdir_open(void* user) {
    return 1;
}

int vfs_install_closedir(void* user) {
    return 0;
}

int vfs_install_stat(const char* path, struct stat* st) {
    st->st_nlink = 1;
    st->st_mode = S_IFDIR | S_IWUSR | S_IWGRP | S_IWOTH;
    return 0;
}

int vfs_install_mkdir(const char* path) {
    return -1;
}

int vfs_install_unlink(const char* path) {
    return -1;
}

int vfs_install_rmdir(const char* path) {
    return -1;
}

int vfs_install_rename(const char* src, const char* dst) {
    return -1;
}

FtpVfs g_vfs_install = {
    .open = vfs_install_open,
    .read = vfs_install_read,
    .write = vfs_install_write,
    .seek = vfs_install_seek,
    .close = vfs_install_close,
    .isfile_open = vfs_install_isfile_open,
    .isfile_ready = vfs_install_isfile_ready,
    .opendir = vfs_install_opendir,
    .readdir = vfs_install_readdir,
    .dirlstat = vfs_install_dirlstat,
    .closedir = vfs_install_closedir,
    .isdir_open = vfs_install_isdir_open,
    .stat = vfs_install_stat,
    .lstat = vfs_install_stat,
    .mkdir = vfs_install_mkdir,
    .unlink = vfs_install_unlink,
    .rmdir = vfs_install_rmdir,
    .rename = vfs_install_rename,
};

void loop(void* arg) {
    while (!g_should_exit) {
        ftpsrv_init(&g_ftpsrv_config);
        while (!g_should_exit) {
            if (ftpsrv_loop(100) != FTP_API_LOOP_ERROR_OK) {
                svcSleepThread(1e+6);
                break;
            }
        }
        ftpsrv_exit();
    }
}

} // namespace

bool Init() {
    std::scoped_lock lock{g_mutex};
    if (g_is_running) {
        return false;
    }

    if (R_FAILED(fsdev_wrapMountSdmc())) {
        return false;
    }

    g_ftpsrv_config.log_callback = ftp_log_callback;
    g_ftpsrv_config.progress_callback = ftp_progress_callback;
    g_ftpsrv_config.anon = ini_getbool("Login", "anon", 0, INI_PATH);
    int user_len = ini_gets("Login", "user", "", g_ftpsrv_config.user, sizeof(g_ftpsrv_config.user), INI_PATH);
    int pass_len = ini_gets("Login", "pass", "", g_ftpsrv_config.pass, sizeof(g_ftpsrv_config.pass), INI_PATH);
    g_ftpsrv_config.port = ini_getl("Network", "port", 5000, INI_PATH); // 5000 to keep compat with older sphaira
    g_ftpsrv_config.timeout = ini_getl("Network", "timeout", 0, INI_PATH);
    g_ftpsrv_config.use_localtime = ini_getbool("Misc", "use_localtime", 0, INI_PATH);
    bool log_enabled = ini_getbool("Log", "log", 0, INI_PATH);

    // get nx config
    bool mount_devices = ini_getbool("Nx", "mount_devices", 1, INI_PATH);
    bool mount_bis = ini_getbool("Nx", "mount_bis", 0, INI_PATH);
    bool save_writable = ini_getbool("Nx", "save_writable", 0, INI_PATH);
    g_ftpsrv_config.port = ini_getl("Nx", "app_port", g_ftpsrv_config.port, INI_PATH); // compat

    // get Nx-App overrides
    g_ftpsrv_config.anon = ini_getbool("Nx-App", "anon", g_ftpsrv_config.anon, INI_PATH);
    user_len = ini_gets("Nx-App", "user", g_ftpsrv_config.user, g_ftpsrv_config.user, sizeof(g_ftpsrv_config.user), INI_PATH);
    pass_len = ini_gets("Nx-App", "pass", g_ftpsrv_config.pass, g_ftpsrv_config.pass, sizeof(g_ftpsrv_config.pass), INI_PATH);
    g_ftpsrv_config.port = ini_getl("Nx-App", "port", g_ftpsrv_config.port, INI_PATH);
    g_ftpsrv_config.timeout = ini_getl("Nx-App", "timeout", g_ftpsrv_config.timeout, INI_PATH);
    g_ftpsrv_config.use_localtime = ini_getbool("Nx-App", "use_localtime", g_ftpsrv_config.use_localtime, INI_PATH);
    log_enabled = ini_getbool("Nx-App", "log", log_enabled, INI_PATH);
    mount_devices = ini_getbool("Nx-App", "mount_devices", mount_devices, INI_PATH);
    mount_bis = ini_getbool("Nx-App", "mount_bis", mount_bis, INI_PATH);
    save_writable = ini_getbool("Nx-App", "save_writable", save_writable, INI_PATH);

    mount_devices = true;
    g_ftpsrv_config.timeout = 0;

    if (!g_ftpsrv_config.port) {
        return false;
    }

    // keep compat with older sphaira
    if (!user_len && !pass_len) {
        g_ftpsrv_config.anon = true;
    }

    const VfsNxCustomPath custom = {
        .name = "install",
        .user = NULL,
        .func = &g_vfs_install,
    };

    vfs_nx_init(&custom, mount_devices, save_writable, mount_bis);

    Result rc;
    if (R_FAILED(rc = threadCreate(&g_thread, loop, nullptr, nullptr, 1024*16, 0x2C, 2))) {
        log_write("failed to create nxlink thread: 0x%X\n", rc);
        return false;
    }

    if (R_FAILED(rc = threadStart(&g_thread))) {
        log_write("failed to start nxlink thread: 0x%X\n", rc);
        threadClose(&g_thread);
        return false;
    }

    return g_is_running = true;
}

void Exit() {
    std::scoped_lock lock{g_mutex};
    if (g_is_running) {
        g_is_running = false;
    }
    g_should_exit = true;
    threadWaitForExit(&g_thread);
    threadClose(&g_thread);

    vfs_nx_exit();
    fsdev_wrapUnmountAll();
}

void InitInstallMode(void* user, OnInstallStart on_start, OnInstallWrite on_write, OnInstallClose on_close) {
    std::scoped_lock lock{g_shared_data.mutex};
    g_shared_data.user = user;
    g_shared_data.on_start = on_start;
    g_shared_data.on_write = on_write;
    g_shared_data.on_close = on_close;
    g_shared_data.enabled = true;
}

void DisableInstallMode() {
    std::scoped_lock lock{g_shared_data.mutex};
    g_shared_data.enabled = false;
}

unsigned GetPort() {
    std::scoped_lock lock{g_mutex};
    return g_ftpsrv_config.port;
}

bool IsAnon() {
    std::scoped_lock lock{g_mutex};
    return g_ftpsrv_config.anon;
}

const char* GetUser() {
    std::scoped_lock lock{g_mutex};
    return g_ftpsrv_config.user;
}

const char* GetPass() {
    std::scoped_lock lock{g_mutex};
    return g_ftpsrv_config.pass;
}

} // namespace sphaira::ftpsrv

extern "C" {

void log_file_write(const char* msg) {
    log_write("%s", msg);
}

void log_file_fwrite(const char* fmt, ...) {
    va_list v{};
    va_start(v, fmt);
    log_write_arg(fmt, &v);
    va_end(v);
}

} // extern "C"
