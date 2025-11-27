#include "utils/devoptab_common.hpp"
#include "utils/profile.hpp"

#include "fs.hpp"
#include "log.hpp"
#include "defines.hpp"
#include <fcntl.h>
#include <curl/curl.h>

#include <string>
#include <vector>
#include <memory>
#include <cstring>
#include <optional>
#include <ctime>
#include <ranges>
#include <sys/stat.h>

namespace sphaira::devoptab {
namespace {

struct DirEntry {
    std::string name{};
    bool is_dir{};
};
using DirEntries = std::vector<DirEntry>;

struct FileEntry {
    std::string path{};
    struct stat st{};
};

struct Device final : common::MountCurlDevice {
    using MountCurlDevice::MountCurlDevice;

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
    int devoptab_fsync(void *fd) override;
    void curl_set_common_options(CURL* curl,  const std::string& url) override;

    static bool ftp_parse_mlst_line(std::string_view line, struct stat* st, std::string* file_out, bool type_only);
    static void ftp_parse_mlsd(std::string_view chunk, DirEntries& out);
    static bool ftp_parse_mlist(std::string_view chunk, struct stat* st);

    std::pair<bool, long> ftp_quote(std::span<const std::string> commands, bool is_dir, std::vector<char>* response_data = nullptr);
    int ftp_dirlist(const std::string& path, DirEntries& out);
    int ftp_stat(const std::string& path, struct stat* st, bool is_dir);
    int ftp_remove_file_folder(const std::string& path, bool is_dir);
    int ftp_unlink(const std::string& path);
    int ftp_rename(const std::string& old_path, const std::string& new_path, bool is_dir);
    int ftp_mkdir(const std::string& path);
    int ftp_rmdir(const std::string& path);

private:
    bool mounted{};
};

struct File {
    FileEntry* entry;
    common::PushPullThreadData* push_pull_thread_data;
    size_t off;
    size_t last_off;
    bool write_mode;
    bool append_mode;
};

struct Dir {
    DirEntries* entries;
    size_t index;
};

void Device::curl_set_common_options(CURL* curl, const std::string& url) {
    MountCurlDevice::curl_set_common_options(curl, url);
    curl_easy_setopt(curl, CURLOPT_FTP_CREATE_MISSING_DIRS, CURLFTP_CREATE_DIR_NONE);
    curl_easy_setopt(curl, CURLOPT_FTP_FILEMETHOD, CURLFTPMETHOD_NOCWD);
}

bool Device::ftp_parse_mlst_line(std::string_view line, struct stat* st, std::string* file_out, bool type_only) {
    // trim leading white space.
    while (line.size() > 0 && std::isspace(line[0])) {
        line = line.substr(1);
    }

    auto file_name_pos = line.rfind(';');
    if (file_name_pos == std::string_view::npos || file_name_pos + 1 >= line.size()) {
        return false;
    }

    // trim white space.
    while (file_name_pos + 1 < line.size() && std::isspace(line[file_name_pos + 1])) {
        file_name_pos++;
    }
    auto file_name = line.substr(file_name_pos + 1);

    auto facts = line.substr(0, file_name_pos);
    if (file_name.empty()) {
        return false;
    }

    bool found_type = false;
    while (!facts.empty()) {
        const auto sep = facts.find(';');
        if (sep == std::string_view::npos) {
            break;
        }

        const auto fact = facts.substr(0, sep);
        facts = facts.substr(sep + 1);

        const auto eq = fact.find('=');
        if (eq == std::string_view::npos || eq + 1 >= fact.size()) {
            continue;
        }

        const auto key = fact.substr(0, eq);
        const auto val = fact.substr(eq + 1);

        if (fs::FsPath::path_equal(key, "type")) {
            if (fs::FsPath::path_equal(val, "file")) {
                st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
            } else if (fs::FsPath::path_equal(val, "dir")) {
                st->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
            } else {
                log_write("[FTP] Unknown type fact value: %.*s\n", (int)val.size(), val.data());
                return false;
            }

            found_type = true;
        } else if (!type_only) {
            if (fs::FsPath::path_equal(key, "size")) {
                st->st_size = std::stoull(std::string(val));
            } else if (fs::FsPath::path_equal(key, "modify")) {
                if (val.size() >= 14) {
                    struct tm tm{};
                    tm.tm_year = std::stoi(std::string(val.substr(0, 4))) - 1900;
                    tm.tm_mon = std::stoi(std::string(val.substr(4, 2))) - 1;
                    tm.tm_mday = std::stoi(std::string(val.substr(6, 2)));
                    tm.tm_hour = std::stoi(std::string(val.substr(8, 2)));
                    tm.tm_min = std::stoi(std::string(val.substr(10, 2)));
                    tm.tm_sec = std::stoi(std::string(val.substr(12, 2)));
                    st->st_mtime = std::mktime(&tm);
                    st->st_atime = st->st_mtime;
                    st->st_ctime = st->st_mtime;
                }
            }
        }
    }

    if (!found_type) {
        log_write("[FTP] MLST line missing type fact\n");
        return false;
    }

    st->st_nlink = 1;
    if (file_out) {
        *file_out = std::string(file_name.data(), file_name.size());
    }

    return true;
}

/*
C> MLst file1
S> 250- Listing file1
S>  Type=file;Modify=19990929003355.237; file1
S> 250 End
*/
bool Device::ftp_parse_mlist(std::string_view chunk, struct stat* st) {
    // sometimes the header data includes the full login exchange
    // so we need to find the actual start of the MLST response.
    const auto start_pos = chunk.find("250-");
    const auto end_pos = chunk.rfind("\n250");

    if (start_pos == std::string_view::npos || end_pos == std::string_view::npos) {
        log_write("[FTP] MLST response missing start or end\n");
        return false;
    }

    const auto end_line = chunk.find('\n', start_pos + 1);
    if (end_line == std::string_view::npos || end_line > end_pos) {
        log_write("[FTP] MLST response missing end line\n");
        return false;
    }

    chunk = chunk.substr(end_line + 1, end_pos - (end_line + 1));
    return ftp_parse_mlst_line(chunk, st, nullptr, false);
}

/*
C> MLSD tmp
S> 150 BINARY connection open for MLSD tmp
D> Type=cdir;Modify=19981107085215;Perm=el; tmp
D> Type=cdir;Modify=19981107085215;Perm=el; /tmp
D> Type=pdir;Modify=19990112030508;Perm=el; ..
D> Type=file;Size=25730;Modify=19940728095854;Perm=; capmux.tar.z
D> Type=file;Size=1024990;Modify=19980130010322;Perm=r; cap60.pl198.tar.gz
S> 226 MLSD completed
*/
void Device::ftp_parse_mlsd(std::string_view chunk, DirEntries& out) {
    if (chunk.ends_with("\r\n")) {
        chunk = chunk.substr(0, chunk.size() - 2);
    } else if (chunk.ends_with('\n')) {
        chunk = chunk.substr(0, chunk.size() - 1);
    }

    for (const auto line : std::views::split(chunk, '\n')) {
        std::string_view line_str(line.data(), line.size());
        if (line_str.empty() || line_str == "\r") {
            continue;
        }

        DirEntry entry{};
        struct stat st{};
        if (!ftp_parse_mlst_line(line_str, &st, &entry.name, true)) {
            log_write("[FTP] Failed to parse MLSD line: %.*s\n", (int)line.size(), line.data());
            continue;
        }

        entry.is_dir = S_ISDIR(st.st_mode);
        out.emplace_back(entry);
    }
}

std::pair<bool, long> Device::ftp_quote(std::span<const std::string> commands, bool is_dir, std::vector<char>* response_data) {
    const auto url = build_url("/", is_dir);

    curl_slist* cmdlist{};
    ON_SCOPE_EXIT(curl_slist_free_all(cmdlist));

    for (const auto& cmd : commands) {
        cmdlist = curl_slist_append(cmdlist, cmd.c_str());
    }

    curl_set_common_options(this->curl, url);
    curl_easy_setopt(this->curl, CURLOPT_QUOTE, cmdlist);
    curl_easy_setopt(this->curl, CURLOPT_NOBODY, 1L);

    if (response_data) {
        response_data->clear();
        curl_easy_setopt(this->curl, CURLOPT_HEADERFUNCTION, write_memory_callback);
        curl_easy_setopt(this->curl, CURLOPT_HEADERDATA, (void *)response_data);
    }

    const auto res = curl_easy_perform(this->curl);
    if (res != CURLE_OK) {
        log_write("[FTP] curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        return {false, 0};
    }

    long response_code = 0;
    curl_easy_getinfo(this->curl, CURLINFO_RESPONSE_CODE, &response_code);
    return {true, response_code};
}

int Device::ftp_dirlist(const std::string& path, DirEntries& out) {
    const auto url = build_url(path, true);
    std::vector<char> chunk;

    curl_set_common_options(this->curl, url);
    curl_easy_setopt(this->curl, CURLOPT_WRITEFUNCTION, write_memory_callback);
    curl_easy_setopt(this->curl, CURLOPT_WRITEDATA, (void *)&chunk);
    curl_easy_setopt(this->curl, CURLOPT_CUSTOMREQUEST, "MLSD");

    const auto res = curl_easy_perform(this->curl);
    if (res != CURLE_OK) {
        log_write("[FTP] curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        return -EIO;
    }

    long response_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

    switch (response_code) {
        case 125: // Data connection already open; transfer starting.
        case 150: // File status okay; about to open data connection.
        case 226: // Closing data connection. Requested file action successful.
            break;
        case 450: // Requested file action not taken. File unavailable (e.g., file busy).
        case 550: // Requested action not taken. File unavailable (e.g., file not found, no access).
            return -ENOENT;
        default:
            return -EIO;
    }

    ftp_parse_mlsd({chunk.data(), chunk.size()}, out);
    return 0;
}

int Device::ftp_stat(const std::string& path, struct stat* st, bool is_dir) {
    std::memset(st, 0, sizeof(*st));

    std::vector<char> chunk;
    const auto [success, response_code] = ftp_quote({"MLST " + path}, is_dir, &chunk);
    if (!success) {
        return -EIO;
    }

    switch (response_code) {
        case 250: // Requested file action okay, completed.
            break;
        case 450: // Requested file action not taken. File unavailable (e.g., file busy).
        case 550: // Requested action not taken. File unavailable (e.g., file not found, no access).
            return -ENOENT;
        default:
            return -EIO;
    }

    if (!ftp_parse_mlist({chunk.data(), chunk.size()}, st)) {
        log_write("[FTP] Failed to parse MLST response for path: %s\n", path.c_str());
        return -EIO;
    }

    return 0;
}

int Device::ftp_remove_file_folder(const std::string& path, bool is_dir) {
    const auto cmd = (is_dir ? "RMD " : "DELE ") + path;
    const auto [success, response_code] = ftp_quote({cmd}, is_dir);

    if (!success) {
        return -EIO;
    }

    switch (response_code) {
        case 250: // Requested file action okay, completed.
        case 200: // Command okay.
            break;
        case 450: // Requested file action not taken. File unavailable (e.g., file busy).
        case 550: // Requested action not taken. File unavailable (e.g., file not found, no access).
            return -ENOENT;
        default:
            return -EIO;
    }

    return 0;
}

int Device::ftp_unlink(const std::string& path) {
    return ftp_remove_file_folder(path, false);
}

int Device::ftp_rename(const std::string& old_path, const std::string& new_path, bool is_dir) {
    const auto url = build_url("/", is_dir);

    std::vector<std::string> commands;
    commands.emplace_back("RNFR " + old_path);
    commands.emplace_back("RNTO " + new_path);

    const auto [success, response_code] = ftp_quote(commands, is_dir);
    if (!success) {
        return -EIO;
    }

    switch (response_code) {
        case 250: // Requested file action okay, completed.
        case 200: // Command okay.
            break;
        case 450: // Requested file action not taken. File unavailable (e.g., file busy).
        case 550: // Requested action not taken. File unavailable (e.g., file not found, no access).
            return -ENOENT;
        case 553: // Requested action not taken. File name not allowed.
            return -EEXIST;
        default:
            return -EIO;
    }

    return 0;
}

int Device::ftp_mkdir(const std::string& path) {
    std::vector<char> chunk;
    const auto [success, response_code] = ftp_quote({"MKD " + path}, true);
    if (!success) {
        return -EIO;
    }

    switch (response_code) {
        case 257: // "PATHNAME" created.
        case 250: // Requested file action okay, completed.
        case 200: // Command okay.
            break;
        case 550: // Requested action not taken. File unavailable (e.g., file not found, no access).
            return -ENOENT; // Parent directory does not exist or no permission.
        case 521: // Directory already exists.
            return -EEXIST;
        default:
            return -EIO;
    }

    return 0;
}

int Device::ftp_rmdir(const std::string& path) {
    return ftp_remove_file_folder(path, true);
}

bool Device::Mount() {
    if (mounted) {
        return true;
    }

    if (!MountCurlDevice::Mount()) {
        return false;
    }

    // issue FEAT command to see if we support MLST/MLSD.
    std::vector<char> chunk;
    const auto [success, response_code] = ftp_quote({"FEAT"}, true, &chunk);
    if (!success || response_code != 211) {
        log_write("[FTP] FEAT command failed with response code: %ld\n", response_code);
        return false;
    }

    std::string_view view(chunk.data(), chunk.size());

    // check for MLST/MLSD support.
    // NOTE: RFC 3659 states that servers must support MLSD if they support MLST.
    if (view.find("MLST") == std::string_view::npos) {
        log_write("[FTP] Server does not support MLST/MLSD commands\n");
        return false;
    }

    // if we support UTF8, enable it.
    if (view.find("UTF8") != std::string_view::npos) {
        // it doesn't matter if this fails tbh.
        // also, i am not sure if this persists between logins or not...
        ftp_quote({"OPTS UTF8 ON"}, true);
    }

    return this->mounted = true;
}

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);
    struct stat st{};

    if ((flags & O_ACCMODE) == O_RDONLY || (flags & O_APPEND)) {
        // ensure the file exists and get its size.
        const auto ret = ftp_stat(path, &st, false);
        if (ret < 0) {
            return ret;
        }

        if (st.st_mode & S_IFDIR) {
            log_write("[FTP] Path is a directory, not a file: %s\n", path);
            return -EISDIR;
        }
    }

    file->entry = new FileEntry{path, st};
    file->write_mode = (flags & (O_WRONLY | O_RDWR));
    file->append_mode = (flags & O_APPEND);

    if (file->append_mode) {
        file->off = st.st_size;
        file->last_off = file->off;
    }

    return 0;
}

int Device::devoptab_close(void *fd) {
    auto file = static_cast<File*>(fd);

    delete file->push_pull_thread_data;
    delete file->entry;
    return 0;
}

ssize_t Device::devoptab_read(void *fd, char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);
    len = std::min(len, file->entry->st.st_size - file->off);

    if (file->write_mode) {
        log_write("[FTP] Attempt to read from a write-only file\n");
        return -EBADF;
    }

    if (!len) {
        return 0;
    }

    if (file->off != file->last_off) {
        log_write("[FTP] File offset changed from %zu to %zu, resetting download thread\n", file->last_off, file->off);
        file->last_off = file->off;
        delete file->push_pull_thread_data;
        file->push_pull_thread_data = nullptr;
    }

    if (!file->push_pull_thread_data) {
        log_write("[FTP] Creating download thread data for file: %s\n", file->entry->path.c_str());
        file->push_pull_thread_data = CreatePushData(this->transfer_curl, build_url(file->entry->path, false), file->off);
        if (!file->push_pull_thread_data) {
            log_write("[FTP] Failed to create download thread data for file: %s\n", file->entry->path.c_str());
            return -EIO;
        }
    }

    const auto ret = file->push_pull_thread_data->PullData(ptr, len);

    file->off += ret;
    file->last_off = file->off;
    return ret;
}

ssize_t Device::devoptab_write(void *fd, const char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);

    if (!file->write_mode) {
        log_write("[FTP] Attempt to write to a read-only file\n");
        return -EBADF;
    }

    if (!len) {
        return 0;
    }

    if (!file->push_pull_thread_data) {
        log_write("[FTP] Creating upload thread data for file: %s\n", file->entry->path.c_str());
        file->push_pull_thread_data = CreatePullData(this->transfer_curl, build_url(file->entry->path, false), file->append_mode);
        if (!file->push_pull_thread_data) {
            log_write("[FTP] Failed to create upload thread data for file: %s\n", file->entry->path.c_str());
            return -EIO;
        }
    }

    const auto ret = file->push_pull_thread_data->PushData(ptr, len);

    file->off += ret;
    file->entry->st.st_size = std::max<off_t>(file->entry->st.st_size, file->off);
    return ret;
}

ssize_t Device::devoptab_seek(void *fd, off_t pos, int dir) {
    auto file = static_cast<File*>(fd);

    if (dir == SEEK_CUR) {
        pos += file->off;
    } else if (dir == SEEK_END) {
        pos = file->entry->st.st_size;
    }

    // for now, random access writes are disabled.
    if (file->write_mode && pos != file->off) {
        log_write("[FTP] Random access writes are not supported\n");
        return file->off;
    }

    return file->off = std::clamp<u64>(pos, 0, file->entry->st.st_size);
}

int Device::devoptab_fstat(void *fd, struct stat *st) {
    auto file = static_cast<File*>(fd);

    std::memcpy(st, &file->entry->st, sizeof(*st));
    return 0;
}

int Device::devoptab_unlink(const char *path) {
    const auto ret = ftp_unlink(path);
    if (ret < 0) {
        log_write("[FTP] ftp_unlink() failed: %s errno: %s\n", path, std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_rename(const char *oldName, const char *newName) {
    auto ret = ftp_rename(oldName, newName, false);
    if (ret == -ENOENT) {
        ret = ftp_rename(oldName, newName, true);
    }

    if (ret < 0) {
        log_write("[FTP] ftp_rename() failed: %s -> %s errno: %s\n", oldName, newName, std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_mkdir(const char *path, int mode) {
    const auto ret = ftp_mkdir(path);
    if (ret < 0) {
        log_write("[FTP] ftp_mkdir() failed: %s errno: %s\n", path, std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_rmdir(const char *path) {
    const auto ret = ftp_rmdir(path);
    if (ret < 0) {
        log_write("[FTP] ftp_rmdir() failed: %s errno: %s\n", path, std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    auto dir = static_cast<Dir*>(fd);

    auto entries = new DirEntries();
    const auto ret = ftp_dirlist(path, *entries);
    if (ret < 0) {
        log_write("[FTP] ftp_dirlist() failed: %s errno: %s\n", path, std::strerror(-ret));
        delete entries;
        return ret;
    }

    dir->entries = entries;
    return 0;
}

int Device::devoptab_dirreset(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    dir->index = 0;
    return 0;
}

int Device::devoptab_dirnext(void* fd, char *filename, struct stat *filestat) {
    auto dir = static_cast<Dir*>(fd);

    if (dir->index >= dir->entries->size()) {
        return -ENOENT;
    }

    auto& entry = (*dir->entries)[dir->index];
    if (entry.is_dir) {
        filestat->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
    } else {
        filestat->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    }

    filestat->st_nlink = 1;
    std::strcpy(filename, entry.name.c_str());

    dir->index++;
    return 0;
}

int Device::devoptab_dirclose(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    delete dir->entries;
    return 0;
}

int Device::devoptab_lstat(const char *path, struct stat *st) {
    auto ret = ftp_stat(path, st, false);
    if (ret == -ENOENT) {
        ret = ftp_stat(path, st, true);
    }

    if (ret < 0) {
        log_write("[FTP] ftp_stat() failed: %s errno: %s\n", path, std::strerror(-ret));
        return ret;
    }

    return 0;
}

int Device::devoptab_ftruncate(void *fd, off_t len) {
    auto file = static_cast<File*>(fd);

    if (!file->write_mode) {
        log_write("[FTP] Attempt to truncate a read-only file\n");
        return -EBADF;
    }

    file->entry->st.st_size = len;
    return 0;
}

int Device::devoptab_fsync(void *fd) {
    auto file = static_cast<File*>(fd);

    if (!file->write_mode) {
        log_write("[FTP] Attempt to fsync a read-only file\n");
        return -EBADF;
    }

    return 0;
}

} // namespace

Result MountFtpAll() {
    return common::MountNetworkDevice([](const common::MountConfig& config) {
            return std::make_unique<Device>(config);
        },
        sizeof(File), sizeof(Dir),
        "FTP"
    );
}

} // namespace sphaira::devoptab
