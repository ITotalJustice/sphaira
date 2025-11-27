#include "utils/devoptab_common.hpp"
#include "utils/profile.hpp"

#include "location.hpp"
#include "log.hpp"
#include "defines.hpp"
#include <sys/iosupport.h>
#include <fcntl.h>
#include <curl/curl.h>
#include <minIni.h>

#include <string>
#include <vector>
#include <memory>
#include <cstring>
#include <optional>
#include <sys/stat.h>

namespace sphaira::devoptab {
namespace {

struct DirEntry {
    // deprecated because the names can be truncated and really set to anything.
    std::string name_deprecated{};
    // url decoded href.
    std::string href{};
    bool is_dir{};
};
using DirEntries = std::vector<DirEntry>;

struct FileEntry {
    std::string path{};
    struct stat st{};
};

struct File {
    FileEntry* entry;
    common::PushPullThreadData* push_pull_thread_data;
    size_t off;
    size_t last_off;
};

struct Dir {
    DirEntries* entries;
    size_t index;
};

struct Device final : common::MountCurlDevice {
    using MountCurlDevice::MountCurlDevice;

private:
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

    int http_dirlist(const std::string& path, DirEntries& out);
    int http_stat(const std::string& path, struct stat* st, bool is_dir);

private:
    bool mounted{};
};

int Device::http_dirlist(const std::string& path, DirEntries& out) {
    const auto url = build_url(path, true);
    std::vector<char> chunk;

    log_write("[HTTP] Listing URL: %s path: %s\n", url.c_str(), path.c_str());

    curl_set_common_options(this->curl, url);
    curl_easy_setopt(this->curl, CURLOPT_WRITEFUNCTION, write_memory_callback);
    curl_easy_setopt(this->curl, CURLOPT_WRITEDATA, (void *)&chunk);

    const auto res = curl_easy_perform(this->curl);
    if (res != CURLE_OK) {
        log_write("[HTTP] curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        return -EIO;
    }

    long response_code = 0;
    curl_easy_getinfo(this->curl, CURLINFO_RESPONSE_CODE, &response_code);

    switch (response_code) {
        case 200: // OK
        case 206: // Partial Content
            break;
        case 301: // Moved Permanently
        case 302: // Found
        case 303: // See Other
        case 307: // Temporary Redirect
        case 308: // Permanent Redirect
            return -EIO;
        case 401: // Unauthorized
        case 403: // Forbidden
            return -EACCES;
        case 404: // Not Found
            return -ENOENT;
        default:
            return -EIO;
    }

    log_write("[HTTP] Received %zu bytes for directory listing\n", chunk.size());

    SCOPED_TIMESTAMP("http_dirlist parse");

    // very fast/basic html parsing.
    // takes 17ms to parse 3MB html with 7641 entries.
    // todo: if i ever add an xml parser to sphaira, use that instead.
    // todo: for the above, benchmark the parser to ensure its faster than the my code.
    std::string_view chunk_view{chunk.data(), chunk.size()};

    const auto body_start = chunk_view.find("<body");
    const auto body_end = chunk_view.rfind("</body>");
    const auto table_start = chunk_view.find("<table");
    const auto table_end = chunk_view.rfind("</table>");

    std::string_view table_view{};

    // try and find the body, if this doesn't exist, fallback it's not a valid html page.
    if (body_start != std::string_view::npos && body_end != std::string_view::npos && body_end > body_start) {
        table_view = chunk_view.substr(body_start, body_end - body_start);
    }

    // try and find the table, massively speeds up parsing if it exists.
    // todo: this may cause issues with some web servers that don't use a table for listings.
    // todo: if table fails to fine anything, fallback to body_view.
    if (table_start != std::string_view::npos && table_end != std::string_view::npos && table_end > table_start) {
        table_view = chunk_view.substr(table_start, table_end - table_start);
    }

    if (!table_view.empty()) {
        const std::string_view href_tag_start = "<a href=\"";
        const std::string_view href_tag_end = "\">";
        const std::string_view anchor_tag_end = "</a>";

        size_t pos = 0;
        out.reserve(10000);

        for (;;) {
            const auto href_pos = table_view.find(href_tag_start, pos);
            if (href_pos == std::string_view::npos) {
                break; // no more href.
            }
            pos = href_pos + href_tag_start.length();

            const auto href_begin = pos;
            const auto href_end = table_view.find(href_tag_end, href_begin);
            if (href_end == std::string_view::npos) {
                break; // no more href.
            }

            const auto href_name_end = table_view.find('"', href_begin);
            if (href_name_end == std::string_view::npos || href_name_end < href_begin || href_name_end > href_end) {
                break; // invalid href.
            }

            const auto name_begin = href_end + href_tag_end.length();
            const auto name_end = table_view.find(anchor_tag_end, name_begin);
            if (name_end == std::string_view::npos) {
                break; // no more names.
            }

            pos = name_end + anchor_tag_end.length();
            auto href = url_decode(std::string{table_view.substr(href_begin, href_name_end - href_begin)});
            auto name = url_decode(std::string{table_view.substr(name_begin, name_end - name_begin)});

            // skip empty names/links, root dir entry and links that are not actual files/dirs (e.g. sorting/filter controls).
            if (name.empty() || href.empty() || name == "/" || href.starts_with('?') || href.starts_with('#')) {
                continue;
            }

            // skip parent directory entry and external links.
            if (href == ".." || name == ".." || href.starts_with("../") || name.starts_with("../") || href.find("://") != std::string::npos) {
                continue;
            }

            const auto is_dir = href.ends_with('/');
            if (is_dir) {
                href.pop_back(); // remove the trailing '/'
            }

            out.emplace_back(name, href, is_dir);
        }
    }

    log_write("[HTTP] Parsed %zu entries from directory listing\n", out.size());

    return 0;
}

int Device::http_stat(const std::string& path, struct stat* st, bool is_dir) {
    std::memset(st, 0, sizeof(*st));
    const auto url = build_url(path, is_dir);

    curl_set_common_options(this->curl, url);
    curl_easy_setopt(this->curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(this->curl, CURLOPT_FILETIME, 1L);

    const auto res = curl_easy_perform(this->curl);
    if (res != CURLE_OK) {
        log_write("[HTTP] curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        return -EIO;
    }

    long response_code = 0;
    curl_easy_getinfo(this->curl, CURLINFO_RESPONSE_CODE, &response_code);

    curl_off_t file_size = 0;
    curl_easy_getinfo(this->curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &file_size);

    curl_off_t file_time = 0;
    curl_easy_getinfo(this->curl, CURLINFO_FILETIME_T, &file_time);

    const char* content_type{};
    curl_easy_getinfo(this->curl, CURLINFO_CONTENT_TYPE, &content_type);

    const char* effective_url{};
    curl_easy_getinfo(this->curl, CURLINFO_EFFECTIVE_URL, &effective_url);

    switch (response_code) {
        case 200: // OK
        case 206: // Partial Content
            break;
        case 301: // Moved Permanently
        case 302: // Found
        case 303: // See Other
        case 307: // Temporary Redirect
        case 308: // Permanent Redirect
            return -EIO;
        case 401: // Unauthorized
        case 403: // Forbidden
            return -EACCES;
        case 404: // Not Found
            return -ENOENT;
        default:
            return -EIO;
    }

    if (effective_url) {
        if (std::string_view{effective_url}.ends_with('/')) {
            is_dir = true;
        }
    }

    if (content_type && !std::strcmp(content_type, "text/html")) {
        is_dir = true;
    }

    if (is_dir) {
        st->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
    } else {
        st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        st->st_size = file_size > 0 ? file_size : 0;
    }

    st->st_mtime = file_time > 0 ? file_time : 0;
    st->st_atime = st->st_mtime;
    st->st_ctime = st->st_mtime;
    st->st_nlink = 1;

    return 0;
}

bool Device::Mount() {
    if (mounted) {
        return true;
    }

    if (!MountCurlDevice::Mount()) {
        return false;
    }

    // todo: query server with OPTIONS to see if it supports range requests.
    // todo: see ftp for example.

    return mounted = true;
}

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);

    struct stat st;
    const auto ret = http_stat(path, &st, false);
    if (ret < 0) {
        log_write("[HTTP] http_stat() failed for file: %s errno: %s\n", path, std::strerror(-ret));
        return ret;
    }

    if (st.st_mode & S_IFDIR) {
        log_write("[HTTP] Attempted to open a directory as a file: %s\n", path);
        return -EISDIR;
    }

    file->entry = new FileEntry{path, st};
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

    if (!len) {
        return 0;
    }

    if (file->off != file->last_off) {
        log_write("[HTTP] File offset changed from %zu to %zu, resetting download thread\n", file->last_off, file->off);
        file->last_off = file->off;
        delete file->push_pull_thread_data;
        file->push_pull_thread_data = nullptr;
    }

    if (!file->push_pull_thread_data) {
        log_write("[HTTP] Creating download thread data for file: %s\n", file->entry->path.c_str());
        file->push_pull_thread_data = CreatePushData(this->transfer_curl, build_url(file->entry->path, false), file->off);
        if (!file->push_pull_thread_data) {
            log_write("[HTTP] Failed to create download thread data for file: %s\n", file->entry->path.c_str());
            return -EIO;
        }
    }

    const auto ret = file->push_pull_thread_data->PullData(ptr, len);

    file->off += ret;
    file->last_off = file->off;
    return ret;
}

ssize_t Device::devoptab_seek(void *fd, off_t pos, int dir) {
    auto file = static_cast<File*>(fd);

    if (dir == SEEK_CUR) {
        pos += file->off;
    } else if (dir == SEEK_END) {
        pos = file->entry->st.st_size;
    }

    return file->off = std::clamp<u64>(pos, 0, file->entry->st.st_size);
}

int Device::devoptab_fstat(void *fd, struct stat *st) {
    auto file = static_cast<File*>(fd);

    std::memcpy(st, &file->entry->st, sizeof(*st));
    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    auto dir = static_cast<Dir*>(fd);

    log_write("[HTTP] Opening directory: %s\n", path);
    auto entries = new DirEntries();
    const auto ret = http_dirlist(path, *entries);
    if (ret < 0) {
        log_write("[HTTP] http_dirlist() failed for directory: %s errno: %s\n", path, std::strerror(-ret));
        delete entries;
        return ret;
    }

    log_write("[HTTP] Opened directory: %s with %zu entries\n", path, entries->size());
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

    // <a href="Compass_2.0.7.1-Release_ScVi3.0.1-Standalone-21-2-0-7-1-1729820977.zip">Compass_2.0.7.1-Release_ScVi3.0.1-Standalone-21..&gt;</a>
    filestat->st_nlink = 1;
    // std::strcpy(filename, entry.name.c_str());
    std::strcpy(filename, entry.href.c_str());

    dir->index++;
    return 0;
}

int Device::devoptab_dirclose(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    delete dir->entries;
    return 0;
}

int Device::devoptab_lstat(const char *path, struct stat *st) {
    auto ret = http_stat(path, st, false);
    if (ret < 0) {
        ret = http_stat(path, st, true);
    }

    if (ret < 0) {
        log_write("[HTTP] http_stat() failed for path: %s errno: %s\n", path, std::strerror(-ret));
        return ret;
    }

    return 0;
}

} // namespace

Result MountHttpAll() {
    return common::MountNetworkDevice([](const common::MountConfig& config) {
            return std::make_unique<Device>(config);
        },
        sizeof(File), sizeof(Dir),
        "HTTP",
        true
    );
}

} // namespace sphaira::devoptab
