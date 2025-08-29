#include "fs.hpp"
#include "defines.hpp"
#include "ui/nvg_util.hpp"
#include "log.hpp"

#include <switch.h>
#include <cstdio>
#include <cstring>
#include <vector>
#include <string_view>
#include <algorithm>
#include <ranges>
#include <string>
#include <functional>

#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <ftw.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <cerrno>

namespace fs {
namespace {

// these folders and internals cannot be modified
constexpr std::string_view READONLY_ROOT_FOLDERS[]{
    "/atmosphere/automatic_backups",

    "/bootloader/res",
    "/bootloader/sys",

    "/backup", // some people never back this up...

    "/Nintendo", // Nintendo private folder
    "/Nintendo/Contents",
    "/Nintendo/save",

    "/emuMMC", // emunand
    "/warmboot_mariko",
};

// these files and folders cannot be modified
constexpr std::string_view READONLY_FILES[]{
    "/", // don't allow deleting root

    "/atmosphere", // don't allow deleting all of /atmosphere
    "/atmosphere/hbl.nsp",
    "/atmosphere/package3",
    "/atmosphere/reboot_payload.bin",
    "/atmosphere/stratosphere.romfs",

    "/bootloader", // don't allow deleting all of /bootloader
    "/bootloader/hekate_ipl.ini",

    "/switch", // don't allow deleting all of /switch
    "/hbmenu.nro", // breaks hbl
    "/payload.bin", // some modchips need this

    "/boot.dat", // sxos
    "/license.dat", // sxos

    "/switch/prod.keys",
    "/switch/title.keys",
    "/switch/reboot_to_payload.nro",
};

bool is_read_only_root(std::string_view path) {
    for (auto p : READONLY_ROOT_FOLDERS) {
        if (path.starts_with(p)) {
            return true;
        }
    }

    return false;
}

bool is_read_only_file(std::string_view path) {
    for (auto p : READONLY_FILES) {
        if (path == p) {
            return true;
        }
    }

    return false;
}

bool is_read_only(std::string_view path) {
    if (is_read_only_root(path)) {
        return true;
    }
    if (is_read_only_file(path)) {
        return true;
    }
    return false;
}

// Helper used by stdio recursive delete: recursively remove files and directories.
// Returns 0 on success, -1 on error (errno set).
static int remove_dir_recursive_impl(const char* path) {
    struct stat st;
    if (lstat(path, &st) != 0) {
        return -1;
    }

    // If it's not a directory, unlink it directly.
    if (!S_ISDIR(st.st_mode)) {
        if (unlink(path) != 0) return -1;
        return 0;
    }

    DIR* dir = opendir(path);
    if (!dir) return -1;
    struct dirent* entry;
    int result = 0;

    while ((entry = readdir(dir)) != nullptr && result == 0) {
        const char* name = entry->d_name;
        if (std::strcmp(name, ".") == 0 || std::strcmp(name, "..") == 0) {
            continue;
        }

        std::string child;
        child.reserve(std::strlen(path) + 1 + std::strlen(name) + 1);
        child = path;
        if (child.back() != '/') child.push_back('/');
        child += name;

        // If d_type is unknown, fall back to lstat.
        bool is_dir = false;
        if (entry->d_type == DT_DIR) {
            is_dir = true;
        } else if (entry->d_type == DT_UNKNOWN || entry->d_type == DT_LNK) {
            struct stat cst;
            if (lstat(child.c_str(), &cst) == 0 && S_ISDIR(cst.st_mode)) {
                is_dir = true;
            }
        }

        if (is_dir) {
            result = remove_dir_recursive_impl(child.c_str());
        } else {
            if (unlink(child.c_str()) != 0) result = -1;
        }
    }

    closedir(dir);

    if (result == 0) {
        if (rmdir(path) != 0) result = -1;
    }

    return result;
}

} // namespace

FsPath AppendPath(const FsPath& root_path, const FsPath& _file_path) {
    // strip leading '/' in file path.
    auto file_path = _file_path.s;
    while (file_path[0] == '/') {
        file_path++;
    }

    FsPath path;
    const std::size_t root_len = std::strlen(root_path.s);
    if (root_len == 0) {
        std::snprintf(path, sizeof(path), "%s", file_path);
    } else if (root_path.s[root_len - 1] != '/') {
        std::snprintf(path, sizeof(path), "%s/%s", root_path.s, file_path);
    } else {
        std::snprintf(path, sizeof(path), "%s%s", root_path.s, file_path);
    }
    return path;
}

Result CreateFile(FsFileSystem* fs, const FsPath& path, u64 size, u32 option, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(path.s), Result_FsReadOnly);

    // mark big file option if >= 4 GiB
    if (size >= (4ULL * 1024ULL * 1024ULL * 1024ULL)) {
        option |= FsCreateOption_BigFile;
    }

    R_TRY(fsFsCreateFile(fs, path, size, option));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result CreateDirectory(FsFileSystem* fs, const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(path.s), Result_FsReadOnly);

    R_TRY(fsFsCreateDirectory(fs, path));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result CreateDirectoryRecursively(FsFileSystem* fs, const FsPath& _path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(_path.s), Result_FsReadOnly);

    // try and create the directory / see if it already exists before the loop.
    Result rc;
    if (fs) {
        rc = CreateDirectory(fs, _path, ignore_read_only);
    } else {
        rc = CreateDirectory(_path, ignore_read_only);
    }

    if (R_SUCCEEDED(rc) || rc == FsError_PathAlreadyExists) {
        R_SUCCEED();
    }

    auto path_view = std::string_view{_path.s};
    // todo: fix this for sdmc: and ums0:
    FsPath path{"/"};
    if (auto s = std::strchr(_path.s, ':')) {
        const int len = (s - _path.s) + 1;
        std::snprintf(path, sizeof(path), "%.*s/", len, _path.s);
        path_view = path_view.substr(len);
    }

    for (const auto dir : std::views::split(path_view, '/')) {
        if (dir.empty()) {
            continue;
        }
        // dir is a subrange; append its chars safely
        std::strncat(path, dir.data(), dir.size());
        log_write("[FS] dir creation path is now: %s\n", path.s);

        if (fs) {
            rc = CreateDirectory(fs, path, ignore_read_only);
        } else {
            rc = CreateDirectory(path, ignore_read_only);
        }

        if (R_FAILED(rc) && rc != FsError_PathAlreadyExists) {
            log_write("failed to create folder: %s\n", path.s);
            return rc;
        }

        // log_write("created_directory: %s\n", path);
        std::strcat(path, "/");
    }
    R_SUCCEED();
}

Result CreateDirectoryRecursivelyWithPath(FsFileSystem* fs, const FsPath& _path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(_path.s), Result_FsReadOnly);

    // strip file name form path.
    const auto last_slash = std::strrchr(_path.s, '/');
    if (!last_slash) {
        R_SUCCEED();
    }

    FsPath new_path{};
    std::snprintf(new_path, sizeof(new_path), "%.*s", (int)(last_slash - _path.s), _path.s);
    R_TRY(CreateDirectoryRecursively(fs, new_path, ignore_read_only));
    R_SUCCEED();
}

Result DeleteFile(FsFileSystem* fs, const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path.s), Result_FsReadOnly);
    R_TRY(fsFsDeleteFile(fs, path));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result DeleteDirectory(FsFileSystem* fs, const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path.s), Result_FsReadOnly);

    R_TRY(fsFsDeleteDirectory(fs, path));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result DeleteDirectoryRecursively(FsFileSystem* fs, const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path.s), Result_FsReadOnly);

    R_TRY(fsFsDeleteDirectoryRecursively(fs, path));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result RenameFile(FsFileSystem* fs, const FsPath& src, const FsPath& dst, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(src.s), Result_FsReadOnly);
    R_UNLESS(ignore_read_only || !is_read_only(dst.s), Result_FsReadOnly);

    R_TRY(fsFsRenameFile(fs, src, dst));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result RenameDirectory(FsFileSystem* fs, const FsPath& src, const FsPath& dst, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(src.s), Result_FsReadOnly);
    R_UNLESS(ignore_read_only || !is_read_only(dst.s), Result_FsReadOnly);

    R_TRY(fsFsRenameDirectory(fs, src, dst));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result GetEntryType(FsFileSystem* fs, const FsPath& path, FsDirEntryType* out) {
    return fsFsGetEntryType(fs, path, out);
}

Result GetFileTimeStampRaw(FsFileSystem* fs, const FsPath& path, FsTimeStampRaw *out) {
    return fsFsGetFileTimeStampRaw(fs, path, out);
}

Result SetTimestamp(FsFileSystem* fs, const FsPath& path, const FsTimeStampRaw* ts) {
    // unsuported for native service implementation
    R_SUCCEED();
}

bool FileExists(FsFileSystem* fs, const FsPath& path) {
    FsDirEntryType type;
    R_TRY_RESULT(GetEntryType(fs, path, &type), false);
    return type == FsDirEntryType_File;
}

bool DirExists(FsFileSystem* fs, const FsPath& path) {
    FsDirEntryType type;
    R_TRY_RESULT(GetEntryType(fs, path, &type), false);
    return type == FsDirEntryType_Dir;
}

Result read_entire_file(FsFileSystem* _fs, const FsPath& path, std::vector<u8>& out) {
    FsNative fs{_fs, false};
    R_TRY(fs.GetFsOpenResult());

    File f;
    R_TRY(fs.OpenFile(path, FsOpenMode_Read, &f));

    s64 size;
    R_TRY(f.GetSize(&size));
    if (size < 0) {
        return Result_FsUnknownStdioError;
    }
    out.resize((size_t)size);

    u64 bytes_read;
    R_TRY(f.Read(0, out.data(), out.size(), FsReadOption_None, &bytes_read));
    R_UNLESS(bytes_read == out.size(), 1);

    R_SUCCEED();
}

Result write_entire_file(FsFileSystem* _fs, const FsPath& path, const std::vector<u8>& in, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path.s), Result_FsReadOnly);

    FsNative fs{_fs, false, ignore_read_only};
    R_TRY(fs.GetFsOpenResult());

    if (auto rc = fs.CreateFile(path, in.size(), 0); R_FAILED(rc) && rc != FsError_PathAlreadyExists) {
        return rc;
    }

    File f;
    R_TRY(fs.OpenFile(path, FsOpenMode_Write, &f));
    R_TRY(f.SetSize(in.size()));
    R_TRY(f.Write(0, in.data(), in.size(), FsWriteOption_None));

    R_SUCCEED();
}

Result copy_entire_file(FsFileSystem* fs, const FsPath& dst, const FsPath& src, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(dst.s), Result_FsReadOnly);

    std::vector<u8> data;
    R_TRY(read_entire_file(fs, src, data));
    return write_entire_file(fs, dst, data, ignore_read_only);
}

Result CreateFile(const FsPath& path, u64 size, u32 option, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(path.s), Result_FsReadOnly);

    auto fd = open(path.s, O_WRONLY | O_CREAT, DEFFILEMODE);
    if (fd == -1) {
        if (errno == EEXIST) {
            return FsError_PathAlreadyExists;
        }

        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    ON_SCOPE_EXIT(close(fd));

    if (size) {
        R_UNLESS(!ftruncate(fd, size), Result_FsUnknownStdioError);
    }

    R_SUCCEED();
}

Result CreateDirectory(const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(path.s), Result_FsReadOnly);

    if (mkdir(path.s, ACCESSPERMS)) {
        if (errno == EEXIST) {
            return FsError_PathAlreadyExists;
        }

        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    R_SUCCEED();
}

Result CreateDirectoryRecursively(const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(path.s), Result_FsReadOnly);

    return CreateDirectoryRecursively(nullptr, path, ignore_read_only);
}

Result CreateDirectoryRecursivelyWithPath(const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(path.s), Result_FsReadOnly);

    return CreateDirectoryRecursivelyWithPath(nullptr, path, ignore_read_only);
}

Result DeleteFile(const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path.s), Result_FsReadOnly);

    if (unlink(path.s)) {
        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    R_SUCCEED();
}

Result DeleteDirectory(const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path.s), Result_FsReadOnly);

    if (rmdir(path.s)) {
        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    R_SUCCEED();
}

// recursive delete implemented for stdio path
Result DeleteDirectoryRecursively(const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path.s), Result_FsReadOnly);

    if (remove_dir_recursive_impl(path.s) != 0) {
        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    R_SUCCEED();
}

Result RenameFile(const FsPath& src, const FsPath& dst, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(src.s), Result_FsReadOnly);
    R_UNLESS(ignore_read_only || !is_read_only(dst.s), Result_FsReadOnly);

    if (rename(src.s, dst.s)) {
        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    R_SUCCEED();
}

Result RenameDirectory(const FsPath& src, const FsPath& dst, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(src.s), Result_FsReadOnly);
    R_UNLESS(ignore_read_only || !is_read_only(dst.s), Result_FsReadOnly);

    return RenameFile(src, dst, ignore_read_only);
}

Result GetEntryType(const FsPath& path, FsDirEntryType* out) {
    struct stat st;
    if (stat(path.s, &st)) {
        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    if (S_ISREG(st.st_mode)) {
        *out = FsDirEntryType_File;
    } else if (S_ISDIR(st.st_mode)) {
        *out = FsDirEntryType_Dir;
    } else {
        // treat other types (symlink, socket, etc.) as file for listing purposes
        *out = FsDirEntryType_File;
    }
    R_SUCCEED();
}

Result GetFileTimeStampRaw(const FsPath& path, FsTimeStampRaw *out) {
    struct stat st;
    if (stat(path.s, &st)) {
        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }

    out->is_valid = true;
    out->created = st.st_ctim.tv_sec;
    out->modified = st.st_mtim.tv_sec;
    out->accessed = st.st_atim.tv_sec;
    R_SUCCEED();
}

Result SetTimestamp(const FsPath& path, const FsTimeStampRaw* ts) {
    if (ts->is_valid) {
        timeval val[2]{};
        val[0].tv_sec = ts->accessed;
        val[1].tv_sec = ts->modified;

        if (utimes(path.s, val)) {
            log_write("utimes() failed: %d %s\n", errno, strerror(errno));
        }
    }

    R_SUCCEED();
}

bool FileExists(const FsPath& path) {
    FsDirEntryType type;
    R_TRY_RESULT(GetEntryType(path, &type), false);
    return type == FsDirEntryType_File;
}

bool DirExists(const FsPath& path) {
    FsDirEntryType type;
    R_TRY_RESULT(GetEntryType(path, &type), false);
    return type == FsDirEntryType_Dir;
}

Result read_entire_file(const FsPath& path, std::vector<u8>& out) {
    auto f = std::fopen(path.s, "rb");
    if (!f) {
        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    ON_SCOPE_EXIT(std::fclose(f));

    if (std::fseek(f, 0, SEEK_END) != 0) {
        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    long pos = std::ftell(f);
    if (pos < 0) {
        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    const auto size = static_cast<size_t>(pos);
    std::rewind(f);

    out.resize(size);

    size_t read = std::fread(out.data(), 1, out.size(), f);
    if (read != out.size()) {
        if (std::ferror(f)) {
            R_TRY(fsdevGetLastResult());
            return Result_FsUnknownStdioError;
        }
        // partial read due to EOF is tolerated but should resize their vector
        out.resize(read);
    }
    R_SUCCEED();
}

Result write_entire_file(const FsPath& path, const std::vector<u8>& in, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path.s), Result_FsReadOnly);

    auto f = std::fopen(path.s, "wb");
    if (!f) {
        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    ON_SCOPE_EXIT(std::fclose(f));

    size_t written = std::fwrite(in.data(), 1, in.size(), f);
    if (written != in.size()) {
        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    R_SUCCEED();
}

Result copy_entire_file(const FsPath& dst, const FsPath& src, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(dst.s), Result_FsReadOnly);

    std::vector<u8> data;
    R_TRY(read_entire_file(src, data));
    return write_entire_file(dst, data, ignore_read_only);
}

Result OpenFile(fs::Fs* fs, const fs::FsPath& path, u32 mode, File* f) {
    f->m_fs = fs;
    f->m_mode = mode;

    if (f->m_fs->IsNative()) {
        auto fsn = (fs::FsNative*)f->m_fs;
        R_TRY(fsFsOpenFile(&fsn->m_fs, path, mode, &f->m_native));
    } else {
        if ((mode & FsOpenMode_Read) && (mode & FsOpenMode_Write)) {
            f->m_stdio = std::fopen(path.s, "rb+");
        } else if (mode & FsOpenMode_Read) {
            f->m_stdio = std::fopen(path.s, "rb");
        } else if (mode & FsOpenMode_Write) {
            // not possible to open file with just write and not append
            // or create or truncate. So rw it is!
            f->m_stdio = std::fopen(path.s, "rb+");
        }

        R_UNLESS(f->m_stdio, Result_FsUnknownStdioError);
    }

    R_SUCCEED();
}

File::~File() {
    Close();
}

Result File::Read( s64 off, void* buf, u64 read_size, u32 option, u64* bytes_read) {
    *bytes_read = 0;
    R_UNLESS(m_fs, Result_FsNotActive);

    if (m_fs->IsNative()) {
        R_TRY(fsFileRead(&m_native, off, buf, read_size, option, bytes_read));
    } else {
        if (m_stdio_off != off) {
            m_stdio_off = off;
            std::fseek(m_stdio, off, SEEK_SET);
        }

        *bytes_read = std::fread(buf, 1, read_size, m_stdio);

        // if we read less bytes than expected, check if there was an error (ignoring eof).
        if (*bytes_read < read_size) {
            if (!std::feof(m_stdio) && std::ferror(m_stdio)) {
                R_THROW(Result_FsUnknownStdioError);
            }
        }

        m_stdio_off += *bytes_read;
    }

    R_SUCCEED();
}

Result File::Write(s64 off, const void* buf, u64 write_size, u32 option) {
    R_UNLESS(m_fs, Result_FsNotActive);

    if (m_fs->IsNative()) {
        R_TRY(fsFileWrite(&m_native, off, buf, write_size, option));
    } else {
        if (m_stdio_off != off) {
            log_write("[FS] diff seek\n");
            m_stdio_off = off;
            std::fseek(m_stdio, off, SEEK_SET);
        }

        const auto result = std::fwrite(buf, 1, write_size, m_stdio);
        // log_write("[FS] fwrite res: %zu vs %zu\n", result, write_size);
        R_UNLESS(result == write_size, Result_FsUnknownStdioError);

        m_stdio_off += write_size;
    }

    R_SUCCEED();
}

Result File::SetSize(s64 sz) {
    R_UNLESS(m_fs, Result_FsNotActive);

    if (m_fs->IsNative()) {
        R_TRY(fsFileSetSize(&m_native, sz));
    } else {
        const auto fd = fileno(m_stdio);
        R_UNLESS(fd > 0, Result_FsUnknownStdioError);
        R_UNLESS(!ftruncate(fd, sz), Result_FsUnknownStdioError);
    }

    R_SUCCEED();
}

Result File::GetSize(s64* out) {
    R_UNLESS(m_fs, Result_FsNotActive);

    if (m_fs->IsNative()) {
        R_TRY(fsFileGetSize(&m_native, out));
    } else {
        struct stat st;
        R_UNLESS(!fstat(fileno(m_stdio), &st), Result_FsUnknownStdioError);
        *out = st.st_size;
    }

    R_SUCCEED();
}

void File::Close() {
    if (!m_fs) {
        return;
    }

    if (m_fs->IsNative()) {
        if (serviceIsActive(&m_native.s)) {
            fsFileClose(&m_native);
            if (m_mode & FsOpenMode_Write) {
                m_fs->Commit();
            }
            m_native = {};
        }
    } else {
        if (m_stdio) {
            std::fclose(m_stdio);
            m_stdio = {};
        }
    }
}

Result OpenDirectory(fs::Fs* fs, const fs::FsPath& path, u32 mode, Dir* d) {
    d->m_fs = fs;
    d->m_mode = mode;

    if (d->m_fs->IsNative()) {
        auto fsn = (fs::FsNative*)d->m_fs;
        R_TRY(fsFsOpenDirectory(&fsn->m_fs, path, mode, &d->m_native));
    } else {
        d->m_stdio = opendir(path.s);
        R_UNLESS(d->m_stdio, Result_FsUnknownStdioError);
    }

    R_SUCCEED();
}

Result DirGetEntryCount(fs::Fs* m_fs, const fs::FsPath& path, s64* count, u32 mode) {
    s64 file_count, dir_count;
    R_TRY(DirGetEntryCount(m_fs, path, &file_count, &dir_count, mode));
    *count = file_count + dir_count;
    R_SUCCEED();
}

Result DirGetEntryCount(fs::Fs* m_fs, const fs::FsPath& path, s64* file_count, s64* dir_count, u32 mode) {
    *file_count = *dir_count = 0;

    if (m_fs->IsNative()) {
        if (mode & FsDirOpenMode_ReadDirs){
            fs::Dir dir;
            R_TRY(m_fs->OpenDirectory(path, FsDirOpenMode_ReadDirs|FsDirOpenMode_NoFileSize, &dir));
            R_TRY(dir.GetEntryCount(dir_count));
        }
        if (mode & FsDirOpenMode_ReadFiles){
            fs::Dir dir;
            R_TRY(m_fs->OpenDirectory(path, FsDirOpenMode_ReadFiles|FsDirOpenMode_NoFileSize, &dir));
            R_TRY(dir.GetEntryCount(file_count));
        }
    } else {
        fs::Dir dir;
        R_TRY(m_fs->OpenDirectory(path, mode, &dir));

        while (auto d = readdir(dir.m_stdio)) {
            if (!std::strcmp(d->d_name, ".") || !std::strcmp(d->d_name, "..")) {
                continue;
            }

            if (d->d_type == DT_DIR) {
                if (!(mode & FsDirOpenMode_ReadDirs)) {
                    continue;
                }
                (*dir_count)++;
            } else if (d->d_type == DT_REG) {
                if (!(mode & FsDirOpenMode_ReadFiles)) {
                    continue;
                }
                (*file_count)++;
            } else {
                // Unknown d_type: use lstat to identify
                std::string child;
                child.reserve(std::strlen(path.s) + 1 + std::strlen(d->d_name) + 1);
                child = path.s;
                if (child.back() != '/') child.push_back('/');
                child += d->d_name;
                struct stat st;
                if (lstat(child.c_str(), &st) == 0) {
                    if (S_ISDIR(st.st_mode) && (mode & FsDirOpenMode_ReadDirs)) {
                        (*dir_count)++;
                    } else if (S_ISREG(st.st_mode) && (mode & FsDirOpenMode_ReadFiles)) {
                        (*file_count)++;
                    }
                }
            }
        }
    }

    R_SUCCEED();
}

Dir::~Dir() {
    Close();
}

Result Dir::GetEntryCount(s64* out) {
    *out = 0;
    R_UNLESS(m_fs, Result_FsNotActive);

    if (m_fs->IsNative()) {
        R_TRY(fsDirGetEntryCount(&m_native, out));
    } else {
        while (auto d = readdir(m_stdio)) {
            if (!std::strcmp(d->d_name, ".") || !std::strcmp(d->d_name, "..")) {
                continue;
            }
            (*out)++;
        }

        // NOTE: this will *not* work for native mounted folders!!!
        rewinddir(m_stdio);
    }

    R_SUCCEED();
}

Result Dir::Read(s64 *total_entries, size_t max_entries, FsDirectoryEntry *buf) {
    R_UNLESS(m_fs, Result_FsNotActive);
    *total_entries = 0;

    if (m_fs->IsNative()) {
        R_TRY(fsDirRead(&m_native, total_entries, max_entries, buf));
    } else {
        while (auto d = readdir(m_stdio)) {
            if (!std::strcmp(d->d_name, ".") || !std::strcmp(d->d_name, "..")) {
                continue;
            }

            FsDirectoryEntry entry{};

            if (d->d_type == DT_DIR) {
                if (!(m_mode & FsDirOpenMode_ReadDirs)) {
                    continue;
                }
                entry.type = FsDirEntryType_Dir;
            } else if (d->d_type == DT_REG) {
                if (!(m_mode & FsDirOpenMode_ReadFiles)) {
                    continue;
                }
                entry.type = FsDirEntryType_File;
            } else {
                // Unknown d_type: try to lstat
                std::string child;
                child.reserve(256);
                child = m_fs->IsNative() ? std::string("") : std::string(""); // placeholder
                // We don't have full path here easily; fall back to treating it as file
                log_write("[FS] WARNING: unknown type when reading dir: %u\n", d->d_type);
                entry.type = FsDirEntryType_File;
            }

            std::strcpy(entry.name, d->d_name);
            std::memcpy(&buf[*total_entries], &entry, sizeof(*buf));
            *total_entries = *total_entries + 1;
            if (*total_entries >= max_entries) {
                break;
            }
        }
    }

    R_SUCCEED();
}

Result Dir::ReadAll(std::vector<FsDirectoryEntry>& buf) {
    buf.clear();
    R_UNLESS(m_fs, Result_FsNotActive);

    if (m_fs->IsNative()) {
        s64 count;
        R_TRY(GetEntryCount(&count));

        buf.resize(count);
        R_TRY(fsDirRead(&m_native, &count, buf.size(), buf.data()));
        buf.resize(count);
    } else {
        buf.reserve(1000);

        while (auto d = readdir(m_stdio)) {
            if (!std::strcmp(d->d_name, ".") || !std::strcmp(d->d_name, "..")) {
                continue;
            }

            FsDirectoryEntry entry{};

            if (d->d_type == DT_DIR) {
                if (!(m_mode & FsDirOpenMode_ReadDirs)) {
                    continue;
                }
                entry.type = FsDirEntryType_Dir;
            } else if (d->d_type == DT_REG) {
                if (!(m_mode & FsDirOpenMode_ReadFiles)) {
                    continue;
                }
                entry.type = FsDirEntryType_File;
            } else {
                log_write("[FS] WARNING: unknown type when reading dir: %u\n", d->d_type);
                continue;
            }

            std::strcpy(entry.name, d->d_name);
            buf.emplace_back(entry);
        }
    }

    R_SUCCEED();
}

void Dir::Close() {
    if (!m_fs) {
        return;
    }

    if (m_fs->IsNative()) {
        if (serviceIsActive(&m_native.s)) {
            fsDirClose(&m_native);
            m_native = {};
        }
    } else {
        if (m_stdio) {
            closedir(m_stdio);
            m_stdio = {};
        }
    }
}

Result FileGetSizeAndTimestamp(fs::Fs* m_fs, const FsPath& path, FsTimeStampRaw* ts, s64* size) {
    *ts = {};
    *size = {};

    if (m_fs->IsNative()) {
        auto fsn = (fs::FsNative*)m_fs;
        R_TRY(fsn->GetFileTimeStampRaw(path, ts));

        File f;
        R_TRY(m_fs->OpenFile(path, FsOpenMode_Read, &f));
        R_TRY(f.GetSize(size));
    } else {
        struct stat st;
        R_UNLESS(!lstat(path.s, &st), Result_FsFailedStdioStat);

        ts->is_valid = true;
        ts->created = st.st_ctim.tv_sec;
        ts->modified = st.st_mtim.tv_sec;
        ts->accessed = st.st_atim.tv_sec;
        *size = st.st_size;
    }

    R_SUCCEED();
}

Result IsDirEmpty(fs::Fs* m_fs, const fs::FsPath& path, bool* out) {
    *out = true;

    if (m_fs->IsNative()) {
        auto fsn = (fs::FsNative*)m_fs;

        s64 count;
        R_TRY(fsn->DirGetEntryCount(path, &count, FsDirOpenMode_ReadDirs | FsDirOpenMode_ReadFiles));
        *out = !count;
    } else {
        auto dir = opendir(path.s);
        R_UNLESS(dir, Result_FsFailedStdioOpendir);
        ON_SCOPE_EXIT(closedir(dir));

        while (auto d = readdir(dir)) {
            if (!std::strcmp(d->d_name, ".") || !std::strcmp(d->d_name, "..")) {
                continue;
            }

            *out = false;
            break;
        }
    }

    R_SUCCEED();
}

} // namespace fs
