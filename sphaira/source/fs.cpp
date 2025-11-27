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

#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <ftw.h>

namespace fs {
namespace {

static_assert(FsPath::Test("abc"));
static_assert(FsPath::Test(std::string_view{"abc"}));
static_assert(FsPath::Test(std::string{"abc"}));
static_assert(FsPath::Test(FsPath{"abc"}));

static_assert(FsPath::TestFrom("abc"));
static_assert(FsPath::TestFrom(std::string_view{"abc"}));
static_assert(FsPath::TestFrom(std::string{"abc"}));
static_assert(FsPath::TestFrom(FsPath{"abc"}));

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

} // namespace

FsPath AppendPath(const FsPath& root_path, const FsPath& _file_path) {
    // strip leading '/' in file path.
    auto file_path = _file_path.s;
    while (file_path[0] == '/') {
        file_path++;
    }

    FsPath path;
    if (root_path[std::strlen(root_path) - 1] != '/') {
        std::snprintf(path, sizeof(path), "%s/%s", root_path.s, file_path);
    } else {
        std::snprintf(path, sizeof(path), "%s%s", root_path.s, file_path);
    }
    return path;
}

Result read_entire_file(Fs* fs, const FsPath& path, std::vector<u8>& out) {
    File f;
    R_TRY(fs->OpenFile(path, FsOpenMode_Read, &f));

    s64 size;
    R_TRY(f.GetSize(&size));
    out.resize(size);

    u64 bytes_read;
    R_TRY(f.Read(0, out.data(), out.size(), FsReadOption_None, &bytes_read));
    R_UNLESS(bytes_read == out.size(), 1);

    R_SUCCEED();
}

Result write_entire_file(Fs* fs, const FsPath& path, std::span<const u8> in, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path), Result_FsReadOnly);

    if (auto rc = fs->CreateFile(path, in.size(), 0); R_FAILED(rc) && rc != FsError_PathAlreadyExists) {
        return rc;
    }

    File f;
    R_TRY(fs->OpenFile(path, FsOpenMode_Write, &f));
    R_TRY(f.SetSize(in.size()));
    R_TRY(f.Write(0, in.data(), in.size(), FsWriteOption_None));

    R_SUCCEED();
}

Result copy_entire_file(Fs* fs, const FsPath& dst, const FsPath& src, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(dst), Result_FsReadOnly);

    std::vector<u8> data;
    R_TRY(read_entire_file(fs, src, data));
    return write_entire_file(fs, dst, data, ignore_read_only);
}

Result CreateFile(FsFileSystem* fs, const FsPathReal& path, u64 size, u32 option, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(path), Result_FsReadOnly);

    if (size >= 1024ULL*1024ULL*1024ULL*4ULL) {
        option |= FsCreateOption_BigFile;
    }

    log_write("trying to create path: %s\n", path.s);
    R_TRY(fsFsCreateFile(fs, path, size, option));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result CreateDirectory(FsFileSystem* fs, const FsPathReal& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(path), Result_FsReadOnly);

    R_TRY(fsFsCreateDirectory(fs, path));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result CreateDirectoryRecursively(FsFileSystem* fs, const FsPath& _path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(_path), Result_FsReadOnly);

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

    auto path_view = std::string_view{_path};
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
        std::strncat(path, dir.data(), dir.size());
        log_write("[FS] dir creation path is now: %s\n", path.s);

        if (fs) {
            rc = CreateDirectory(fs, path, ignore_read_only);
        } else {
            rc = CreateDirectory(path, ignore_read_only);
        }

        std::strcat(path, "/");
    }

    // only check if the last folder creation failed.
    // reason being is that it may try to create "/" root folder, which some network
    // fs will return a EPERM/EACCES error.
    // however if the last directory failed, then it is a real error.
    if (R_FAILED(rc) && rc != FsError_PathAlreadyExists) {
        log_write("failed to create folder: %s\n", path.s);
        return rc;
    }

    R_SUCCEED();
}

Result CreateDirectoryRecursivelyWithPath(FsFileSystem* fs, const FsPath& _path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(_path), Result_FsReadOnly);

    // strip file name form path.
    const auto last_slash = std::strrchr(_path, '/');
    if (!last_slash || last_slash == _path.s) {
        R_SUCCEED();
    }

    FsPath new_path{};
    std::snprintf(new_path, sizeof(new_path), "%.*s", (int)(last_slash - _path.s), _path.s);
    R_TRY(CreateDirectoryRecursively(fs, new_path, ignore_read_only));
    R_SUCCEED();
}

Result DeleteFile(FsFileSystem* fs, const FsPathReal& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path), Result_FsReadOnly);
    R_TRY(fsFsDeleteFile(fs, path));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result DeleteDirectory(FsFileSystem* fs, const FsPathReal& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path), Result_FsReadOnly);

    R_TRY(fsFsDeleteDirectory(fs, path));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result DeleteDirectoryRecursively(FsFileSystem* fs, const FsPathReal& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path), Result_FsReadOnly);

    R_TRY(fsFsDeleteDirectoryRecursively(fs, path));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result RenameFile(FsFileSystem* fs, const FsPathReal& src, const FsPathReal& dst, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(src), Result_FsReadOnly);
    R_UNLESS(ignore_read_only || !is_read_only(dst), Result_FsReadOnly);

    R_TRY(fsFsRenameFile(fs, src, dst));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result RenameDirectory(FsFileSystem* fs, const FsPathReal& src, const FsPathReal& dst, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(src), Result_FsReadOnly);
    R_UNLESS(ignore_read_only || !is_read_only(dst), Result_FsReadOnly);

    R_TRY(fsFsRenameDirectory(fs, src, dst));
    fsFsCommit(fs);
    R_SUCCEED();
}

Result GetEntryType(FsFileSystem* fs, const FsPathReal& path, FsDirEntryType* out) {
    return fsFsGetEntryType(fs, path, out);
}

Result GetFileTimeStampRaw(FsFileSystem* fs, const FsPathReal& path, FsTimeStampRaw *out) {
    return fsFsGetFileTimeStampRaw(fs, path, out);
}

Result SetTimestamp(FsFileSystem* fs, const FsPathReal& path, const FsTimeStampRaw* ts) {
    // unsuported.
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

Result CreateFile(const FsPathReal& path, u64 size, u32 option, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(path), Result_FsReadOnly);

    auto fd = open(path, O_WRONLY | O_CREAT, DEFFILEMODE);
    if (fd == -1) {
        if (errno == EEXIST) {
            return FsError_PathAlreadyExists;
        }

        R_TRY(fsdevGetLastResult());
        return Result_FsStdioFailedToCreate;
    }
    ON_SCOPE_EXIT(close(fd));

    if (size) {
        R_UNLESS(!ftruncate(fd, size), Result_FsStdioFailedToTruncate);
    }

    R_SUCCEED();
}

Result CreateDirectory(const FsPathReal& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(path), Result_FsReadOnly);

    if (mkdir(path, ACCESSPERMS)) {
        if (errno == EEXIST) {
            return FsError_PathAlreadyExists;
        }

        R_TRY(fsdevGetLastResult());
        return Result_FsStdioFailedToCreateDirectory;
    }
    R_SUCCEED();
}

Result CreateDirectoryRecursively(const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(path), Result_FsReadOnly);

    return CreateDirectoryRecursively(nullptr, path, ignore_read_only);
}

Result CreateDirectoryRecursivelyWithPath(const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only_root(path), Result_FsReadOnly);

    return CreateDirectoryRecursivelyWithPath(nullptr, path, ignore_read_only);
}

Result DeleteFile(const FsPathReal& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path), Result_FsReadOnly);

    if (unlink(path)) {
        R_TRY(fsdevGetLastResult());
        return Result_FsStdioFailedToDeleteFile;
    }
    R_SUCCEED();
}

Result DeleteDirectory(const FsPathReal& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path), Result_FsReadOnly);

    if (rmdir(path)) {
        R_TRY(fsdevGetLastResult());
        return Result_FsStdioFailedToDeleteDirectory;
    }
    R_SUCCEED();
}

// ftw / ntfw isn't found by linker...
Result DeleteDirectoryRecursively(const FsPath& path, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(path), Result_FsReadOnly);

    #if 0
    // const auto unlink_cb = [](const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) -> int {
    const auto unlink_cb = [](const char *fpath, const struct stat *sb, int typeflag) -> int {
        return remove(fpath);
    };
    // todo: check for reasonable max fd limit
    // if (nftw(path, unlink_cb, 16, FTW_DEPTH)) {
    if (ftw(path, unlink_cb, 16)) {
        R_TRY(fsdevGetLastResult());
        return Result_FsUnknownStdioError;
    }
    R_SUCCEED();
    #else
    R_THROW(0xFFFF);
    #endif
}

Result RenameFile(const FsPathReal& src, const FsPathReal& dst, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(src), Result_FsReadOnly);
    R_UNLESS(ignore_read_only || !is_read_only(dst), Result_FsReadOnly);

    if (rename(src, dst)) {
        R_TRY(fsdevGetLastResult());
        return Result_FsStdioFailedToRename;
    }
    R_SUCCEED();
}

Result RenameDirectory(const FsPathReal& src, const FsPathReal& dst, bool ignore_read_only) {
    R_UNLESS(ignore_read_only || !is_read_only(src), Result_FsReadOnly);
    R_UNLESS(ignore_read_only || !is_read_only(dst), Result_FsReadOnly);

    return RenameFile(src, dst, ignore_read_only);
}

Result GetEntryType(const FsPathReal& path, FsDirEntryType* out) {
    struct stat st;
    if (stat(path, &st)) {
        R_TRY(fsdevGetLastResult());
        return Result_FsStdioFailedToStat;
    }
    *out = S_ISREG(st.st_mode) ? FsDirEntryType_File : FsDirEntryType_Dir;
    R_SUCCEED();
}

Result GetFileTimeStampRaw(const FsPathReal& path, FsTimeStampRaw *out) {
    struct stat st;
    if (stat(path, &st)) {
        R_TRY(fsdevGetLastResult());
        return Result_FsStdioFailedToStat;
    }

    out->is_valid = true;
    out->created = st.st_ctim.tv_sec;
    out->modified = st.st_mtim.tv_sec;
    out->accessed = st.st_atim.tv_sec;
    R_SUCCEED();
}

Result SetTimestamp(const FsPathReal& path, const FsTimeStampRaw* ts) {
    if (ts->is_valid) {
        timeval val[2]{};
        val[0].tv_sec = ts->accessed;
        val[1].tv_sec = ts->modified;

        if (utimes(path, val)) {
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

Result OpenFile(fs::Fs* fs, const FsPathReal& path, u32 mode, File* f) {
    const auto should_buffer = (mode & OpenMode_EnableBuffer);
    // remove the invalid flag so that native fs doesn't error.
    mode &= ~OpenMode_EnableBuffer;

    f->m_fs = fs;
    f->m_mode = mode;

    if (f->m_fs->IsNative()) {
        auto fs = (fs::FsNative*)f->m_fs;
        R_TRY(fsFsOpenFile(&fs->m_fs, path, mode, &f->m_native));
    } else {
        if ((mode & FsOpenMode_Read) && (mode & FsOpenMode_Write)) {
            f->m_stdio = std::fopen(path, "rb+");
        } else if (mode & FsOpenMode_Read) {
            f->m_stdio = std::fopen(path, "rb");
        } else if (mode & FsOpenMode_Write) {
            // not possible to open file with just write and not append
            // or create or truncate. So rw it is!
            f->m_stdio = std::fopen(path, "rb+");
        }

        R_UNLESS(f->m_stdio, Result_FsStdioFailedToOpenFile);

        // disable buffering to match native fs behavior.
        // this also causes problems with network io as it will do double reads.
        // which kills performance (see sftp).
        if (!should_buffer) {
            std::setvbuf(f->m_stdio, nullptr, _IONBF, 0);
        }
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
        R_UNLESS(m_stdio, Result_FsUnknownStdioError);

        if (off != std::ftell(m_stdio)) {
            const auto ret = std::fseek(m_stdio, off, SEEK_SET);
            log_write("[FS] fseek to %ld ret: %d new_off: %zd\n", off, ret, std::ftell(m_stdio));
            R_UNLESS(ret == 0, Result_FsStdioFailedToSeek);
            R_UNLESS(off == std::ftell(m_stdio), Result_FsStdioFailedToSeek);
        }

        *bytes_read = std::fread(buf, 1, read_size, m_stdio);

        // if we read less bytes than expected, check if there was an error (ignoring eof).
        if (*bytes_read < read_size) {
            if (!std::feof(m_stdio) && std::ferror(m_stdio)) {
                log_write("[FS] fread error: %d\n", std::ferror(m_stdio));
                R_THROW(Result_FsStdioFailedToRead);
            }
        }
    }

    R_SUCCEED();
}

Result File::Write(s64 off, const void* buf, u64 write_size, u32 option) {
    R_UNLESS(m_fs, Result_FsNotActive);

    if (m_fs->IsNative()) {
        R_TRY(fsFileWrite(&m_native, off, buf, write_size, option));
    } else {
        R_UNLESS(m_stdio, Result_FsUnknownStdioError);

        if (off != std::ftell(m_stdio)) {
            const auto ret = std::fseek(m_stdio, off, SEEK_SET);
            R_UNLESS(ret == 0, Result_FsStdioFailedToSeek);
            R_UNLESS(off == std::ftell(m_stdio), Result_FsStdioFailedToSeek);
        }

        const auto result = std::fwrite(buf, 1, write_size, m_stdio);
        // log_write("[FS] fwrite res: %zu vs %zu\n", result, write_size);
        R_UNLESS(result == write_size, Result_FsStdioFailedToWrite);
    }

    R_SUCCEED();
}

Result File::SetSize(s64 sz) {
    R_UNLESS(m_fs, Result_FsNotActive);

    if (m_fs->IsNative()) {
        R_TRY(fsFileSetSize(&m_native, sz));
    } else {
        R_UNLESS(m_stdio, Result_FsUnknownStdioError);
        const auto fd = fileno(m_stdio);
        R_UNLESS(fd > 0, Result_FsStdioFailedToTruncate);
        R_UNLESS(!ftruncate(fd, sz), Result_FsStdioFailedToTruncate);
    }

    R_SUCCEED();
}

Result File::GetSize(s64* out) {
    R_UNLESS(m_fs, Result_FsNotActive);

    if (m_fs->IsNative()) {
        R_TRY(fsFileGetSize(&m_native, out));
    } else {
        R_UNLESS(m_stdio, Result_FsUnknownStdioError);

        struct stat st;
        R_UNLESS(!fstat(fileno(m_stdio), &st), Result_FsStdioFailedToStat);
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
            log_write("[FS] closing stdio file\n");
            std::fclose(m_stdio);
            m_stdio = {};
        }
    }
}

Result OpenDirectory(fs::Fs* fs, const FsPathReal& path, u32 mode, Dir* d) {
    d->m_fs = fs;
    d->m_mode = mode;

    if (d->m_fs->IsNative()) {
        auto fs = (fs::FsNative*)d->m_fs;
        R_TRY(fsFsOpenDirectory(&fs->m_fs, path, mode, &d->m_native));
    } else {
        d->m_stdio = opendir(path);
        R_UNLESS(d->m_stdio, Result_FsStdioFailedToOpenDirectory);
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

            if (d->d_type == DT_DIR) {
                if (!(m_mode & FsDirOpenMode_ReadDirs)) {
                    continue;
                }
            } else if (d->d_type == DT_REG) {
                if (!(m_mode & FsDirOpenMode_ReadFiles)) {
                    continue;
                }
            } else {
                log_write("[FS] WARNING: unknown type when counting dir: %u\n", d->d_type);
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
                log_write("[FS] WARNING: unknown type when reading dir: %u\n", d->d_type);
                continue;
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
        auto fs = (fs::FsNative*)m_fs;
        R_TRY(fs->GetFileTimeStampRaw(path, ts));

        File f;
        R_TRY(m_fs->OpenFile(path, FsOpenMode_Read, &f));
        R_TRY(f.GetSize(size));
    } else {
        struct stat st;
        R_UNLESS(!lstat(path, &st), Result_FsFailedStdioStat);

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
        auto fs = (fs::FsNative*)m_fs;

        s64 count;
        R_TRY(fs->DirGetEntryCount(path, &count, FsDirOpenMode_ReadDirs | FsDirOpenMode_ReadFiles));
        *out = !count;
    } else {
        auto dir = opendir(path);
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
