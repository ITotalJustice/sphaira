#include "minizip_helper.hpp"
#include <minizip/unzip.h>
#include <minizip/zip.h>
#include <cstring>
#include <cstdio>

#include "log.hpp"

namespace sphaira::mz {
namespace {

// mmz is part of ftpsrv code.
#define LOCAL_HEADER_SIG 0x4034B50
#define FILE_HEADER_SIG 0x2014B50
#define END_RECORD_SIG 0x6054B50

// 30 bytes (0x1E)
#pragma pack(push,1)
typedef struct mmz_LocalHeader {
    uint32_t sig;
    uint16_t version;
    uint16_t flags;
    uint16_t compression;
    uint16_t modtime;
    uint16_t moddate;
    uint32_t crc32;
    uint32_t compressed_size;
    uint32_t uncompressed_size;
    uint16_t filename_len;
    uint16_t extrafield_len;
} mmz_LocalHeader;
#pragma pack(pop)

// 46 bytes (0x2E)
#pragma pack(push,1)
typedef struct mmz_FileHeader {
    uint32_t sig;
    uint16_t version;
    uint16_t version_needed;
    uint16_t flags;
    uint16_t compression;
    uint16_t modtime;
    uint16_t moddate;
    uint32_t crc32;
    uint32_t compressed_size;
    uint32_t uncompressed_size;
    uint16_t filename_len;
    uint16_t extrafield_len;
    uint16_t filecomment_len;
    uint16_t disk_start; // wat
    uint16_t internal_attr; // wat
    uint32_t external_attr; // wat
    uint32_t local_hdr_off;
} mmz_FileHeader;
#pragma pack(pop)

#pragma pack(push,1)
typedef struct mmz_EndRecord {
    uint32_t sig;
    uint16_t disk_number;
    uint16_t disk_wcd;
    uint16_t disk_entries;
    uint16_t total_entries;
    uint32_t central_directory_size;
    uint32_t file_hdr_off;
    uint16_t comment_len;
} mmz_EndRecord;
#pragma pack(pop)

static_assert(sizeof(mmz_LocalHeader) == 0x1E);
static_assert(sizeof(mmz_FileHeader) == 0x2E);
static_assert(sizeof(mmz_EndRecord) == 0x16);

voidpf minizip_open_file_func_mem(voidpf opaque, const void* filename, int mode) {
    return opaque;
}

ZPOS64_T minizip_tell_file_func_mem(voidpf opaque, voidpf stream) {
    auto mem = static_cast<const MzMem*>(opaque);
    return mem->offset;
}

long minizip_seek_file_func_mem(voidpf opaque, voidpf stream, ZPOS64_T offset, int origin) {
    auto mem = static_cast<MzMem*>(opaque);
    size_t new_offset = 0;

    switch (origin) {
        case ZLIB_FILEFUNC_SEEK_SET: new_offset = offset; break;
        case ZLIB_FILEFUNC_SEEK_CUR: new_offset = mem->offset + offset; break;
        case ZLIB_FILEFUNC_SEEK_END: new_offset = (mem->buf.size() - 1) + offset; break;
        default: return -1;
    }

    if (new_offset > mem->buf.size()) {
        return -1;
    }

    mem->offset = new_offset;
    return 0;
}

uLong minizip_read_file_func_mem(voidpf opaque, voidpf stream, void* buf, uLong size) {
    auto mem = static_cast<MzMem*>(opaque);

    size = std::min(size, mem->buf.size() - mem->offset);
    std::memcpy(buf, mem->buf.data() + mem->offset, size);
    mem->offset += size;

    return size;
}

uLong minizip_write_file_func_mem(voidpf opaque, voidpf stream, const void* buf, uLong size) {
    auto mem = static_cast<MzMem*>(opaque);

    // give it more memory
    if (mem->buf.capacity() < mem->offset + size) {
        mem->buf.reserve(mem->buf.capacity() + 1024*1024*64);
    }

    if (mem->buf.size() < mem->offset + size) {
        mem->buf.resize(mem->offset + size);
    }

    std::memcpy(mem->buf.data() + mem->offset, buf, size);
    mem->offset += size;

    return size;
}

int minizip_close_file_func_mem(voidpf opaque, voidpf stream) {
    return 0;
}

constexpr zlib_filefunc64_def zlib_filefunc_mem = {
    .zopen64_file = minizip_open_file_func_mem,
    .zread_file = minizip_read_file_func_mem,
    .zwrite_file = minizip_write_file_func_mem,
    .ztell64_file = minizip_tell_file_func_mem,
    .zseek64_file = minizip_seek_file_func_mem,
    .zclose_file = minizip_close_file_func_mem,
};

voidpf minizip_open_file_func_span(voidpf opaque, const void* filename, int mode) {
    return opaque;
}

ZPOS64_T minizip_tell_file_func_span(voidpf opaque, voidpf stream) {
    auto mem = static_cast<const MzSpan*>(opaque);
    return mem->offset;
}

long minizip_seek_file_func_span(voidpf opaque, voidpf stream, ZPOS64_T offset, int origin) {
    auto mem = static_cast<MzSpan*>(opaque);
    size_t new_offset = 0;

    switch (origin) {
        case ZLIB_FILEFUNC_SEEK_SET: new_offset = offset; break;
        case ZLIB_FILEFUNC_SEEK_CUR: new_offset = mem->offset + offset; break;
        case ZLIB_FILEFUNC_SEEK_END: new_offset = (mem->buf.size() - 1) + offset; break;
        default: return -1;
    }

    if (new_offset > mem->buf.size()) {
        return -1;
    }

    mem->offset = new_offset;
    return 0;
}

uLong minizip_read_file_func_span(voidpf opaque, voidpf stream, void* buf, uLong size) {
    auto mem = static_cast<MzSpan*>(opaque);

    size = std::min(size, mem->buf.size() - mem->offset);
    std::memcpy(buf, mem->buf.data() + mem->offset, size);
    mem->offset += size;

    return size;
}

int minizip_close_file_func_span(voidpf opaque, voidpf stream) {
    return 0;
}

constexpr zlib_filefunc64_def zlib_filefunc_span = {
    .zopen64_file = minizip_open_file_func_span,
    .zread_file = minizip_read_file_func_span,
    .ztell64_file = minizip_tell_file_func_span,
    .zseek64_file = minizip_seek_file_func_span,
    .zclose_file = minizip_close_file_func_span,
};

voidpf minizip_open_file_func_stdio(voidpf opaque, const void* filename, int mode) {
    const char* mode_fopen = NULL;
    if ((mode & ZLIB_FILEFUNC_MODE_READWRITEFILTER) == ZLIB_FILEFUNC_MODE_READ) {
        mode_fopen = "rb";
    } else if (mode & ZLIB_FILEFUNC_MODE_EXISTING) {
        log_write("[ZIP] opening r/w\n");
        mode_fopen = "r+b";
    } else if (mode & ZLIB_FILEFUNC_MODE_CREATE) {
        log_write("[ZIP] opening r/w +\n");
        mode_fopen = "wb";
    } else {
        return NULL;
    }

    auto f = std::fopen((const char*)filename, mode_fopen);
    if (f) {
        std::setvbuf(f, nullptr, _IOFBF, 1024 * 512);
    }
    return f;
}

ZPOS64_T minizip_tell_file_func_stdio(voidpf opaque, voidpf stream) {
    auto file = static_cast<std::FILE*>(stream);
    return std::ftell(file);
}

long minizip_seek_file_func_stdio(voidpf opaque, voidpf stream, ZPOS64_T offset, int origin) {
    auto file = static_cast<std::FILE*>(stream);
    return std::fseek(file, offset, origin);
}

uLong minizip_read_file_func_stdio(voidpf opaque, voidpf stream, void* buf, uLong size) {
    auto file = static_cast<std::FILE*>(stream);
    return std::fread(buf, 1, size, file);
}

uLong minizip_write_file_func_stdio(voidpf opaque, voidpf stream, const void* buf, uLong size) {
    auto file = static_cast<std::FILE*>(stream);
    return std::fwrite(buf, 1, size, file);
}

int minizip_close_file_func_stdio(voidpf opaque, voidpf stream) {
    auto file = static_cast<std::FILE*>(stream);
    if (file) {
        return std::fclose(file);
    }
    return 0;
}

int minizip_error_file_func_stdio(voidpf opaque, voidpf stream) {
    auto file = static_cast<std::FILE*>(stream);
    if (file) {
        return std::ferror(file);
    }
    return 0;
}

constexpr zlib_filefunc64_def zlib_filefunc_stdio = {
    .zopen64_file = minizip_open_file_func_stdio,
    .zread_file = minizip_read_file_func_stdio,
    .zwrite_file = minizip_write_file_func_stdio,
    .ztell64_file = minizip_tell_file_func_stdio,
    .zseek64_file = minizip_seek_file_func_stdio,
    .zclose_file = minizip_close_file_func_stdio,
    .zerror_file = minizip_error_file_func_stdio,
};






struct Internal {
    FsFile file;
    s64 offset;
    s64 size;
    Result rc;
};

static void* zopen64_file(void* opaque, const void* filename, int mode)
{
    struct Internal* fs = (struct Internal*)calloc(1, sizeof(*fs));
    if (R_FAILED(fs->rc = fsFsOpenFile(fsdevGetDeviceFileSystem("sdmc:"), (const char*)filename, FsOpenMode_Read, &fs->file))) {
        free(fs);
        return NULL;
    }

    if (R_FAILED(fs->rc = fsFileGetSize(&fs->file, &fs->size))) {
        free(fs);
        return NULL;
    }

    return fs;
}

static uLong zread_file(void* opaque, void* stream, void* buf, unsigned long size)
{
    struct Internal* fs = (struct Internal*)stream;

    u64 bytes_read;
    if (R_FAILED(fs->rc = fsFileRead(&fs->file, fs->offset, buf, size, 0, &bytes_read))) {
        return 0;
    }

    fs->offset += bytes_read;
    return bytes_read;
}

static ZPOS64_T ztell64_file(void* opaque, void* stream)
{
    struct Internal* fs = (struct Internal*)stream;
    return fs->offset;
}

static long zseek64_file(void* opaque, void* stream, ZPOS64_T offset, int origin)
{
    struct Internal* fs = (struct Internal*)stream;
    switch (origin) {
        case SEEK_SET: {
            fs->offset = offset;
        } break;
        case SEEK_CUR: {
            fs->offset += offset;
        } break;
        case SEEK_END: {
            fs->offset = fs->size + offset;
        } break;
    }
    return 0;
}

static int zclose_file(void* opaque, void* stream)
{
    if (stream) {
        struct Internal* fs = (struct Internal*)stream;
        fsFileClose(&fs->file);
        memset(fs, 0, sizeof(*fs));
        free(fs);
    }
    return 0;
}

static int zerror_file(void* opaque, void* stream)
{
    struct Internal* fs = (struct Internal*)stream;
    if (R_FAILED(fs->rc)) {
        return -1;
    }
    return 0;
}

static const zlib_filefunc64_def zlib_filefunc_native = {
    .zopen64_file = zopen64_file,
    .zread_file = zread_file,
    .ztell64_file = ztell64_file,
    .zseek64_file = zseek64_file,
    .zclose_file = zclose_file,
    .zerror_file = zerror_file,
};

} // namespace

void FileFuncMem(MzMem* mem, zlib_filefunc64_def* funcs) {
    *funcs = zlib_filefunc_mem;
    funcs->opaque = mem;
}

void FileFuncSpan(MzSpan* span, zlib_filefunc64_def* funcs) {
    *funcs = zlib_filefunc_span;
    funcs->opaque = span;
}

void FileFuncStdio(zlib_filefunc64_def* funcs) {
    *funcs = zlib_filefunc_stdio;
}

void FileFuncNative(zlib_filefunc64_def* funcs) {
    *funcs = zlib_filefunc_native;
}

Result PeekFirstFileName(fs::Fs* fs, const fs::FsPath& path, fs::FsPath& name) {
    fs::File file;
    R_TRY(fs->OpenFile(path, fs::OpenMode_ReadBuffered, &file));

    mmz_LocalHeader local_hdr;
    u64 bytes_read;
    R_TRY(file.Read(0, &local_hdr, sizeof(local_hdr), 0, &bytes_read));

    R_UNLESS(bytes_read == sizeof(local_hdr), Result_MmzBadLocalHeaderRead);
    R_UNLESS(local_hdr.sig == LOCAL_HEADER_SIG, Result_MmzBadLocalHeaderSig);

    const auto name_len = std::min<u64>(local_hdr.filename_len, sizeof(name) - 1);
    R_TRY(file.Read(bytes_read, name, name_len, 0, &bytes_read));
    name[name_len] = '\0';

    R_SUCCEED();
}

} // namespace sphaira::mz
