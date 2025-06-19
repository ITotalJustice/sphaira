#include "minizip_helper.hpp"
#include <minizip/unzip.h>
#include <minizip/zip.h>
#include <cstring>
#include <cstdio>

namespace sphaira::mz {
namespace {

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
        mode_fopen = "r+b";
    } else if (mode & ZLIB_FILEFUNC_MODE_CREATE) {
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

} // namespace sphaira::mz
