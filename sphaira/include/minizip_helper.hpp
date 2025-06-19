#pragma once

#include <minizip/ioapi.h>
#include <vector>
#include <span>
#include <switch.h>

namespace sphaira::mz {

struct MzMem {
    std::vector<u8> buf;
    size_t offset;
};

struct MzSpan {
    std::span<const u8> buf;
    size_t offset;
};

void FileFuncMem(MzMem* mem, zlib_filefunc64_def* funcs);
void FileFuncSpan(MzSpan* span, zlib_filefunc64_def* funcs);
void FileFuncStdio(zlib_filefunc64_def* funcs);

} // namespace sphaira::mz
