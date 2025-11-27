#pragma once

#include <minizip/ioapi.h>
#include <vector>
#include <span>
#include <switch.h>
#include "fs.hpp"

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
void FileFuncNative(zlib_filefunc64_def* funcs);

// minizip takes 18ms to open a zip and 4ms to parse the first file entry.
// this results in a dropped frame.
// this version simply reads the local header + file name in 2 reads,
// which takes 1-2ms.
Result PeekFirstFileName(fs::Fs* fs, const fs::FsPath& path, fs::FsPath& name);

} // namespace sphaira::mz
