// this is used for testing that streams work, this code isn't used in normal
// release builds as it is slower / less feature complete than normal.
#pragma once

#include "stream.hpp"
#include "fs.hpp"
#include <switch.h>

namespace sphaira::yati::source {

struct StreamFile final : Stream {
    StreamFile(FsFileSystem* fs, const fs::FsPath& path);
    ~StreamFile();

    Result ReadChunk(void* buf, s64 size, u64* bytes_read) override;

private:
    FsFile m_file{};
    s64 m_offset{};
};

} // namespace sphaira::yati::source
