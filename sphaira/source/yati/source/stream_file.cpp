#include "yati/source/stream_file.hpp"
#include "log.hpp"

namespace sphaira::yati::source {

StreamFile::StreamFile(FsFileSystem* fs, const fs::FsPath& path) {
    m_open_result = fsFsOpenFile(fs, path, FsOpenMode_Read, std::addressof(m_file));
}

StreamFile::~StreamFile() {
    if (R_SUCCEEDED(GetOpenResult())) {
        fsFileClose(std::addressof(m_file));
    }
}

Result StreamFile::ReadChunk(void* buf, s64 size, u64* bytes_read) {
    R_TRY(GetOpenResult());
    const auto rc = fsFileRead(std::addressof(m_file), m_offset, buf, size, 0, bytes_read);
    m_offset += *bytes_read;
    return rc;
}

} // namespace sphaira::yati::source
