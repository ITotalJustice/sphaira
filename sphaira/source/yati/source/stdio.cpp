#include "yati/source/stdio.hpp"

namespace sphaira::yati::source {

Stdio::Stdio(const fs::FsPath& path) {
    m_file = std::fopen(path, "rb");
    if (!m_file) {
        m_open_result = fsdevGetLastResult();
    }
}

Stdio::~Stdio() {
    if (R_SUCCEEDED(GetOpenResult())) {
        std::fclose(m_file);
    }
}

Result Stdio::Read(void* buf, s64 off, s64 size, u64* bytes_read) {
    R_TRY(GetOpenResult());

    std::fseek(m_file, off, SEEK_SET);
    R_TRY(fsdevGetLastResult());

    *bytes_read = std::fread(buf, 1, size, m_file);
    return fsdevGetLastResult();
}

} // namespace sphaira::yati::source
