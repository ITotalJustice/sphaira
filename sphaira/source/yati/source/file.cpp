#include "yati/source/file.hpp"

namespace sphaira::yati::source {

File::File(fs::Fs* fs, const fs::FsPath& path) : m_fs{fs} {
    m_open_result = m_fs->OpenFile(path, FsOpenMode_Read, std::addressof(m_file));
}

Result File::Read(void* buf, s64 off, s64 size, u64* bytes_read) {
    R_TRY(GetOpenResult());
    return m_file.Read(off, buf, size, 0, bytes_read);
}

Result File::GetSize(s64* out) {
    return m_file.GetSize(out);
}

} // namespace sphaira::yati::source
