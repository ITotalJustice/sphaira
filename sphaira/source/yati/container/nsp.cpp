#include "yati/container/nsp.hpp"
#include "defines.hpp"
#include "log.hpp"
#include <memory>

namespace sphaira::yati::container {
namespace {

#define PFS0_MAGIC 0x30534650

struct Pfs0Header {
    u32 magic;
    u32 total_files;
    u32 string_table_size;
    u32 padding;
};

struct Pfs0FileTableEntry {
    u64 data_offset;
    u64 data_size;
    u32 name_offset;
    u32 padding;
};

} // namespace

Result Nsp::GetCollections(Collections& out) {
    u64 bytes_read;
    s64 off = 0;

    // get header
    Pfs0Header header{};
    R_TRY(m_source->Read(std::addressof(header), off, sizeof(header), std::addressof(bytes_read)));
    R_UNLESS(header.magic == PFS0_MAGIC, 0x1);
    off += bytes_read;

    // get file table
    std::vector<Pfs0FileTableEntry> file_table(header.total_files);
    R_TRY(m_source->Read(file_table.data(), off, file_table.size() * sizeof(Pfs0FileTableEntry), std::addressof(bytes_read)))
    off += bytes_read;

    // get string table
    std::vector<char> string_table(header.string_table_size);
    R_TRY(m_source->Read(string_table.data(), off, string_table.size(), std::addressof(bytes_read)))
    off += bytes_read;

    out.reserve(header.total_files);
    for (u32 i = 0; i < header.total_files; i++) {
        CollectionEntry entry;
        entry.name = string_table.data() + file_table[i].name_offset;
        entry.offset = off + file_table[i].data_offset;
        entry.size = file_table[i].data_size;
        out.emplace_back(entry);
    }

    R_SUCCEED();
}

} // namespace sphaira::yati::container
