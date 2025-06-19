#include "yati/container/xci.hpp"
#include "defines.hpp"
#include "log.hpp"

namespace sphaira::yati::container {
namespace {

#define XCI_MAGIC std::byteswap(0x48454144)
#define HFS0_MAGIC 0x30534648
#define HFS0_HEADER_OFFSET 0xF000

struct Hfs0Header {
    u32 magic;
    u32 total_files;
    u32 string_table_size;
    u32 padding;
};

struct Hfs0FileTableEntry {
    u64 data_offset;
    u64 data_size;
    u32 name_offset;
    u32 hash_size;
    u64 padding;
    u8 hash[0x20];
};

struct Hfs0 {
    Hfs0Header header{};
    std::vector<Hfs0FileTableEntry> file_table{};
    std::vector<std::string> string_table{};
    s64 data_offset{};
};

Result Hfs0GetPartition(source::Base* source, s64 off, Hfs0& out) {
    u64 bytes_read;

    // get header
    R_TRY(source->Read(std::addressof(out.header), off, sizeof(out.header), std::addressof(bytes_read)));
    R_UNLESS(out.header.magic == HFS0_MAGIC, Result_XciBadMagic);
    off += bytes_read;

    // get file table
    out.file_table.resize(out.header.total_files);
    R_TRY(source->Read(out.file_table.data(), off, out.file_table.size() * sizeof(Hfs0FileTableEntry), std::addressof(bytes_read)))
    off += bytes_read;

    // get string table
    std::vector<char> string_table(out.header.string_table_size);
    R_TRY(source->Read(string_table.data(), off, string_table.size(), std::addressof(bytes_read)))
    off += bytes_read;

    for (u32 i = 0; i < out.header.total_files; i++) {
        out.string_table.emplace_back(string_table.data() + out.file_table[i].name_offset);
    }

    out.data_offset = off;
    R_SUCCEED();
}

} // namespace

Result Xci::GetCollections(Collections& out) {
    Hfs0 root{};
    R_TRY(Hfs0GetPartition(m_source.get(), HFS0_HEADER_OFFSET, root));

    for (u32 i = 0; i < root.header.total_files; i++) {
        if (root.string_table[i] == "secure") {
            Hfs0 secure{};
            R_TRY(Hfs0GetPartition(m_source.get(), root.data_offset + root.file_table[i].data_offset, secure));

            for (u32 i = 0; i < secure.header.total_files; i++) {
                CollectionEntry entry;
                entry.name = secure.string_table[i];
                entry.offset = secure.data_offset + secure.file_table[i].data_offset;
                entry.size = secure.file_table[i].data_size;
                out.emplace_back(entry);
            }

            R_SUCCEED();
        }
    }

    return 0x1;
}

} // namespace sphaira::yati::container
