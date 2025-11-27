#include "yati/container/nsp.hpp"
#include "defines.hpp"
#include "log.hpp"
#include <memory>
#include <cstring>

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

// stdio-like wrapper for std::vector
struct BufHelper {
    BufHelper() = default;
    BufHelper(std::span<const u8> data) {
        write(data);
    }

    void write(const void* data, u64 size) {
        if (offset + size >= buf.size()) {
            buf.resize(offset + size);
        }
        std::memcpy(buf.data() + offset, data, size);
        offset += size;
    }

    void write(std::span<const u8> data) {
        write(data.data(), data.size());
    }

    void seek(u64 where_to) {
        offset = where_to;
    }

    [[nodiscard]]
    auto tell() const {
        return offset;
    }

    std::vector<u8> buf;
    u64 offset{};
};

} // namespace

Result Nsp::GetCollections(Collections& out) {
    return GetCollections(out, 0);
}

Result Nsp::GetCollections(Collections& out, s64 off) {
    u64 bytes_read;

    // get header
    Pfs0Header header{};
    R_TRY(m_source->Read(std::addressof(header), off, sizeof(header), std::addressof(bytes_read)));
    R_UNLESS(header.magic == PFS0_MAGIC, Result_NspBadMagic);
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

auto Nsp::Build(std::span<const CollectionEntry> entries, s64& size) -> std::vector<u8> {
    BufHelper buf;

    Pfs0Header header{};
    std::vector<Pfs0FileTableEntry> file_table(entries.size());
    std::vector<char> string_table;

    u64 string_offset{};
    u64 data_offset{};

    for (u32 i = 0; i < entries.size(); i++) {
        file_table[i].data_offset = data_offset;
        file_table[i].data_size = entries[i].size;
        file_table[i].name_offset = string_offset;
        file_table[i].padding = 0;

        string_table.resize(string_offset + entries[i].name.length() + 1);
        std::memcpy(string_table.data() + string_offset, entries[i].name.c_str(), entries[i].name.length() + 1);

        data_offset += file_table[i].data_size;
        string_offset += entries[i].name.length() + 1;
    }

    // Add padding to the string table so that the header as a whole is well-aligned
    const auto nameless_header_size = sizeof(Pfs0Header) + (file_table.size() * sizeof(Pfs0FileTableEntry));
    auto padded_string_table_size = ((nameless_header_size + string_table.size() + 0x1F) & ~0x1F) - nameless_header_size;

    // Add manual padding if the full Partition FS header would already be properly aligned.
    if (padded_string_table_size == string_table.size()) {
        padded_string_table_size += 0x20;
    }

    string_table.resize(padded_string_table_size);

    header.magic = PFS0_MAGIC;
    header.total_files = entries.size();
    header.string_table_size = string_table.size();
    header.padding = 0;

    buf.write(&header, sizeof(header));
    buf.write(file_table.data(), sizeof(Pfs0FileTableEntry) * file_table.size());
    buf.write(string_table.data(), string_table.size());

    // calculate nsp size.
    size = buf.tell();
    for (const auto& e : file_table) {
        size += e.data_size;
    }

    return buf.buf;
}

} // namespace sphaira::yati::container
