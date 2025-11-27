#include "yati/container/xci.hpp"
#include "defines.hpp"
#include "log.hpp"

#include <cstring>

namespace sphaira::yati::container {
namespace {

#define XCI_MAGIC std::byteswap(0x48454144)
#define HFS0_MAGIC 0x30534648
#define HFS0_ROOT_HEADER_OFFSET 0xF000
#define HFS0_ROOT_HEADER_OFFSET_WITH_KEY_AREA (HFS0_ROOT_HEADER_OFFSET + 0x1000)

} // namespace

auto Xci::Hfs0::GetHfs0Data() const -> std::vector<u8> {
    s64 off = 0;
    std::vector<u8> out(GetHfs0Size());

    std::memcpy(out.data() + off, &header, sizeof(header));
    off += sizeof(header);

    std::memcpy(out.data() + off, file_table.data(), file_table.size() * sizeof(Hfs0FileTableEntry));
    off += file_table.size() * sizeof(Hfs0FileTableEntry);

    for (auto& str : string_table) {
        std::memcpy(out.data() + off, str.data(), str.length());
        off += str.length() + 1;
    }

    return out;
}

Result Xci::GetRoot(Root& out) {
    // try and get root at normal offset.
    s64 offset = HFS0_ROOT_HEADER_OFFSET;
    auto rc = Hfs0GetPartition(m_source, offset, out.hfs0);
    if (rc == Result_XciBadMagic) {
        // otherwise, try and again as maybe the key area pre-prended.
        offset = HFS0_ROOT_HEADER_OFFSET_WITH_KEY_AREA;
        rc = Hfs0GetPartition(m_source, offset, out.hfs0);
    }

    if (R_SUCCEEDED(rc)) {
        out.hfs0_offset = offset;

        for (u32 i = 0; i < out.hfs0.header.total_files; i++) {
            Partition partition{out.hfs0.string_table[i]};
            R_TRY(ReadPartitionFromHfs0(m_source, out.hfs0, i, partition));
            out.partitions.emplace_back(partition);
        }
    }

    return rc;
}

Result Xci::GetCollections(Collections& out) {
    Root root;
    R_TRY(GetRoot(root));
    log_write("[XCI] got root partition\n");

    for (auto& parition : root.partitions) {
        if (parition.name == "secure") {
            out = parition.collections;
            R_SUCCEED();
        }
    }

    return Result_XciSecurePartitionNotFound;
}

Result Xci::Hfs0GetPartition(source::Base* source, s64 off, Hfs0& out) {
    u64 bytes_read;

    // get header
    R_TRY(source->Read(std::addressof(out.header), off, sizeof(out.header), std::addressof(bytes_read)));
    log_write("checking magic: %X vs %X\n", out.header.magic, HFS0_MAGIC);
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

Result Xci::ReadPartitionFromHfs0(source::Base* source, const Hfs0& root, u32 index, Partition& out) {
    log_write("[XCI] fetching %s partition\n", root.string_table[index].c_str());
    out.hfs0_offset = root.data_offset + root.file_table[index].data_offset;
    out.hfs0_size = root.file_table[index].data_size;

    R_TRY(Hfs0GetPartition(source, out.hfs0_offset, out.hfs0));
    log_write("[XCI] got %s partition\n", root.string_table[index].c_str());

    for (u32 i = 0; i < out.hfs0.header.total_files; i++) {
        CollectionEntry entry;
        entry.name = out.hfs0.string_table[i];
        entry.offset = out.hfs0.data_offset + out.hfs0.file_table[i].data_offset;
        entry.size = out.hfs0.file_table[i].data_size;
        out.collections.emplace_back(entry);
    }

    log_write("[XCI] read %s partition count: %zu\n", root.string_table[index].c_str(), out.collections.size());
    R_SUCCEED();
}

} // namespace sphaira::yati::container
