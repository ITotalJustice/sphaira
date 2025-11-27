#pragma once

#include "base.hpp"
#include <vector>
#include <memory>
#include <switch.h>

namespace sphaira::yati::container {

struct Xci final : Base {

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

        auto GetHfs0Size() const {
            return sizeof(header) + file_table.size() * sizeof(Hfs0FileTableEntry) + header.string_table_size;
        }

        auto GetHfs0Data() const -> std::vector<u8>;
    };

    struct Partition {
        // name of the partition.
        std::string name;
        // offset of this hfs0.
        s64 hfs0_offset;
        s64 hfs0_size;
        Hfs0 hfs0;
        // all the collections for this partition, may be empty.
        Collections collections;
    };

    struct Root {
        // offset of this hfs0.
        s64 hfs0_offset;
        Hfs0 hfs0;
        std::vector<Partition> partitions;
    };

    using Partitions = std::vector<Partition>;

    using Base::Base;
    Result GetCollections(Collections& out) override;
    Result GetRoot(Root& out);

private:
    Result Hfs0GetPartition(source::Base* source, s64 off, Hfs0& out);
    Result ReadPartitionFromHfs0(source::Base* source, const Hfs0& root, u32 index, Partition& out);
};

} // namespace sphaira::yati::container
