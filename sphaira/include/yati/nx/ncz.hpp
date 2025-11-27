#pragma once

#include "yati/source/base.hpp"
#include "utils/lru.hpp"
#include "defines.hpp"

#include <switch.h>
#include <vector>
#include <memory>
#include <zstd.h>

namespace sphaira::ncz {

#define NCZ_SECTION_MAGIC 0x4E544345535A434EUL
// todo: byteswap this
#define NCZ_BLOCK_MAGIC std::byteswap(0x4E435A424C4F434BUL)

#define NCZ_BLOCK_VERSION (2)
#define NCZ_BLOCK_TYPE (1)

#define NCZ_NORMAL_SIZE    (0x4000)
#define NCZ_SECTION_OFFSET (NCZ_NORMAL_SIZE + sizeof(ncz::Header))

struct Header {
    u64 magic; // NCZ_SECTION_MAGIC
    u64 total_sections;
};

struct BlockHeader {
    u64 magic; // NCZ_BLOCK_MAGIC
    u8 version;
    u8 type;
    u8 padding;
    u8 block_size_exponent;
    u32 total_blocks;
    u64 decompressed_size;

    Result IsValid() const {
        R_UNLESS(magic == NCZ_BLOCK_MAGIC, 9);
        R_UNLESS(version == NCZ_BLOCK_VERSION, Result_YatiInvalidNczBlockVersion);
        R_UNLESS(type == NCZ_BLOCK_TYPE, Result_YatiInvalidNczBlockType);
        R_UNLESS(total_blocks, Result_YatiInvalidNczBlockTotal);
        R_UNLESS(block_size_exponent >= 14 && block_size_exponent <= 32, Result_YatiInvalidNczBlockSizeExponent);
        R_SUCCEED();
    }
};

struct Block {
    u32 size;
};
using Blocks = std::vector<Block>;

struct BlockInfo {
    u64 offset; // compressed offset.
    u64 size; // compressed size.

    auto InRange(u64 off) const -> bool {
        return off < offset + size && off >= offset;
    }
};

struct Section {
    u64 offset;
    u64 size;
    u64 crypto_type;
    u64 padding;
    u8 key[0x10];
    u8 counter[0x10];

    auto InRange(u64 off) const -> bool {
        return off < offset + size && off >= offset;
    }
};
using Sections = std::vector<Section>;

struct NczBlockReader final : yati::source::Base {
    explicit NczBlockReader(const Header& header, const Sections& sections, const BlockHeader& block_header, const Blocks& blocks, u64 offset, const std::shared_ptr<yati::source::Base>& source);
    Result Read(void *_buf, s64 off, s64 size, u64* bytes_read) override;

private:
    struct LruData {
        s64 offset{};
        std::vector<u8> data{};

        auto InRange(u64 off) const -> bool {
            return off < offset + data.size() && off >= offset;
        }
    };

private:
    Result ReadInternal(void *_buf, s64 off, s64 size, u64* bytes_read, bool decrypt);

private:
    const Header m_header;
    const Sections m_sections;
    const BlockHeader m_block_header;
    const Blocks m_blocks;
    const u64 m_block_offset;
    std::shared_ptr<yati::source::Base> m_source;

    u32 m_block_size{};
    std::vector<BlockInfo> m_block_infos{};

    // lru cache of blocks
    std::vector<LruData> m_lru_data{};
    utils::Lru<LruData> m_lru{};
};

} // namespace sphaira::ncz
