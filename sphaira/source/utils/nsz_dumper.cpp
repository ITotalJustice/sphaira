#include "utils/nsz_dumper.hpp"
#include "utils/utils.hpp"

#include "app.hpp"
#include "log.hpp"
#include "fs.hpp"
#include "dumper.hpp"
#include "defines.hpp"
#include "i18n.hpp"
#include "image.hpp"
#include "swkbd.hpp"
#include "threaded_file_transfer.hpp"

#include "yati/nx/ncm.hpp"
#include "yati/nx/nca.hpp"
#include "yati/nx/ncz.hpp"
#include "yati/nx/ns.hpp"
#include "yati/nx/es.hpp"
#include "yati/nx/crypto.hpp"

#include <utility>
#include <cstring>
#include <algorithm>
#include <minIni.h>
#include <zstd.h>

namespace sphaira::utils::nsz {
namespace {

struct NszInfo {
    int threads;
    int level;
    bool ldm;
    bool use_block;
    u8 block_exponent;
};

} // namespace

Result NszExport(ui::ProgressBox* pbox, const NcaReaderCreator& nca_creator, s64& read_offset, s64& write_offset, Collections& collections, const keys::Keys& keys, dump::BaseSource* source, dump::WriteSource* writer, const fs::FsPath& path) {
    const auto threaded_write = [pbox, source, writer, &path](const std::string& name, s64& read_offset, s64& write_offset, s64 size) -> Result {
        if (size > 0) {
            pbox->NewTransfer(name);

            R_TRY(thread::Transfer(pbox, size,
                [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                    return source->Read(path, data, read_offset + off, size, bytes_read);
                },
                [&](const void* data, s64 off, s64 size) -> Result {
                    return writer->Write(data, write_offset + off, size);
                }
            ));

            read_offset += size;
            write_offset += size;
        }

        R_SUCCEED();
    };

    // writes padding between partitions and files.
    const auto write_padding = [&](const std::string& name, s64& read_offset, s64& write_offset, s64 size) -> Result {
        return threaded_write("Writing padding - " + name, read_offset, write_offset, size);
    };

    const auto ldm = App::GetApp()->m_nsz_compress_ldm.Get();
    // don't set this to higher than 3.
    const auto threads = App::GetNszThreadCount();
    // don't set this to higher than 8.
    const auto level = App::GetNszCompressLevel();
    // enable to use block over solid.
    const auto use_block = App::GetApp()->m_nsz_compress_block.Get();
    const auto block_exponent = App::GetNszBlockExponent();

    log_write("[NSZ] start\n");

    auto cctx = ZSTD_createCCtx();
    R_UNLESS(cctx, Result_NszFailedCreateCctx);
    ON_SCOPE_EXIT(ZSTD_freeCCtx(cctx));

    R_UNLESS(!ZSTD_isError(ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, level)), Result_NszFailedSetCompressionLevel);
    R_UNLESS(!ZSTD_isError(ZSTD_CCtx_setParameter(cctx, ZSTD_c_nbWorkers, threads)), Result_NszFailedSetThreadCount);
    R_UNLESS(!ZSTD_isError(ZSTD_CCtx_setParameter(cctx, ZSTD_c_enableLongDistanceMatching, ldm)), Result_NszFailedSetLongDistanceMode);
    std::vector<u8> zstd_out_buf(ZSTD_CStreamOutSize());

    // skip nsp header, this is written later on.
    s64 source_off = read_offset;
    s64 file_off = write_offset;

    for (auto& collection : collections) {
        // there may be paddding between entries, example see minecraft gamecard .cert.
        R_TRY(write_padding(collection.name, source_off, file_off, collection.offset - source_off));

        pbox->NewTransfer(collection.name);
        log_write("processing: %s\n", collection.name.c_str());
        const auto collection_start_off = file_off;

        bool should_compress = false;
        nca::Header header;

        // check if we can compress this nca.
        if (collection.name.ends_with(".nca") && collection.size > NCZ_NORMAL_SIZE) {
            log_write("[NSZ] reading\n");
            R_TRY(source->Read(path, &header, source_off, sizeof(header)));
            log_write("[NSZ] read data\n");
            R_TRY(nca::DecryptHeader(&header, keys, header));
            log_write("[NSZ] done and decrypt\n");

            // nsz only compresses these 2 types.
            // todo: update yati to ensure only these 2 types are compressed, as it currently accepts anything.
            if (header.content_type == nca::ContentType_Program || header.content_type == nca::ContentType_PublicData) {
                should_compress = true;
            }
        }

        if (should_compress) {
            collection.name = collection.name.substr(0, collection.name.find_last_of('.')) + ".ncz";

            // todo: add this to nca.hpp
            keys::KeyEntry title_key;
            R_TRY(nca::GetDecryptedTitleKey(header, keys, title_key));

            auto nca_reader = nca_creator(header, title_key, collection);
            R_UNLESS(nca_reader, 3);

            const ncz::Header ncz_header{NCZ_SECTION_MAGIC, header.GetSectionCount()};
            std::vector<ncz::Section> ncz_sections(ncz_header.total_sections);

            for (u32 i = 0; i < header.GetSectionCount(); i++) {
                const auto& fs_header = header.fs_header[i];
                const auto& fs_table = header.fs_table[i];

                ncz::Section section{};
                section.offset = fs_table.GetOffset();
                section.size = fs_table.GetSize();
                section.crypto_type = fs_header.encryption_type;
                std::memcpy(section.key, &title_key, sizeof(section.key));
                crypto::SetCtr(section.counter, fs_header.section_ctr);
                ncz_sections[i] = section;

                log_write("[%u] got offset: %zu size: %zu\n", i, section.offset, section.size);
            }

            std::ranges::sort(ncz_sections, [](const auto& lhs, const auto& rhs) -> bool {
                return lhs.offset < rhs.offset;
            });

            const u64 blockSize = 1UL << block_exponent;
            const u64 bytesToCompress = collection.size - NCZ_NORMAL_SIZE;
            const u64 blocksToCompress = bytesToCompress / blockSize + (bytesToCompress % blockSize > 0);

            log_write("\n[NCZ] block size: %zu\n", blockSize);
            log_write("[NCZ] bytesToCompress: %zu\n", bytesToCompress);
            log_write("[NCZ] blocksToCompress: %zu\n", blocksToCompress);
            log_write("[NCZ] block mod: %zu\n", bytesToCompress % blockSize);

            ncz::BlockHeader ncz_block_header{};
            if (use_block) {
                ncz_block_header.magic = NCZ_BLOCK_MAGIC;
                ncz_block_header.version = NCZ_BLOCK_VERSION;
                ncz_block_header.type = NCZ_BLOCK_TYPE;
                ncz_block_header.block_size_exponent = block_exponent;
                ncz_block_header.total_blocks = blocksToCompress;
                ncz_block_header.decompressed_size = bytesToCompress;
            }

            std::vector<ncz::Block> ncz_blocks(ncz_block_header.total_blocks);
            u32 ncz_block_index = 0;

            // buffer that zstd compresses into.
            std::vector<u8> ncz_block_out_buffer;
            // buffer that is written into.
            std::vector<u8> ncz_block_in_buffer;
            ncz_block_in_buffer.reserve(blockSize);

            const auto ncz_header_off = file_off + NCZ_NORMAL_SIZE;
            const auto ncz_header_size = sizeof(ncz_header);

            const auto ncz_section_off = ncz_header_off + ncz_header_size;
            const auto ncz_section_size = ncz_sections.size() * sizeof(ncz::Section);

            const auto ncz_block_header_off = ncz_section_off + ncz_section_size;
            const auto ncz_block_header_size = sizeof(ncz_block_header);

            const auto ncz_blocks_off = ncz_block_header_off + ncz_block_header_size;
            const auto ncz_blocks_size = ncz_blocks.size() * sizeof(ncz::Block);

            // read the first 0x4000 encypted, taking into account padding between first section.
            s64 nca_off = 0;
            const auto initial_data_size = std::max<u64>(NCZ_NORMAL_SIZE, ncz_sections[0].offset);

            R_TRY(thread::Transfer(pbox, initial_data_size,
                [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                    R_TRY(nca_reader->ReadEncrypted(data, nca_off, size, bytes_read));
                    nca_off += *bytes_read;
                    R_SUCCEED();
                },
                [&](const void* data, s64 off, s64 size) -> Result {
                    R_TRY(writer->Write(data, file_off, size));
                    file_off += size;
                    R_SUCCEED();
                }
            ));

            // store ncz header + sections.
            R_TRY(writer->Write(&ncz_header, ncz_header_off, ncz_header_size));
            R_TRY(writer->Write(ncz_sections.data(), ncz_section_off, ncz_section_size));
            file_off += ncz_header_size + ncz_section_size;

            // adjust offset for block
            if (use_block) {
                R_TRY(writer->Write(&ncz_block_header, ncz_block_header_off, ncz_block_header_size));
                R_TRY(writer->Write(ncz_blocks.data(), ncz_blocks_off, ncz_blocks_size));
                file_off += ncz_block_header_size + ncz_blocks_size;
            }

            // process nca sections.
            s64 size_remaining = collection.size - initial_data_size;
            while (size_remaining) {
                const auto section = std::ranges::find_if(ncz_sections, [nca_off](auto& e){
                    return e.InRange(nca_off);
                });

                R_UNLESS(section != ncz_sections.cend(), Result_YatiNczSectionNotFound);

                const auto section_number = std::distance(ncz_sections.begin(), section);
                const auto rsize = section->size - (nca_off - section->offset);

                pbox->NewTransfer("Section #"_i18n + std::to_string(section_number) + " - " + collection.name);
                ZSTD_CCtx_reset(cctx, ZSTD_reset_session_only);

                R_TRY(thread::Transfer(pbox, rsize,
                    [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                        R_TRY(nca_reader->Read(data, nca_off, size, bytes_read));
                        nca_off += *bytes_read;
                        R_SUCCEED();
                    },
                    [&](void* _data, s64 off, s64 size, const thread::DecompressWriteCallback& callback) -> Result {
                        auto data = (const u8*)_data;

                        if (use_block) {
                            const auto flush_block = [&]() -> Result {
                                R_UNLESS(ncz_block_index <= ncz_blocks.size(), Result_NszTooManyBlocks);
                                ncz_block_out_buffer.resize(ncz_block_in_buffer.size());
                                const auto result = ZSTD_compress2(cctx, ncz_block_out_buffer.data(), ncz_block_out_buffer.size(), ncz_block_in_buffer.data(), ncz_block_in_buffer.size());

                                // check if we got an error, ignoring if the dst buffer was too small.
                                const auto error_code = ZSTD_getErrorCode(result);
                                R_UNLESS(error_code == ZSTD_error_no_error || error_code == ZSTD_error_dstSize_tooSmall, Result_NszFailedCompress2);

                                // use src buffer instead if zstd failed to compress.
                                auto output = std::span{ncz_block_out_buffer.data(), result};
                                if (error_code == ZSTD_error_dstSize_tooSmall || result >= ncz_block_in_buffer.size()) {
                                    output = ncz_block_in_buffer;
                                }

                                // write block data, advance the block index.
                                R_TRY(callback(output.data(), output.size()));
                                ncz_blocks[ncz_block_index++].size = output.size();

                                ncz_block_in_buffer.resize(0);
                                R_SUCCEED();
                            };

                            const auto last_chunk = off + size >= size_remaining;

                            while (size) {
                                const auto block_off = ncz_block_in_buffer.size();
                                const auto rsize = std::min<s64>(size, blockSize - block_off);

                                ncz_block_in_buffer.resize(block_off + rsize);
                                std::memcpy(ncz_block_in_buffer.data() + block_off, data, rsize);

                                // check if we've filled the block.
                                if (ncz_block_in_buffer.size() == blockSize) {
                                    // log_write("\t\t[NSZ] flushing block\n");
                                    R_TRY(flush_block());
                                }

                                size -= rsize;
                                off += rsize;
                                data += rsize;
                            }

                            // flush last block.
                            if (last_chunk) {
                                if (!ncz_block_in_buffer.empty()) {
                                    log_write("\t\t[NSZ] flushing block end: %zu\n", ncz_block_in_buffer.size());
                                    R_TRY(flush_block());
                                }

                                // ensure that we are at the last block.
                                log_write("block index: %u vs %zu\n", ncz_block_index, ncz_blocks.size());
                                R_UNLESS(ncz_block_index == ncz_blocks.size(), Result_NszMissingBlocks);
                            }
                        } else {
                            ZSTD_inBuffer input = { data, (u64)size, 0 };

                            const auto last_chunk = off + size >= rsize;
                            const auto mode = last_chunk ? ZSTD_e_end : ZSTD_e_continue;

                            int finished;
                            do {
                                ZSTD_outBuffer output = { zstd_out_buf.data(), zstd_out_buf.size(), 0 };
                                const size_t remaining = ZSTD_compressStream2(cctx, &output, &input, mode);

                                if (ZSTD_isError(remaining)) {
                                    log_write("[ZSTD] error: %zu %s\n", remaining, ZSTD_getErrorName(remaining));
                                    R_THROW(Result_NszFailedCompressStream2);
                                }

                                if (output.pos) {
                                    R_TRY(callback(output.dst, output.pos));
                                } else {
                                    log_write("got no output pos so skipping\n");
                                }

                                finished = last_chunk ? (remaining == 0) : (input.pos == input.size);
                            } while (!finished);
                        }

                        R_SUCCEED();
                    },
                    [&](const void* data, s64 off, s64 size) -> Result {
                        R_TRY(writer->Write(data, file_off, size));
                        file_off += size;
                        R_SUCCEED();
                    }
                ));

                size_remaining -= rsize;
            }

            if (use_block) {
                // update blocks with new compressed sizes.
                R_TRY(writer->Write(ncz_blocks.data(), ncz_blocks_off, ncz_blocks_size));
            }

            source_off += collection.size;
        } else {
            R_TRY(threaded_write(collection.name, source_off, file_off, collection.size));
        }

        // update offset and size.
        // collection.offset = collection_start_off - write_offset;
        collection.offset = collection_start_off;
        collection.size = file_off - collection_start_off;
    }

    read_offset = source_off;
    write_offset = file_off;
    R_SUCCEED();
}

} // namespace sphaira::utils::nsz
