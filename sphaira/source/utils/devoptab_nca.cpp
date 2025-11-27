#include "utils/devoptab.hpp"
#include "utils/devoptab_common.hpp"
#include "utils/devoptab_romfs.hpp"
#include "utils/utils.hpp"

#include "defines.hpp"
#include "log.hpp"

#include "yati/nx/es.hpp"
#include "yati/nx/nca.hpp"
#include "yati/nx/ncz.hpp"
#include "yati/nx/ncm.hpp"
#include "yati/nx/keys.hpp"
#include "yati/nx/crypto.hpp"
#include "yati/container/nsp.hpp"
#include "yati/source/file.hpp"

#include <cstring>
#include <cerrno>
#include <array>
#include <memory>
#include <algorithm>

namespace sphaira::devoptab {
namespace {

struct NcaContentTypeFsName {
    const char* name;
    nca::FileSystemType fs_type;
};

constexpr const NcaContentTypeFsName CONTENT_TYPE_FS_NAMES[][NCA_SECTION_TOTAL] = {
    [nca::ContentType_Program] = {
        { "exeFS", nca::FileSystemType_PFS0 },
        { "RomFS", nca::FileSystemType_RomFS },
        { "Logo", nca::FileSystemType_PFS0 },
    },
    [nca::ContentType_Meta] = {
        { "Meta", nca::FileSystemType_PFS0 },
    },
    [nca::ContentType_Control] = {
        { "RomFS", nca::FileSystemType_RomFS },
    },
    [nca::ContentType_Manual] = {
        { "RomFS", nca::FileSystemType_RomFS },
    },
    [nca::ContentType_Data] = {
        { "RomFS", nca::FileSystemType_RomFS }, // verify
    },
    [nca::ContentType_PublicData] = {
        { "RomFS", nca::FileSystemType_RomFS },
    },
};

struct NamedCollection {
    std::string name; // exeFS, RomFS, Logo.
    u8 fs_type; // PFS0 or RomFS.
    yati::container::Collections pfs0_collections;
    romfs::RomfsCollection romfs_collections;
};

struct FileEntry {
    u8 fs_type; // PFS0 or RomFS.
    romfs::FileEntry romfs;
    const yati::container::CollectionEntry* pfs0;
    u64 offset;
    u64 size;
};

struct DirEntry {
    u8 fs_type; // PFS0 or RomFS.
    romfs::DirEntry romfs;
    const yati::container::Collections* pfs0;
};

struct File {
    FileEntry entry;
    size_t off;
};

struct Dir {
    DirEntry entry;
    u32 index;
    bool is_root;
};

bool find_file(std::span<const NamedCollection> named, std::string_view path, FileEntry& out) {
    for (auto& e : named) {
        if (path.starts_with("/" + e.name)) {
            out.fs_type = e.fs_type;

            const auto rel_name = path.substr(e.name.length() + 1);

            if (out.fs_type == nca::FileSystemType_RomFS) {
                if (!romfs::find_file(e.romfs_collections, rel_name, out.romfs)) {
                    return false;
                }

                out.offset = out.romfs.offset;
                out.size = out.romfs.size;
                return true;
            } else if (out.fs_type == nca::FileSystemType_PFS0) {
                for (const auto& collection : e.pfs0_collections) {
                    if (rel_name == "/" + collection.name) {
                        out.pfs0 = &collection;
                        out.offset = out.pfs0->offset;
                        out.size = out.pfs0->size;
                        return true;
                    }
                }

                return false;
            } else {
                log_write("[NCAFS] invalid fs type in find file\n");
                return false;
            }
        }
    }

    return false;
}

bool find_dir(std::span<const NamedCollection> named, std::string_view path, DirEntry& out) {
    for (auto& e : named) {
        if (path.starts_with("/" + e.name)) {
            out.fs_type = e.fs_type;

            const auto rel_name = path.substr(e.name.length() + 1);

            if (out.fs_type == nca::FileSystemType_RomFS) {
                return romfs::find_dir(e.romfs_collections, rel_name, out.romfs);
            } else if (out.fs_type == nca::FileSystemType_PFS0) {
                if (rel_name.length()) {
                    return false;
                }

                out.pfs0 = &e.pfs0_collections;
                return true;
            } else {
                log_write("[NCAFS] invalid fs type in find file\n");
                return false;
            }
        }
    }

    return false;
}

struct Device final : common::MountDevice {
    Device(std::unique_ptr<yati::source::Base>&& _source, const std::vector<NamedCollection>& _collections, const common::MountConfig& _config)
    : MountDevice{_config}
    , source{std::forward<decltype(_source)>(_source)}
    , collections{_collections} {

    }

private:
    bool Mount() override { return true; }
    int devoptab_open(void *fileStruct, const char *path, int flags, int mode) override;
    int devoptab_close(void *fd) override;
    ssize_t devoptab_read(void *fd, char *ptr, size_t len) override;
    ssize_t devoptab_seek(void *fd, off_t pos, int dir) override;
    int devoptab_fstat(void *fd, struct stat *st) override;
    int devoptab_diropen(void* fd, const char *path) override;
    int devoptab_dirreset(void* fd) override;
    int devoptab_dirnext(void* fd, char *filename, struct stat *filestat) override;
    int devoptab_dirclose(void* fd) override;
    int devoptab_lstat(const char *path, struct stat *st) override;

private:
    std::unique_ptr<yati::source::Base> source;
    const std::vector<NamedCollection> collections;
};

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);
    std::memset(file, 0, sizeof(*file));

    FileEntry entry{};
    if (!find_file(this->collections, path, entry)) {
        log_write("[NCAFS] failed to find file entry: %s\n", path);
        return -ENOENT;
    }

    file->entry = entry;
    return 0;
}

int Device::devoptab_close(void *fd) {
    auto file = static_cast<File*>(fd);
    std::memset(file, 0, sizeof(*file));

    return 0;
}

ssize_t Device::devoptab_read(void *fd, char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);
    const auto& entry = file->entry;

    u64 bytes_read;
    len = std::min(len, entry.size - file->off);
    if (R_FAILED(this->source->Read(ptr, entry.offset + file->off, len, &bytes_read))) {
        return -EIO;
    }

    file->off += bytes_read;
    return bytes_read;
}

ssize_t Device::devoptab_seek(void *fd, off_t pos, int dir) {
    auto file = static_cast<File*>(fd);
    const auto& entry = file->entry;

    if (dir == SEEK_CUR) {
        pos += file->off;
    } else if (dir == SEEK_END) {
        pos = entry.size;
    }

    return file->off = std::clamp<u64>(pos, 0, entry.size);
}

int Device::devoptab_fstat(void *fd, struct stat *st) {
    auto file = static_cast<File*>(fd);
    const auto& entry = file->entry;

    st->st_nlink = 1;
    st->st_size = entry.size;
    st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    auto dir = static_cast<Dir*>(fd);
    std::memset(dir, 0, sizeof(*dir));

    if (!std::strcmp(path, "/")) {
        dir->is_root = true;
        return 0;
    } else {
        DirEntry entry{};
        if (!find_dir(this->collections, path, entry)) {
            return -ENOENT;
        }

        dir->entry = entry;
        return 0;
    }
}

int Device::devoptab_dirreset(void* fd) {
    auto dir = static_cast<Dir*>(fd);
    auto& entry = dir->entry;

    if (dir->is_root) {
        dir->index = 0;
    } else {
        if (entry.fs_type == nca::FileSystemType_RomFS) {
            romfs::dirreset(entry.romfs);
        } else {
            dir->index = 0;
        }
    }

    return 0;
}

int Device::devoptab_dirnext(void* fd, char *filename, struct stat *filestat) {
    auto dir = static_cast<Dir*>(fd);
    auto& entry = dir->entry;

    if (dir->is_root) {
        if (dir->index >= this->collections.size()) {
            return -ENOENT;
        }

        filestat->st_nlink = 1;
        filestat->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
        std::strcpy(filename, this->collections[dir->index].name.c_str());
    } else {
        if (entry.fs_type == nca::FileSystemType_RomFS) {
            if (!romfs::dirnext(entry.romfs, filename, filestat)) {
                return -ENOENT;
            }
        } else {
            if (dir->index >= entry.pfs0->size()) {
                return -ENOENT;
            }

            const auto& collection = (*entry.pfs0)[dir->index];
            filestat->st_nlink = 1;
            filestat->st_size = collection.size;
            filestat->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
            std::strcpy(filename, collection.name.c_str());
        }
    }

    dir->index++;
    return 0;
}

int Device::devoptab_dirclose(void* fd) {
    auto dir = static_cast<Dir*>(fd);
    std::memset(dir, 0, sizeof(*dir));

    return 0;
}

int Device::devoptab_lstat(const char *path, struct stat *st) {
    st->st_nlink = 1;

    if (!std::strcmp(path, "/")) {
        st->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
    } else {
        // can be optimised for romfs.
        FileEntry file_entry{};
        DirEntry dir_entry{};
        if (find_file(this->collections, path, file_entry)) {
            st->st_size = file_entry.size;
            st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        } else if (find_dir(this->collections, path, dir_entry)) {
            st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        } else {
            return -ENOENT;
        }
    }

    return 0;
}

Result MountNcaInternal(fs::Fs* fs, const std::shared_ptr<yati::source::Base>& source, s64 size, const fs::FsPath& path, fs::FsPath& out_path) {
    // todo: rather than manually fetching tickets, use spl to
    // decrypt the nca for use (somehow, look how ams does it?).
    keys::Keys keys;
    R_TRY(keys::parse_keys(keys, true));

    nca::Header header{};
    R_TRY(source->Read2(&header, 0, sizeof(header)));
    R_TRY(nca::DecryptHeader(&header, keys, header));

    std::unique_ptr<yati::source::Base> nca_reader{};
    log_write("[NCA] got header, type: %s\n", nca::GetContentTypeStr(header.content_type));

    // check if this is a ncz.
    ncz::Header ncz_header{};
    if (size >= NCZ_NORMAL_SIZE) {
        R_TRY(source->Read2(&ncz_header, NCZ_NORMAL_SIZE, sizeof(ncz_header)));
    }

    if (size >= NCZ_NORMAL_SIZE && ncz_header.magic == NCZ_SECTION_MAGIC) {
        // read all the sections.
        s64 ncz_offset = NCZ_SECTION_OFFSET;
        ncz::Sections ncz_sections(ncz_header.total_sections);
        R_TRY(source->Read2(ncz_sections.data(), ncz_offset, ncz_sections.size() * sizeof(ncz::Section)));

        ncz_offset += ncz_sections.size() * sizeof(ncz::Section);
        ncz::BlockHeader ncz_block_header{};
        R_TRY(source->Read2(&ncz_block_header, ncz_offset, sizeof(ncz_block_header)));

        // ensure this is a block compressed nsz, otherwise bail out
        // because random access is not supported with solid compression.
        R_TRY(ncz_block_header.IsValid());

        ncz_offset += sizeof(ncz_block_header);
        ncz::Blocks ncz_blocks(ncz_block_header.total_blocks);
        R_TRY(source->Read2(ncz_blocks.data(), ncz_offset, ncz_blocks.size() * sizeof(ncz::Block)));

        ncz_offset += ncz_blocks.size() * sizeof(ncz::Block);
        nca_reader = std::make_unique<ncz::NczBlockReader>(
            ncz_header, ncz_sections, ncz_block_header, ncz_blocks, ncz_offset, source
        );
    } else {
        keys::KeyEntry title_key;
        R_TRY(nca::GetDecryptedTitleKey(fs, path, header, keys, title_key));

        // create nca reader which will handle decryption for us.
        // create a LRU buffer cache as the source in order to reduce small reads.
        nca_reader = std::make_unique<nca::NcaReader>(
            header, &title_key, size,
            std::make_shared<common::LruBufferedData>(source, size)
        );
    }

    std::vector<NamedCollection> collections{};
    const auto& content_type_fs = CONTENT_TYPE_FS_NAMES[header.content_type];

    for (u32 i = 0; i < NCA_SECTION_TOTAL; i++) {
        const auto& fs_header = header.fs_header[i];
        const auto& fs_table = header.fs_table[i];

        const auto section_offset = NCA_MEDIA_REAL(fs_table.media_start_offset);
        const auto section_offset_end = NCA_MEDIA_REAL(fs_table.media_end_offset);
        const auto section_size = section_offset_end - section_offset;

        // check if we have hit eof.
        if (fs_header.version != 2 || !section_offset || !section_offset_end) {
            break;
        }

        // ensure the section_offset is valid.
        R_UNLESS(section_offset_end >= section_offset, 0x1);

        if (!content_type_fs[i].name) {
            log_write("[NCA] extra fs section found\n");
            R_THROW(0x1);
        }

        if (content_type_fs[i].fs_type != fs_header.fs_type) {
            log_write("[NCA] fs type missmatch! expected: %u got: %u\n", content_type_fs[i].fs_type, fs_header.fs_type);
            R_THROW(0x1);
        }

        if (fs_header.compression_info.table_offset || fs_header.compression_info.table_size) {
            log_write("[NCA] skipping compressed fs section\n");
            continue;
        }

        if (fs_header.encryption_type == nca::EncryptionType_AesCtrEx || fs_header.encryption_type == nca::EncryptionType_AesCtrExSkipLayerHash) {
            log_write("[NCA] skipping AesCtrEx encryption: %u\n", fs_header.encryption_type);
            continue;
        }

        NamedCollection collection{};
        collection.name = content_type_fs[i].name;
        collection.fs_type = fs_header.fs_type;

        log_write("\t[NCA] section[%u] fs_type: %u\n", i, fs_header.fs_type);
        log_write("\t[NCA] section[%u] encryption_type: %u\n", i, fs_header.encryption_type);
        log_write("\t[NCA] section[%u] section_offset: %zu\n", i, section_offset);
        log_write("\t[NCA] section[%u] size: %zu\n", i, section_size);
        log_write("\n");

        if (fs_header.fs_type == nca::FileSystemType_PFS0) {
            const auto& hash_data = fs_header.hash_data.hierarchical_sha256_data;
            const auto off = section_offset + hash_data.pfs0_layer.offset;
            // const auto size = hash_data.pfs0_layer.size;

            log_write("[NCA] found pfs0, trying\n");
            yati::container::Nsp pfs0(nca_reader.get());

            R_TRY(pfs0.GetCollections(collection.pfs0_collections, off));
        } else if (fs_header.fs_type == nca::FileSystemType_RomFS) {
            const auto& hash_data = fs_header.hash_data.integrity_meta_info;
            R_UNLESS(hash_data.magic == 0x43465649, 0x1);
            R_UNLESS(hash_data.version == 0x20000, 0x2);
            R_UNLESS(hash_data.master_hash_size == SHA256_HASH_SIZE, 0x3);
            R_UNLESS(hash_data.info_level_hash.max_layers == 0x7, 0x4);

            if (fs_header.encryption_type == nca::EncryptionType_AesCtrEx || fs_header.encryption_type == nca::EncryptionType_AesCtrExSkipLayerHash) {
                R_UNLESS(fs_header.patch_info.indirect_header.magic == 0x52544B42, 0x5);
                R_UNLESS(fs_header.patch_info.aes_ctr_header.magic == 0x52544B42, 0x6);
                // todo: bktr
                continue;
            } else {
                auto& romfs = collection.romfs_collections;
                const auto offset = section_offset + hash_data.info_level_hash.levels[5].logical_offset;
                R_TRY(romfs::LoadRomfsCollection(nca_reader.get(), offset, romfs));
            }
        } else {
            log_write("[NCA] unsupported fs type: %u\n", fs_header.fs_type);
            R_THROW(0x1);
        }

        collections.emplace_back(std::move(collection));
    }

    R_UNLESS(!collections.empty(), 0x9);

    if (!common::MountReadOnlyIndexDevice(
        [&nca_reader, &collections](const common::MountConfig& config) {
            return std::make_unique<Device>(std::move(nca_reader), collections, config);
        },
        sizeof(File), sizeof(Dir),
        "NCA", out_path
    )) {
        log_write("[NCA] Failed to mount %s\n", path.s);
        R_THROW(0x1);
    }

    R_SUCCEED();
}

} // namespace

Result MountNca(fs::Fs* fs, const fs::FsPath& path, fs::FsPath& out_path) {
    s64 size;
    auto source = std::make_shared<yati::source::File>(fs, path);
    R_TRY(source->GetSize(&size));

    return MountNcaInternal(fs, source, size, path, out_path);
}

Result MountNcaNcm(NcmContentStorage* cs, const NcmContentId* id, fs::FsPath& out_path) {
    s64 size;
    auto source = std::make_shared<ncm::NcmSource>(cs, id);
    R_TRY(source->GetSize(&size));

    return MountNcaInternal(nullptr, source, size, {}, out_path);
}

} // namespace sphaira::devoptab
