#include "utils/devoptab.hpp"
#include "utils/devoptab_common.hpp"
#include "utils/devoptab_romfs.hpp"

#include "defines.hpp"
#include "log.hpp"
#include "nro.hpp"

#include "yati/source/file.hpp"

#include <cstring>
#include <cerrno>
#include <array>
#include <memory>
#include <algorithm>

namespace sphaira::devoptab {
namespace {

struct AssetCollection {
    u64 offset;
    u64 size;
};

struct NamedCollection {
    std::string name; // exeFS, RomFS, Logo.
    bool is_romfs;
    AssetCollection asset_collection;
    romfs::RomfsCollection romfs_collections;
};

struct FileEntry {
    bool is_romfs;
    romfs::FileEntry romfs;
    const AssetCollection* asset;
    u64 offset;
    u64 size;
};

struct DirEntry {
    bool is_romfs;
    romfs::DirEntry romfs;
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
            out.is_romfs = e.is_romfs;

            const auto rel_name = path.substr(e.name.length() + 1);

            if (out.is_romfs) {
                if (!romfs::find_file(e.romfs_collections, rel_name, out.romfs)) {
                    return false;
                }

                out.offset = out.romfs.offset;
                out.size = out.romfs.size;
                return true;
            } else {
                out.offset = e.asset_collection.offset;
                out.size = e.asset_collection.size;
                return true;
            }
        }
    }

    return false;
}

bool find_dir(std::span<const NamedCollection> named, std::string_view path, DirEntry& out) {
    for (auto& e : named) {
        if (path.starts_with("/" + e.name)) {
            out.is_romfs = e.is_romfs;

            const auto rel_name = path.substr(e.name.length() + 1);

            if (out.is_romfs) {
                return romfs::find_dir(e.romfs_collections, rel_name, out.romfs);
            } else {
                log_write("[NROFS] invalid fs type in find file\n");
                return false;
            }
        }
    }

    return false;
}

void fill_timestamp_from_device(const FsTimeStampRaw& timestamp, struct stat *st) {
    st->st_atime = timestamp.accessed;
    st->st_ctime = timestamp.created;
    st->st_mtime = timestamp.modified;
}

struct Device final : common::MountDevice {
    Device(std::unique_ptr<yati::source::Base>&& _source, const std::vector<NamedCollection>& _collections, const FsTimeStampRaw& _timestamp, const common::MountConfig& _config)
    : MountDevice{_config}
    , source{std::forward<decltype(_source)>(_source)}
    , collections{_collections}
    , timestamp{_timestamp} {

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
    const FsTimeStampRaw timestamp;
};

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);

    FileEntry entry{};
    if (!find_file(this->collections, path, entry)) {
        log_write("[NROFS] failed to find file entry: %s\n", path);
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
    fill_timestamp_from_device(this->timestamp, st);

    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    auto dir = static_cast<Dir*>(fd);

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
        if (entry.is_romfs) {
            romfs::dirreset(entry.romfs);
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

        const auto& e = this->collections[dir->index];
        if (e.is_romfs) {
            filestat->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
        } else {
            filestat->st_size = e.asset_collection.size;
            filestat->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        }

        filestat->st_nlink = 1;
        std::strcpy(filename, e.name.c_str());
    } else {
        if (entry.is_romfs) {
            if (!romfs::dirnext(entry.romfs, filename, filestat)) {
                return -ENOENT;
            }
        }
    }

    fill_timestamp_from_device(this->timestamp, filestat);
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

    fill_timestamp_from_device(this->timestamp, st);
    return 0;
}

} // namespace

Result MountNro(fs::Fs* fs, const fs::FsPath& path, fs::FsPath& out_path) {
    auto source = std::make_unique<yati::source::File>(fs, path);

    NroData data{};
    R_TRY(source->Read2(&data, 0, sizeof(data)));
    R_UNLESS(data.header.magic == NROHEADER_MAGIC, Result_NroBadMagic);

    NroAssetHeader asset{};
    R_TRY(source->Read2(&asset, data.header.size, sizeof(asset)));
    R_UNLESS(asset.magic == NROASSETHEADER_MAGIC, Result_NroBadMagic);

    std::vector<NamedCollection> collections{};

    if (asset.icon.size) {
        NamedCollection collection{"icon.jpg", false, AssetCollection{data.header.size + asset.icon.offset, asset.icon.size}};
        collections.emplace_back(collection);
    }
    if (asset.nacp.size) {
        NamedCollection collection{"control.nacp", false, AssetCollection{data.header.size + asset.nacp.offset, asset.nacp.size}};
        collections.emplace_back(collection);
    }
    if (asset.romfs.size) {
        NamedCollection collection{"RomFS", true};
        if (R_SUCCEEDED(romfs::LoadRomfsCollection(source.get(), data.header.size + asset.romfs.offset, collection.romfs_collections))) {
            collections.emplace_back(collection);
        }
    }

    R_UNLESS(!collections.empty(), 0x9);

    FsTimeStampRaw timestamp{};
    fs->GetFileTimeStampRaw(path, &timestamp);

    if (!common::MountReadOnlyIndexDevice(
        [&source, &collections, &timestamp](const common::MountConfig& config) {
            return std::make_unique<Device>(std::move(source), collections, timestamp, config);
        },
        sizeof(File), sizeof(Dir),
        "NRO", out_path
    )) {
        log_write("[NRO] Failed to mount %s\n", path.s);
        R_THROW(0x1);
    }

    R_SUCCEED();
}

} // namespace sphaira::devoptab
