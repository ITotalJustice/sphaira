
#include "utils/devoptab.hpp"
#include "utils/devoptab_common.hpp"
#include "defines.hpp"
#include "log.hpp"

#include "yati/container/xci.hpp"
#include "yati/source/file.hpp"

#include <cstring>
#include <cerrno>
#include <array>
#include <memory>
#include <algorithm>

namespace sphaira::devoptab {
namespace {

struct File {
    const yati::container::CollectionEntry* collection;
    size_t off;
};

struct Dir {
    const yati::container::Collections* collections;
    u32 index;
};

struct Device final : common::MountDevice {
    Device(std::unique_ptr<common::LruBufferedData>&& _source, const yati::container::Xci::Partitions& _partitions, const common::MountConfig& _config)
    : MountDevice{_config}
    , source{std::forward<decltype(_source)>(_source)}
    , partitions{_partitions} {

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
    std::unique_ptr<common::LruBufferedData> source;
    const yati::container::Xci::Partitions partitions;
};

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);

    for (const auto& partition : this->partitions) {
        for (const auto& collection : partition.collections) {
            if (path == "/" + partition.name + "/" + collection.name) {
                file->collection = &collection;
                return 0;
            }
        }
    }

    log_write("[XCI] devoptab_open: failed to find path: %s\n", path);
    return -ENOENT;
}

int Device::devoptab_close(void *fd) {
    auto file = static_cast<File*>(fd);
    std::memset(file, 0, sizeof(*file));

    return 0;
}

ssize_t Device::devoptab_read(void *fd, char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);

    const auto& collection = file->collection;
    len = std::min(len, collection->size - file->off);

    u64 bytes_read;
    if (R_FAILED(this->source->Read(ptr, collection->offset + file->off, len, &bytes_read))) {
        return -EIO;
    }

    file->off += bytes_read;
    return bytes_read;
}

ssize_t Device::devoptab_seek(void *fd, off_t pos, int dir) {
    auto file = static_cast<File*>(fd);
    const auto& collection = file->collection;

    if (dir == SEEK_CUR) {
        pos += file->off;
    } else if (dir == SEEK_END) {
        pos = collection->size;
    }

    return file->off = std::clamp<u64>(pos, 0, collection->size);
}

int Device::devoptab_fstat(void *fd, struct stat *st) {
    auto file = static_cast<File*>(fd);
    const auto& collection = file->collection;

    st->st_nlink = 1;
    st->st_size = collection->size;
    st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    auto dir = static_cast<Dir*>(fd);

    if (!std::strcmp(path, "/")) {
        return 0;
    } else {
        for (const auto& partition : this->partitions) {
            if (path == "/" + partition.name) {
                dir->collections = &partition.collections;
                return 0;
            }
        }
    }

    return -ENOENT;
}

int Device::devoptab_dirreset(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    dir->index = 0;

    return 0;
}

int Device::devoptab_dirnext(void* fd, char *filename, struct stat *filestat) {
    auto dir = static_cast<Dir*>(fd);

    if (!dir->collections) {
        if (dir->index >= this->partitions.size()) {
            return -ENOENT;
        }

        filestat->st_nlink = 1;
        filestat->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
        std::strcpy(filename, this->partitions[dir->index].name.c_str());
    } else {
        if (dir->index >= dir->collections->size()) {
            return -ENOENT;
        }

        const auto& collection = (*dir->collections)[dir->index];
        filestat->st_nlink = 1;
        filestat->st_size = collection.size;
        filestat->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        std::strcpy(filename, collection.name.c_str());
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
        for (const auto& partition : this->partitions) {
            if (path == "/" + partition.name) {
                st->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
                return 0;
            }

            for (const auto& collection : partition.collections) {
                if (path == "/" + partition.name + "/" + collection.name) {
                    st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
                    st->st_size = collection.size;
                    return 0;
                }
            }
        }
    }

    return -ENOENT;
}

Result MountXciInternal(const std::shared_ptr<yati::source::Base>& source, s64 size, const fs::FsPath& path, fs::FsPath& out_path) {
    auto buffered = std::make_unique<common::LruBufferedData>(source, size);
    yati::container::Xci xci{buffered.get()};
    yati::container::Xci::Root root;
    R_TRY(xci.GetRoot(root));

    if (!common::MountReadOnlyIndexDevice(
        [&buffered, &root](const common::MountConfig& config) {
            return std::make_unique<Device>(std::move(buffered), root.partitions, config);
        },
        sizeof(File), sizeof(Dir),
        "XCI", out_path
    )) {
        log_write("[XCI] Failed to mount %s\n", path.s);
        R_THROW(0x1);
    }

    R_SUCCEED();
}

} // namespace

Result MountXci(fs::Fs* fs, const fs::FsPath& path, fs::FsPath& out_path) {
    s64 size;
    auto source = std::make_shared<yati::source::File>(fs, path);
    R_TRY(source->GetSize(&size));

    return MountXciInternal(source, size, path, out_path);
}

Result MountXciSource(const std::shared_ptr<sphaira::yati::source::Base>& source, s64 size, const fs::FsPath& path, fs::FsPath& out_path) {
    return MountXciInternal(source, size, path, out_path);
}

} // namespace sphaira::devoptab
