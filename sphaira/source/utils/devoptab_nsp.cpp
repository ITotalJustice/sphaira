
#include "utils/devoptab.hpp"
#include "utils/devoptab_common.hpp"
#include "defines.hpp"
#include "log.hpp"

#include "yati/container/nsp.hpp"
#include "yati/container/xci.hpp"
#include "yati/source/file.hpp"

#include <cstring>
#include <cerrno>
#include <array>
#include <memory>
#include <algorithm>

namespace sphaira::devoptab {
namespace {

using Collections = yati::container::Collections;

struct File {
    const yati::container::CollectionEntry* collection;
    size_t off;
};

struct Dir {
    u32 index;
};

struct Device final : common::MountDevice {
    Device(std::unique_ptr<common::LruBufferedData>&& _source, const Collections& _collections, const common::MountConfig& _config)
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
    std::unique_ptr<common::LruBufferedData> source;
    const Collections collections;
};

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);

    for (const auto& collection : this->collections) {
        if (path == "/" + collection.name) {
            file->collection = &collection;
            return 0;
        }
    }

    log_write("[NSP] failed to open file %s\n", path);
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
    if (!std::strcmp(path, "/")) {
        return 0;
    }

    log_write("[NSP] failed to open dir %s\n", path);
    return -ENOENT;
}

int Device::devoptab_dirreset(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    dir->index = 0;
    return 0;
}

int Device::devoptab_dirnext(void* fd, char *filename, struct stat *filestat) {
    auto dir = static_cast<Dir*>(fd);

    if (dir->index >= this->collections.size()) {
        return -ENOENT;
    }

    const auto& collection = this->collections[dir->index];
    filestat->st_nlink = 1;
    filestat->st_size = collection.size;
    filestat->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    std::strcpy(filename, collection.name.c_str());

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
        const auto it = std::ranges::find_if(this->collections, [path](auto& e){
            return path == "/" + e.name;
        });

        if (it == this->collections.end()) {
            return -ENOENT;
        }

        st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        st->st_size = it->size;
    }

    return 0;
}

} // namespace

Result MountNsp(fs::Fs* fs, const fs::FsPath& path, fs::FsPath& out_path) {
    auto source = std::make_shared<yati::source::File>(fs, path);

    s64 size;
    R_TRY(source->GetSize(&size));
    auto buffered = std::make_unique<common::LruBufferedData>(source, size);

    yati::container::Nsp nsp{buffered.get()};
    yati::container::Collections collections;
    R_TRY(nsp.GetCollections(collections));

    if (!common::MountReadOnlyIndexDevice(
        [&buffered, &collections](const common::MountConfig& config) {
            return std::make_unique<Device>(std::move(buffered), collections, config);
        },
        sizeof(File), sizeof(Dir),
        "NSP", out_path
    )) {
        log_write("[NSP] Failed to mount %s\n", path.s);
        R_THROW(0x1);
    }

    R_SUCCEED();
}

} // namespace sphaira::devoptab
