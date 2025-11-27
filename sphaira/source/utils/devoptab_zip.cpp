#include "utils/devoptab.hpp"
#include "utils/devoptab_common.hpp"
#include "defines.hpp"
#include "log.hpp"

#include "yati/source/file.hpp"

#include <cstring>
#include <cerrno>
#include <array>
#include <memory>
#include <algorithm>
#include <zlib.h>

namespace sphaira::devoptab {
namespace {

#define LOCAL_HEADER_SIG 0x4034B50
#define FILE_HEADER_SIG 0x2014B50
#define DATA_DESCRIPTOR_SIG 0x8074B50
#define END_RECORD_SIG 0x6054B50

enum mmz_Flag {
    mmz_Flag_Encrypted = 1 << 0,
    mmz_Flag_DataDescriptor = 1 << 3,
    mmz_Flag_StrongEncrypted = 1 << 6,
};

enum mmz_Compression {
    mmz_Compression_None = 0,
    mmz_Compression_Deflate = 8,
};

// 30 bytes (0x1E)
#pragma pack(push,1)
typedef struct mmz_LocalHeader {
    uint32_t sig;
    uint16_t version;
    uint16_t flags;
    uint16_t compression;
    uint16_t modtime;
    uint16_t moddate;
    uint32_t crc32;
    uint32_t compressed_size;
    uint32_t uncompressed_size;
    uint16_t filename_len;
    uint16_t extrafield_len;
} mmz_LocalHeader;
#pragma pack(pop)

#pragma pack(push,1)
typedef struct mmz_DataDescriptor {
    uint32_t sig;
    uint32_t crc32;
    uint32_t compressed_size;
    uint32_t uncompressed_size;
} mmz_DataDescriptor;
#pragma pack(pop)

// 46 bytes (0x2E)
#pragma pack(push,1)
typedef struct mmz_FileHeader {
    uint32_t sig;
    uint16_t version;
    uint16_t version_needed;
    uint16_t flags;
    uint16_t compression;
    uint16_t modtime;
    uint16_t moddate;
    uint32_t crc32;
    uint32_t compressed_size;
    uint32_t uncompressed_size;
    uint16_t filename_len;
    uint16_t extrafield_len;
    uint16_t filecomment_len;
    uint16_t disk_start; // wat
    uint16_t internal_attr; // wat
    uint32_t external_attr; // wat
    uint32_t local_hdr_off;
} mmz_FileHeader;
#pragma pack(pop)

#pragma pack(push,1)
typedef struct mmz_EndRecord {
    uint32_t sig;
    uint16_t disk_number;
    uint16_t disk_wcd;
    uint16_t disk_entries;
    uint16_t total_entries;
    uint32_t central_directory_size;
    uint32_t file_hdr_off;
    uint16_t comment_len;
} mmz_EndRecord;
#pragma pack(pop)

struct FileEntry {
    std::string path;
    u16 flags;
    u16 compression_type;
    u16 modtime;
    u16 moddate;
    u32 compressed_size; // may be zero.
    u32 uncompressed_size; // may be zero.
    u32 local_file_header_off;
};

struct DirectoryEntry {
    std::string path;
    std::vector<DirectoryEntry> dir_child;
    std::vector<FileEntry> file_child;
};

using FileTableEntries = std::vector<FileEntry>;

struct Zfile {
    z_stream z; // zlib stream.
    Bytef* buffer; // buffer that compressed data is read into.
    size_t buffer_size; // size of the above buffer.
    size_t compressed_off; // offset of the compressed file.
};

struct File {
    const FileEntry* entry;
    Zfile zfile; // only used if the file is compressed.
    size_t data_off; // offset of the file data.
    size_t off;
};

struct Dir {
    const DirectoryEntry* entry;
    u32 index;
};

const FileEntry* find_file_entry(const DirectoryEntry& dir, std::string_view path) {
    if (path.starts_with(dir.path)) {
        // todo: check if / comes after file name in order to only check dirs.
        for (const auto& e : dir.file_child) {
            if (e.path == path) {
                return &e;
            }
        }

        for (const auto& e : dir.dir_child) {
            if (auto entry = find_file_entry(e, path)) {
                return entry;
            }
        }
    }

    return nullptr;
}

const DirectoryEntry* find_dir_entry(const DirectoryEntry& dir, std::string_view path) {
    if (dir.path == path) {
        return &dir;
    }

    if (path.starts_with(dir.path)) {
        for (const auto& e : dir.dir_child) {
            if (auto entry = find_dir_entry(e, path)) {
                return entry;
            }
        }
    }

    return nullptr;
}

void set_stat_file(const FileEntry* entry, struct stat *st) {
    std::memset(st, 0, sizeof(*st));

    st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    st->st_size = entry->uncompressed_size;
    st->st_nlink = 1;

    struct tm tm{};
    tm.tm_sec = (entry->modtime & 0x1F) << 1;
    tm.tm_min = (entry->modtime >> 5) & 0x3F;
    tm.tm_hour = (entry->modtime >> 11);
    tm.tm_mday = (entry->moddate & 0x1F);
    tm.tm_mon = ((entry->moddate >> 5) & 0xF) - 1;
    tm.tm_year = (entry->moddate >> 9) + 80;

    st->st_atime = mktime(&tm);
    st->st_mtime = st->st_atime;
    st->st_ctime = st->st_atime;
}

struct Device final : common::MountDevice {
    Device(std::unique_ptr<common::LruBufferedData>&& _source, const DirectoryEntry& _root, const common::MountConfig& _config)
    : MountDevice{_config}
    , source{std::forward<decltype(_source)>(_source)}
    , root{_root} {

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
    const DirectoryEntry root;
};

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);

    const auto entry = find_file_entry(this->root, path);
    if (!entry) {
        return -ENOENT;
    }

    if ((entry->flags & mmz_Flag_Encrypted) || (entry->flags & mmz_Flag_StrongEncrypted)) {
        log_write("[ZIP] encrypted zip not supported\n");
        return -ENOENT;
    }

    if (entry->compression_type != mmz_Compression_None && entry->compression_type != mmz_Compression_Deflate) {
        log_write("[ZIP] unsuported compression type: %u\n", entry->compression_type);
        return -ENOENT;
    }

    mmz_LocalHeader local_hdr{};
    auto offset = entry->local_file_header_off;
    if (R_FAILED(this->source->Read2(&local_hdr, offset, sizeof(local_hdr)))) {
        return -ENOENT;
    }

    if (local_hdr.sig != LOCAL_HEADER_SIG) {
        return -ENOENT;
    }

    offset += sizeof(local_hdr) + local_hdr.filename_len + local_hdr.extrafield_len;

    // todo: does a decs take prio over file header?
    if (local_hdr.flags & mmz_Flag_DataDescriptor) {
        mmz_DataDescriptor data_desc{};
        if (R_FAILED(this->source->Read2(&data_desc, offset, sizeof(data_desc)))) {
            return -ENOENT;
        }

        if (data_desc.sig != DATA_DESCRIPTOR_SIG) {
            return -ENOENT;
        }

        offset += sizeof(data_desc);
    }

    if (entry->compression_type == mmz_Compression_Deflate) {
        auto& zfile = file->zfile;
        zfile.buffer_size = 1024 * 64;
        zfile.buffer = (Bytef*)std::calloc(1, zfile.buffer_size);
        if (!zfile.buffer) {
            return -ENOENT;
        }

        // skip zlib header.
        if (Z_OK != inflateInit2(&zfile.z, -MAX_WBITS)) {
            std::free(zfile.buffer);
            zfile.buffer = nullptr;
            return -ENOENT;
        }
    }

    file->entry = entry;
    file->data_off = offset;
    return 0;
}

int Device::devoptab_close(void *fd) {
    auto file = static_cast<File*>(fd);

     if (file->entry->compression_type == mmz_Compression_Deflate) {
        inflateEnd(&file->zfile.z);

        if (file->zfile.buffer) {
            std::free(file->zfile.buffer);
        }
    }

    return 0;
}

ssize_t Device::devoptab_read(void *fd, char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);
    len = std::min(len, file->entry->uncompressed_size - file->off);

    if (!len) {
        return 0;
    }

    if (file->entry->compression_type == mmz_Compression_None) {
        if (R_FAILED(this->source->Read2(ptr, file->data_off + file->off, len))) {
            return -ENOENT;
        }
    } else if (file->entry->compression_type == mmz_Compression_Deflate) {
        auto& zfile = file->zfile;
        zfile.z.next_out = (Bytef*)ptr;
        zfile.z.avail_out = len;

        // run until we have inflated enough data.
        while (zfile.z.avail_out) {
            // check if we need to fetch more data.
            if (!zfile.z.next_in || !zfile.z.avail_in) {
                const auto clen = std::min(zfile.buffer_size, file->entry->compressed_size - zfile.compressed_off);
                if (R_FAILED(this->source->Read2(zfile.buffer, file->data_off + zfile.compressed_off, clen))) {
                    return -ENOENT;
                }

                zfile.compressed_off += clen;
                zfile.z.next_in = zfile.buffer;
                zfile.z.avail_in = clen;
            }

            const auto rc = inflate(&zfile.z, Z_SYNC_FLUSH);
            if (Z_OK != rc) {
                if (Z_STREAM_END == rc) {
                    len -= zfile.z.avail_out;
                } else {
                    log_write("[ZLIB] failed to inflate: %d %s\n", rc, zfile.z.msg);
                    return -ENOENT;
                }
            }
        }
    }

    file->off += len;
    return len;
}

ssize_t Device::devoptab_seek(void *fd, off_t pos, int dir) {
    auto file = static_cast<File*>(fd);

    // seek like normal.
    if (file->entry->compression_type == mmz_Compression_None) {
        if (dir == SEEK_CUR) {
            pos += file->off;
        } else if (dir == SEEK_END) {
            pos = file->entry->uncompressed_size;
        }
    } else if (file->entry->compression_type == mmz_Compression_Deflate) {
        // limited seek options.
        // todo: support seeking to end and then back to orig position.
        if (dir == SEEK_SET) {
            if (pos == 0) {
                inflateReset(&file->zfile.z);
            } else if (pos != file->off) {
                // seeking to the end is fine as it may be used to calc size.
                if (pos != file->entry->uncompressed_size) {
                    // random access seek is not supported.
                    pos = file->off;
                }
            }
        } else if (dir == SEEK_CUR) {
            // random access seek is not supported.
            pos = file->off;
        } else if (dir == SEEK_END) {
            pos = file->entry->uncompressed_size;
        }
    }

    return file->off = std::clamp<u64>(pos, 0, file->entry->uncompressed_size);
}

int Device::devoptab_fstat(void *fd, struct stat *st) {
    auto file = static_cast<File*>(fd);

    set_stat_file(file->entry, st);
    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    auto dir = static_cast<Dir*>(fd);

    const auto entry = find_dir_entry(this->root, path);
    if (!entry) {
        return -ENOENT;
    }

    dir->entry = entry;
    return 0;
}

int Device::devoptab_dirreset(void* fd) {
    auto dir = static_cast<Dir*>(fd);

    dir->index = 0;

    return 0;
}

int Device::devoptab_dirnext(void* fd, char *filename, struct stat *filestat) {
    auto dir = static_cast<Dir*>(fd);

    u32 index = dir->index;
    if (index >= dir->entry->dir_child.size()) {
        index -= dir->entry->dir_child.size();
        if (index >= dir->entry->file_child.size()) {
            return -ENOENT;
        } else {
            const auto& entry = dir->entry->file_child[index];
            const auto rel_path = entry.path.substr(entry.path.find_last_of('/') + 1);

            set_stat_file(&entry, filestat);
            std::strcpy(filename, rel_path.c_str());
        }

    } else {
        const auto& entry = dir->entry->dir_child[index];
        const auto rel_path = entry.path.substr(entry.path.find_last_of('/') + 1);

        filestat->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
        std::strcpy(filename, rel_path.c_str());
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

    if (find_dir_entry(this->root, path)) {
        st->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
    } else if (auto entry = find_file_entry(this->root, path)) {
        set_stat_file(entry, st);
    } else {
        log_write("[ZIP] didn't find in lstat\n");
        return -ENOENT;
    }

    return 0;
}

auto BuildPath(const std::string& path) -> std::string {
    if (path.starts_with('/')) {
        return path;
    }
    return "/" + path;
}

void Parse(const FileTableEntries& entries, u32& index, DirectoryEntry& out) {
    for (; index < entries.size(); index++) {
        const auto path = BuildPath(entries[index].path);

        // check if this path belongs to this dir.
        if (!path.starts_with(out.path)) {
            return;
        }

        if (path.ends_with('/')) {
            auto& new_entry = out.dir_child.emplace_back();
            new_entry.path = path.substr(0, path.length() - 1);

            u32 new_index = index + 1;
            Parse(entries, new_index, new_entry);
            index = new_index - 1;
        } else {
            // check if this file actually belongs to this folder.
            const auto idx = path.find_first_of('/', out.path.length() + 1);
            const auto sub = path.substr(0, idx);

            if (idx != path.npos && out.path != sub) {
                auto& new_entry = out.dir_child.emplace_back();
                new_entry.path = sub;
                Parse(entries, index, new_entry);
            } else {
                auto& new_entry = out.file_child.emplace_back(entries[index]);
                new_entry.path = path;
            }
        }
    }
}

void Parse(const FileTableEntries& entries, DirectoryEntry& out) {
    u32 index = 0;
    out.path = "/"; // add root folder.
    Parse(entries, index, out);
}

Result find_central_dir_offset(common::LruBufferedData* source, s64 size, mmz_EndRecord* record) {
    // check if the record is at the end (no extra header).
    auto offset = size - sizeof(*record);
    R_TRY(source->Read2(record, offset, sizeof(*record)));

    if (record->sig == END_RECORD_SIG) {
        R_SUCCEED();
    }

    // failed, find the sig by reading the last 64k and loop across it.
    const auto rsize = std::min<u64>(UINT16_MAX, size);
    offset = size - rsize;
    std::vector<u8> data(rsize);
    R_TRY(source->Read2(data.data(), offset, data.size()));

    // check in reverse order as it's more likely at the end.
    for (s64 i = data.size() - sizeof(*record); i >= 0; i--) {
        u32 sig;
        std::memcpy(&sig, data.data() + i, sizeof(sig));
        if (sig == END_RECORD_SIG) {
            std::memcpy(record, data.data() + i, sizeof(*record));
            R_SUCCEED();
        }
    }

    R_THROW(0x1);
}

Result ParseZip(common::LruBufferedData* source, s64 size, FileTableEntries& out) {
    mmz_EndRecord end_rec;
    R_TRY(find_central_dir_offset(source, size, &end_rec));

    out.reserve(end_rec.total_entries);
    auto file_header_off = end_rec.file_hdr_off;

    for (u16 i = 0; i < end_rec.total_entries; i++) {
        // read the file header.
        mmz_FileHeader file_hdr{};
        R_TRY(source->Read2(&file_hdr, file_header_off, sizeof(file_hdr)));

        if (file_hdr.sig != FILE_HEADER_SIG) {
            log_write("[ZIP] invalid file record\n");
            R_THROW(0x1);
        }

        // save all the data hat we care about.
        auto& new_entry = out.emplace_back();
        new_entry.flags = file_hdr.flags;
        new_entry.compression_type = file_hdr.compression;
        new_entry.modtime = file_hdr.modtime;
        new_entry.moddate = file_hdr.moddate;
        new_entry.compressed_size = file_hdr.compressed_size;
        new_entry.uncompressed_size = file_hdr.uncompressed_size;
        new_entry.local_file_header_off = file_hdr.local_hdr_off;

        // read the file name.
        const auto filename_off = file_header_off + sizeof(file_hdr);
        new_entry.path.resize(file_hdr.filename_len);
        R_TRY(source->Read2(new_entry.path.data(), filename_off, new_entry.path.size()));

        // advance the offset.
        file_header_off += sizeof(file_hdr) + file_hdr.filename_len + file_hdr.extrafield_len + file_hdr.filecomment_len;
    }

    R_SUCCEED();
}

} // namespace

Result MountZip(fs::Fs* fs, const fs::FsPath& path, fs::FsPath& out_path) {
    auto source = std::make_shared<yati::source::File>(fs, path);

    s64 size;
    R_TRY(source->GetSize(&size));
    auto buffered = std::make_unique<common::LruBufferedData>(source, size);

    FileTableEntries table_entries;
    R_TRY(ParseZip(buffered.get(), size, table_entries));
    log_write("[ZIP] parsed zip\n");

    DirectoryEntry root;
    Parse(table_entries, root);

    if (!common::MountReadOnlyIndexDevice(
        [&buffered, &root](const common::MountConfig& config) {
            return std::make_unique<Device>(std::move(buffered), root, config);
        },
        sizeof(File), sizeof(Dir),
        "ZIP", out_path
    )) {
        log_write("[ZIP] Failed to mount %s\n", path.s);
        R_THROW(0x1);
    }

    R_SUCCEED();
}

} // namespace sphaira::devoptab
