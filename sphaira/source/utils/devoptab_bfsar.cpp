
#include "utils/devoptab.hpp"
#include "utils/devoptab_common.hpp"
#include "defines.hpp"
#include "log.hpp"

#include "yati/source/file.hpp"

#include <pulsar.h>

#include <cstring>
#include <cerrno>
#include <array>
#include <memory>
#include <algorithm>

namespace sphaira::devoptab {
namespace {

struct File {
    PLSR_BFWARFileInfo info;
    size_t off;
};

struct Dir {
    u32 index;
};

PLSR_RC GetFileInfo(const PLSR_BFSAR *bfsar, std::string_view path, PLSR_BFWARFileInfo& out) {
    if (path.starts_with('/')) {
        path = path.substr(1);
    }
    path = path.substr(0, path.find_last_of('.'));
    char buf123[255];
    std::snprintf(buf123, sizeof(buf123), "%.*s", (int)path.length(), path.data());

    PLSR_BFSARStringSearchInfo searchInfo;
    R_TRY(plsrBFSARStringSearch(bfsar, buf123, &searchInfo));

    PLSR_BFSARSoundInfo soundInfo;
	R_TRY(plsrBFSARSoundGet(bfsar, searchInfo.itemId.index, &soundInfo));

    PLSR_BFSARFileInfo soundFileInfo;
    R_TRY(plsrBFSARFileScan(bfsar, soundInfo.fileIndex, &soundFileInfo));
    R_TRY(plsrBFSARFileInfoNormalize(bfsar, &soundFileInfo));

    PLSR_BFWSD bfwsd;
    R_TRY(plsrBFWSDOpenInside(&bfsar->ar, soundFileInfo.internal.offset, &bfwsd));
    ON_SCOPE_EXIT(plsrBFWSDClose(&bfwsd));

    PLSR_BFWSDSoundDataInfo soundDataInfo;
    R_TRY(plsrBFWSDSoundDataGet(&bfwsd, soundInfo.wave.index, &soundDataInfo));

    PLSR_BFWSDNoteInfo noteInfo;
	R_TRY(plsrBFWSDSoundDataNoteGet(&bfwsd, &soundDataInfo.noteInfoTable, 0, &noteInfo));

	PLSR_BFWSDWaveId waveId;
	R_TRY(plsrBFWSDWaveIdListGetEntry(&bfwsd, noteInfo.waveIdIndex, &waveId));

    PLSR_BFSARWaveArchiveInfo waveArchiveInfo;
	R_TRY(plsrBFSARWaveArchiveGet(bfsar, waveId.archiveItemId.index, &waveArchiveInfo));

    PLSR_BFWAR bfwar;
	R_TRY(plsrBFSARWaveArchiveOpen(bfsar, &waveArchiveInfo, &bfwar));
    ON_SCOPE_EXIT(plsrBFWARClose(&bfwar));

	R_TRY(plsrBFWARFileGet(&bfwar, waveId.index, &out));

    // adjust offset.
    out.offset += bfwar.ar.offset;
    R_SUCCEED();

}

struct Device final : common::MountDevice {
    Device(const PLSR_BFSAR& _bfsar, const common::MountConfig& _config)
    : MountDevice{_config}
    , bfsar{_bfsar} {
        this->file = this->bfsar.ar.handle->f;
    }

    ~Device() {
        plsrBFSARClose(&bfsar);
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
    PLSR_BFSAR bfsar;
    std::FILE* file; // points to archive file.
};

int Device::devoptab_open(void *fileStruct, const char *path, int flags, int mode) {
    auto file = static_cast<File*>(fileStruct);

    PLSR_BFWARFileInfo info;
    if (R_FAILED(GetFileInfo(&this->bfsar, path, info))) {
        return -ENOENT;
    }

    file->info = info;
    return 0;
}

int Device::devoptab_close(void *fd) {
    auto file = static_cast<File*>(fd);
    std::memset(file, 0, sizeof(*file));

    return 0;
}

ssize_t Device::devoptab_read(void *fd, char *ptr, size_t len) {
    auto file = static_cast<File*>(fd);
    const auto& info = file->info;

    // plsr seems to read oob, so allow for some tollerance.
    const auto oob_allowed = 64;
    len = std::min(len, info.size + oob_allowed - file->off);
    std::fseek(this->file, file->info.offset + file->off, SEEK_SET);
    const auto bytes_read = std::fread(ptr, 1, len, this->file);

    file->off += bytes_read;
    return bytes_read;
}

ssize_t Device::devoptab_seek(void *fd, off_t pos, int dir) {
    auto file = static_cast<File*>(fd);
    const auto& info = file->info;

    if (dir == SEEK_CUR) {
        pos += file->off;
    } else if (dir == SEEK_END) {
        pos = info.size;
    }

    return file->off = std::clamp<u64>(pos, 0, info.size);
}

int Device::devoptab_fstat(void *fd, struct stat *st) {
    auto file = static_cast<File*>(fd);
    const auto& info = file->info;

    std::memset(st, 0, sizeof(*st));
    st->st_nlink = 1;
    st->st_size = info.size;
    st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    return 0;
}

int Device::devoptab_diropen(void* fd, const char *path) {
    if (!std::strcmp(path, "/")) {
        return 0;
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

    do {
        if (dir->index >= plsrBFSARSoundCount(&this->bfsar)) {
            log_write("finished getting call entries: %u vs %u\n", dir->index, plsrBFSARSoundCount(&this->bfsar));
            return -ENOENT;
        }

        PLSR_BFSARSoundInfo info{};
        if (R_FAILED(plsrBFSARSoundGet(&this->bfsar, dir->index, &info))) {
            continue;
        }

        // todo: skip this entry.
        if (!info.hasStringIndex) {
            continue;
        }

        if (R_FAILED(plsrBFSARStringGet(&this->bfsar, info.stringIndex, filename, NAME_MAX))) {
            continue;
        }

        switch (info.type) {
            case PLSR_BFSARSoundType_Wave:
                std::strcat(filename, ".bfwav");
                break;
            case PLSR_BFSARSoundType_Sequence:
                // std::strcat(filename, ".bfseq");
                continue;
            case PLSR_BFSARSoundType_Stream:
                // std::strcat(filename, ".bfstm");
                continue;
            default:
                continue;
        }

        filestat->st_nlink = 1;
        filestat->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        dir->index++;
        break;
    } while (++dir->index);

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
        PLSR_BFWARFileInfo info{};
        if (R_FAILED(GetFileInfo(&this->bfsar, path, info))) {
            return -ENOENT;
        }

        st->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        st->st_size = info.size;
    }

    return 0;
}

} // namespace

Result MountBfsar(fs::Fs* fs, const fs::FsPath& path, fs::FsPath& out_path) {
    PLSR_BFSAR bfsar{};
    PLSR_RC_TRY(plsrBFSAROpen(path, &bfsar));

    if (!common::MountReadOnlyIndexDevice(
        [&bfsar](const common::MountConfig& config) {
            return std::make_unique<Device>(bfsar, config);
        },
        sizeof(File), sizeof(Dir),
        "BFSAR", out_path
    )) {
        log_write("[BFSAR] Failed to mount %s\n", path.s);
        R_THROW(0x1);
    }

    R_SUCCEED();
}

} // namespace sphaira::devoptab
