#pragma once

#include "fs.hpp"
#include "location.hpp"
#include "ui/progress_box.hpp"

#include <switch.h>
#include <vector>
#include <memory>
#include <functional>
#include <minizip/ioapi.h>

namespace sphaira::dump {

enum DumpLocationType {
    // dump using native fs.
    DumpLocationType_SdCard,
    // dump to usb pc.
    DumpLocationType_Usb,
    // dump to usb using tinfoil protocol.
    DumpLocationType_UsbS2S,
    // speed test, only reads the data, doesn't write anything.
    DumpLocationType_DevNull,
    // dump to stdio, ideal for custom mount points using devoptab, such as hdd.
    DumpLocationType_Stdio,
};

enum DumpLocationFlag {
    DumpLocationFlag_SdCard = 1 << DumpLocationType_SdCard,
    DumpLocationFlag_Usb = 1 << DumpLocationType_Usb,
    DumpLocationFlag_UsbS2S = 1 << DumpLocationType_UsbS2S,
    DumpLocationFlag_DevNull = 1 << DumpLocationType_DevNull,
    DumpLocationFlag_Stdio = 1 << DumpLocationType_Stdio,
    DumpLocationFlag_All = DumpLocationFlag_SdCard | DumpLocationFlag_Usb | DumpLocationFlag_UsbS2S | DumpLocationFlag_DevNull | DumpLocationFlag_Stdio,
};

struct DumpEntry {
    DumpLocationType type;
    s32 index;
};

struct DumpLocation {
    DumpEntry entry{};
    location::StdioEntries stdio{};
};

struct BaseSource {
    virtual ~BaseSource() = default;
    virtual Result Read(const std::string& path, void* buf, s64 off, s64 size, u64* bytes_read) = 0;
    virtual auto GetName(const std::string& path) const -> std::string = 0;
    virtual auto GetSize(const std::string& path) const -> s64 = 0;
    virtual auto GetIcon(const std::string& path) const -> int { return 0; }

    Result Read(const std::string& path, void* buf, s64 off, s64 size) {
        u64 bytes_read;
        return Read(path, buf, off, size, &bytes_read);
    }
};

struct WriteSource {
    virtual ~WriteSource() = default;
    virtual Result Write(const void* buf, s64 off, s64 size) = 0;
    virtual Result SetSize(s64 size) = 0;
};

// called after dump has finished.
using OnExit = std::function<void(Result rc)>;
using OnLocation = std::function<void(const DumpLocation& loc)>;

using CustomTransfer = std::function<Result(ui::ProgressBox* pbox, BaseSource* source, WriteSource* writer, const fs::FsPath& path)>;

// prompts the user to select dump location, calls on_loc on success with the selected location.
void DumpGetLocation(const std::string& title, u32 location_flags, const OnLocation& on_loc, const CustomTransfer& custom_transfer = nullptr);

Result Dump(ui::ProgressBox* pbox, const std::shared_ptr<BaseSource>& source, const DumpLocation& location, const std::vector<fs::FsPath>& paths, const CustomTransfer& custom_transfer = nullptr);

// dumps to a fetched location using DumpGetLocation().
void Dump(const std::shared_ptr<BaseSource>& source, const DumpLocation& location, const std::vector<fs::FsPath>& paths, const OnExit& on_exit, const CustomTransfer& custom_transfer = nullptr);
// DumpGetLocation() + Dump() all in one.
void Dump(const std::shared_ptr<BaseSource>& source, const std::vector<fs::FsPath>& paths, const OnExit& on_exit = nullptr, u32 location_flags = DumpLocationFlag_All);
void Dump(const std::shared_ptr<BaseSource>& source, const std::vector<fs::FsPath>& paths, const CustomTransfer& custom_transfer, const OnExit& on_exit = nullptr, u32 location_flags = DumpLocationFlag_All);

void FileFuncWriter(WriteSource* writer, zlib_filefunc64_def* funcs);

} // namespace sphaira::dump
