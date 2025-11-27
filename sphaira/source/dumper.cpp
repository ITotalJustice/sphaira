#include "dumper.hpp"
#include "app.hpp"
#include "log.hpp"
#include "fs.hpp"
#include "download.hpp"
#include "defines.hpp"
#include "i18n.hpp"
#include "location.hpp"
#include "threaded_file_transfer.hpp"

#include "ui/sidebar.hpp"
#include "ui/error_box.hpp"
#include "ui/option_box.hpp"
#include "ui/progress_box.hpp"
#include "ui/popup_list.hpp"
#include "ui/nvg_util.hpp"

#include "yati/source/stream.hpp"

#include "usb/usb_uploader.hpp"
#include "usb/usb_dumper.hpp"
#include "usb/usbds.hpp"

namespace sphaira::dump {
namespace {

struct ZipInternal {
    WriteSource* writer;
    s64 offset;
    s64 size;
    Result rc;
};

voidpf zopen64_file(voidpf opaque, const void* filename, int mode)
{
    ZipInternal* fs = (ZipInternal*)calloc(1, sizeof(*fs));
    fs->writer = (WriteSource*)opaque;
    return fs;
}

ZPOS64_T ztell64_file(voidpf opaque, voidpf stream)
{
    auto fs = (ZipInternal*)stream;
    return fs->offset;
}

long zseek64_file(voidpf opaque, voidpf stream, ZPOS64_T offset, int origin)
{
    auto fs = (ZipInternal*)stream;
    switch (origin) {
        case SEEK_SET: {
            fs->offset = offset;
        } break;
        case SEEK_CUR: {
            fs->offset += offset;
        } break;
        case SEEK_END: {
            fs->offset = fs->size + offset;
        } break;
    }
    return 0;
}

uLong zwrite_file(voidpf opaque, voidpf stream, const void* buf, uLong size) {
    auto fs = (ZipInternal*)stream;

    if (R_FAILED(fs->rc = fs->writer->Write(buf, fs->offset, size))) {
        return 0;
    }

    fs->offset += size;
    fs->size = std::max(fs->size, fs->offset);
    return size;
}

int zclose_file(voidpf opaque, voidpf stream) {
    if (stream) {
        auto fs = (ZipInternal*)stream;
        std::memset(fs, 0, sizeof(*fs));
        std::free(fs);
    }
    return 0;
}

int zerror_file(voidpf opaque, voidpf stream) {
    auto fs = (ZipInternal*)stream;
    if (R_FAILED(fs->rc)) {
        return -1;
    }
    return 0;
}

constexpr zlib_filefunc64_def zlib_filefunc = {
    .zopen64_file = zopen64_file,
    .zwrite_file = zwrite_file,
    .ztell64_file = ztell64_file,
    .zseek64_file = zseek64_file,
    .zclose_file = zclose_file,
    .zerror_file = zerror_file,
};

struct DumpLocationEntry {
    const DumpLocationType type;
    const char* name;
};

struct WriteFileSource final : WriteSource {
    WriteFileSource(fs::File* file) : m_file{file} {
    }

    Result Write(const void* buf, s64 off, s64 size) override {
        return m_file->Write(off, buf, size, FsWriteOption_None);
    }

    Result SetSize(s64 size) override {
        return m_file->SetSize(size);
    }

private:
    fs::File* m_file;
};

struct WriteNullSource final : WriteSource {
    Result Write(const void* buf, s64 off, s64 size) override {
        R_SUCCEED();
    }
    Result SetSize(s64 size) override {
        R_SUCCEED();
    }
};

struct WriteUsbSource final : WriteSource {
    WriteUsbSource(u64 transfer_timeout) {
        // disable mtp if enabled.
        m_was_mtp_enabled = App::GetMtpEnable();
        if (m_was_mtp_enabled) {
            App::SetMtpEnable(false);
        }

        m_usb = std::make_unique<usb::dump::Usb>(transfer_timeout);
    }

    ~WriteUsbSource() {
        m_usb.reset();

        if (m_was_mtp_enabled) {
            App::SetMtpEnable(true);
        }
    }

    Result WaitForConnection(std::string_view path, u64 timeout) {
        return m_usb->WaitForConnection(path, timeout);
    }

    Result CloseFile() {
        return m_usb->CloseFile();
    }

    Result Write(const void* buf, s64 off, s64 size) override {
        return m_usb->Write(buf, off, size);
    }

    Result SetSize(s64 size) override {
        R_SUCCEED();
    }

    auto GetOpenResult() const {
        return m_usb->GetOpenResult();
    }

    auto GetCancelEvent() {
        return m_usb->GetCancelEvent();
    }

private:
    std::unique_ptr<usb::dump::Usb> m_usb{};
    bool m_was_mtp_enabled{};
};

constexpr DumpLocationEntry DUMP_LOCATIONS[]{
    { DumpLocationType_SdCard, "microSD card (/dumps/)" },
    { DumpLocationType_Usb, "USB export to PC (usb_export.py)" },
    { DumpLocationType_UsbS2S, "USB transfer (Switch 2 Switch)" },
    { DumpLocationType_DevNull, "/dev/null (Speed Test)" },
};

struct UsbTest final : usb::upload::Usb, yati::source::Stream {
    UsbTest(ui::ProgressBox* pbox, BaseSource* source, std::span<const fs::FsPath> paths, u64 timeout)
    : Usb{timeout}
    , m_pbox{pbox}
    , m_source{source}
    , m_paths{paths} {

    }

    auto& GetPath(u32 index) const {
        return m_paths[index];
    }

    auto& GetPath() const {
        return GetPath(m_current_file_index);
    }

    Result ReadChunk(void* buf, s64 size, u64* bytes_read) override {
        R_TRY(m_pull(buf, size, bytes_read));
        m_pull_offset += *bytes_read;
        R_SUCCEED();
    }

    Result Read(void* buf, u64 off, u32 size, u64* bytes_read) override {
        if (m_pull) {
            return Stream::Read(buf, off, size, bytes_read);
        } else {
            return ReadInternal(GetPath(), buf, off, size, bytes_read);
        }
    }

    Result Open(u32 index, s64& out_size, u16& out_flags) override {
        const auto path = m_paths[index];
        const auto size = m_source->GetSize(path);

        m_progress = 0;
        m_pull_offset = 0;
        Stream::Reset();
        m_size = size;
        m_pbox->SetImage(m_source->GetIcon(path));
        m_pbox->SetTitle(m_source->GetName(path));
        m_pbox->NewTransfer(path);

        m_current_file_index = index;
        out_size = size;
        out_flags = usb::api::FLAG_STREAM;
        R_SUCCEED();
    }

    Result ReadInternal(const std::string& path, void* buf, s64 off, s64 size, u64* bytes_read) {
        R_TRY(m_source->Read(path, buf, off, size, bytes_read));

        m_offset += *bytes_read;
        m_progress += *bytes_read;
        m_pbox->UpdateTransfer(m_progress, m_size);

        R_SUCCEED();
    }

    void SetPullCallback(thread::PullCallback pull) {
        m_pull = pull;
    }

    auto* GetSource() {
        return m_source;
    }

    auto GetPullOffset() const {
        return m_pull_offset;
    }

    auto GetOpenResult() const {
        return Usb::GetOpenResult();
    }

private:
    ui::ProgressBox* m_pbox{};
    BaseSource* m_source{};
    std::span<const fs::FsPath> m_paths{};
    thread::PullCallback m_pull{};
    s64 m_offset{};
    s64 m_size{};
    s64 m_progress{};
    s64 m_pull_offset{};
    u32 m_current_file_index{};
};

Result DumpToUsb(ui::ProgressBox* pbox, BaseSource* source, std::span<const fs::FsPath> paths, const CustomTransfer& custom_transfer) {
    // create write source and verify that it opened.
    constexpr u64 timeout = UINT64_MAX;
    auto write_source = std::make_unique<WriteUsbSource>(timeout);
    R_TRY(write_source->GetOpenResult());

    // add cancel event.
    pbox->AddCancelEvent(write_source->GetCancelEvent());
    ON_SCOPE_EXIT(pbox->RemoveCancelEvent(write_source->GetCancelEvent()));

    for (const auto& path : paths) {
        const auto file_size = source->GetSize(path);
        pbox->SetImage(source->GetIcon(path));
        pbox->SetTitle(source->GetName(path));
        pbox->NewTransfer("Waiting for USB connection..."_i18n);

        // wait until usb is ready.
        while (true) {
            R_TRY(pbox->ShouldExitResult());

            const auto rc = write_source->WaitForConnection(path, timeout);
            if (R_SUCCEEDED(rc)) {
                break;
            }
        }

        pbox->NewTransfer(path);
        ON_SCOPE_EXIT(write_source->CloseFile());

        if (custom_transfer) {
            R_TRY(custom_transfer(pbox, source, write_source.get(), path));
        } else {
            R_TRY(thread::Transfer(pbox, file_size,
                [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                    return source->Read(path, data, off, size, bytes_read);
                },
                [&](const void* data, s64 off, s64 size) -> Result {
                    return write_source->Write(data, off, size);
                }
            ));
        }
    }

    R_SUCCEED();
}

Result DumpToFile(ui::ProgressBox* pbox, fs::Fs* fs, const fs::FsPath& root, BaseSource* source, std::span<const fs::FsPath> paths, const CustomTransfer& custom_transfer) {
    const auto is_file_based_emummc = App::IsFileBaseEmummc();

    for (const auto& path : paths) {
        const auto base_path = fs::AppendPath(root, path);
        const auto file_size = source->GetSize(path);
        pbox->SetImage(source->GetIcon(path));
        pbox->SetTitle(source->GetName(path));
        pbox->NewTransfer(base_path);

        const auto temp_path = base_path + ".temp";
        fs->CreateDirectoryRecursivelyWithPath(temp_path);
        fs->DeleteFile(temp_path);

        R_TRY(fs->CreateFile(temp_path, file_size));
        ON_SCOPE_EXIT(fs->DeleteFile(temp_path));

        {
            fs::File file;
            R_TRY(fs->OpenFile(temp_path, FsOpenMode_Write|FsOpenMode_Append, &file));
            auto write_source = std::make_unique<WriteFileSource>(&file);

            if (custom_transfer) {
                R_TRY(custom_transfer(pbox, source, write_source.get(), path));
            } else {
                R_TRY(thread::Transfer(pbox, file_size,
                    [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                        return source->Read(path, data, off, size, bytes_read);
                    },
                    [&](const void* data, s64 off, s64 size) -> Result {
                        const auto rc = write_source->Write(data, off, size);
                        if (is_file_based_emummc) {
                            svcSleepThread(2e+6); // 2ms
                        }
                        return rc;
                    }
                ));
            }
        }

        fs->DeleteFile(base_path);
        R_TRY(fs->RenameFile(temp_path, base_path));
    }

    R_SUCCEED();
}

Result DumpToFileNative(ui::ProgressBox* pbox, BaseSource* source, std::span<const fs::FsPath> paths, const CustomTransfer& custom_transfer) {
    fs::FsNativeSd fs{};
    return DumpToFile(pbox, &fs, "/", source, paths, custom_transfer);
}

Result DumpToStdio(ui::ProgressBox* pbox, const location::StdioEntry& loc, BaseSource* source, std::span<const fs::FsPath> paths, const CustomTransfer& custom_transfer) {
    fs::FsStdio fs{};
    const auto mount_path = fs::AppendPath(loc.mount, loc.dump_path);
    return DumpToFile(pbox, &fs, mount_path, source, paths, custom_transfer);
}

Result DumpToUsbS2SInternal(ui::ProgressBox* pbox, UsbTest* usb) {
    auto source = usb->GetSource();

    while (!pbox->ShouldExit()) {
        R_TRY(usb->PollCommands());

        const auto path = usb->GetPath();
        const auto file_size = source->GetSize(path);

        R_TRY(thread::TransferPull(pbox, file_size,
            [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                return usb->ReadInternal(path, data, off, size, bytes_read);
            },
            [&](const thread::StartThreadCallback& start, const thread::PullCallback& pull) -> Result {
                usb->SetPullCallback(pull);
                R_TRY(start());

                while (!pbox->ShouldExit()) {
                    const auto rc = usb->file_transfer_loop();
                    if (R_FAILED(rc)) {
                        if (rc == Result_UsbUploadExit) {
                            break;
                        } else {
                            R_THROW(rc);
                        }
                    }
                }

                return pbox->ShouldExitResult();
            }
        ));
    }

    return pbox->ShouldExitResult();
}

Result DumpToUsbS2S(ui::ProgressBox* pbox, BaseSource* source, std::span<const fs::FsPath> paths) {
    std::vector<std::string> file_list;
    for (const auto& path : paths) {
        file_list.emplace_back(path);
    }

    // create usb test instance and verify that it opened.
    constexpr u64 timeout = UINT64_MAX;
    auto usb = std::make_unique<UsbTest>(pbox, source, paths, timeout);
    R_TRY(usb->GetOpenResult());

    // add cancel event.
    pbox->AddCancelEvent(usb->GetCancelEvent());
    ON_SCOPE_EXIT(pbox->RemoveCancelEvent(usb->GetCancelEvent()));

    while (!pbox->ShouldExit()) {
        if (R_SUCCEEDED(usb->IsUsbConnected(timeout))) {
            pbox->NewTransfer("USB connected, sending file list"_i18n);
            if (R_SUCCEEDED(usb->WaitForConnection(timeout, file_list))) {
                pbox->NewTransfer("Sent file list, waiting for command..."_i18n);

                Result rc = DumpToUsbS2SInternal(pbox, usb.get());

                // wait for exit command.
                if (R_SUCCEEDED(rc)) {
                    log_write("waiting for exit command\n");
                    rc = usb->PollCommands();
                    log_write("finished polling for exit command\n");
                } else {
                    log_write("skipped polling for exit command\n");
                }

                if (rc == Result_UsbUploadExit) {
                    log_write("got exit command\n");
                    R_SUCCEED();
                }

                return rc;
            }
        } else {
            pbox->NewTransfer("waiting for usb connection..."_i18n);
        }
    }

    return pbox->ShouldExitResult();
}

Result DumpToDevNull(ui::ProgressBox* pbox, BaseSource* source, std::span<const fs::FsPath> paths, const CustomTransfer& custom_transfer) {
    for (auto path : paths) {
        R_TRY(pbox->ShouldExitResult());

        const auto file_size = source->GetSize(path);
        pbox->SetImage(source->GetIcon(path));
        pbox->SetTitle(source->GetName(path));
        pbox->NewTransfer(path);

        auto write_source = std::make_unique<WriteNullSource>();

        if (custom_transfer) {
            R_TRY(custom_transfer(pbox, source, write_source.get(), path));
        } else {
            R_TRY(thread::Transfer(pbox, file_size,
                [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                    return source->Read(path, data, off, size, bytes_read);
                },
                [&](const void* data, s64 off, s64 size) -> Result {
                    return write_source->Write(data, off, size);
                }
            ));
        }
    }

    R_SUCCEED();
}

} // namespace

void DumpGetLocation(const std::string& title, u32 location_flags, const OnLocation& on_loc, const CustomTransfer& custom_transfer) {
    DumpLocation out;
    ui::PopupList::Items items;
    std::vector<DumpEntry> dump_entries;

    const auto stdio_entries = location::GetStdio(true);
    if (location_flags & (1 << DumpLocationType_Stdio)) {
        for (auto& e : stdio_entries) {
            if (e.dump_hidden) {
                continue;
            }

            const auto index = out.stdio.size();
            dump_entries.emplace_back(DumpLocationType_Stdio, index);
            items.emplace_back(e.name);
            out.stdio.emplace_back(e);
        }
    }

    for (s32 i = 0; i < std::size(DUMP_LOCATIONS); i++) {
        if (location_flags & (1 << DUMP_LOCATIONS[i].type)) {
            log_write("[dump] got name: %s\n", DUMP_LOCATIONS[i].name);
            if (!custom_transfer || DUMP_LOCATIONS[i].type != DumpLocationType_UsbS2S) {
                log_write("[dump] got name 2: %s\n", DUMP_LOCATIONS[i].name);
                dump_entries.emplace_back(DUMP_LOCATIONS[i].type, i);
                items.emplace_back(i18n::get(DUMP_LOCATIONS[i].name));
            }
        }
    }

    App::Push<ui::PopupList>(
        title, items, [dump_entries, out, on_loc](auto op_index) mutable {
            out.entry = dump_entries[*op_index];
            log_write("got entry: %u index: %zu\n", out.entry.type, *op_index);
            on_loc(out);
        }
    );
}

Result Dump(ui::ProgressBox* pbox, const std::shared_ptr<BaseSource>& source, const DumpLocation& location, const std::vector<fs::FsPath>& paths, const CustomTransfer& custom_transfer) {
    if (location.entry.type == DumpLocationType_Stdio) {
        R_TRY(DumpToStdio(pbox, location.stdio[location.entry.index], source.get(), paths, custom_transfer));
    } else if (location.entry.type == DumpLocationType_SdCard) {
        R_TRY(DumpToFileNative(pbox, source.get(), paths, custom_transfer));
    } else if (location.entry.type == DumpLocationType_Usb) {
        R_TRY(DumpToUsb(pbox, source.get(), paths, custom_transfer));
    } else if (location.entry.type == DumpLocationType_UsbS2S) {
        R_TRY(DumpToUsbS2S(pbox, source.get(), paths));
    } else if (location.entry.type == DumpLocationType_DevNull) {
        R_TRY(DumpToDevNull(pbox, source.get(), paths, custom_transfer));
    }

    R_SUCCEED();
}

void Dump(const std::shared_ptr<BaseSource>& source, const DumpLocation& location, const std::vector<fs::FsPath>& paths, const OnExit& on_exit, const CustomTransfer& custom_transfer) {
    App::Push<ui::ProgressBox>(0, "Exporting"_i18n, "", [source, paths, location, custom_transfer](auto pbox) -> Result {
        return Dump(pbox, source, location, paths, custom_transfer);
    }, [on_exit](Result rc){
        App::PushErrorBox(rc, "Export failed!"_i18n);

        if (R_SUCCEEDED(rc)) {
            App::Notify("Export successfull!"_i18n);
            log_write("dump successfull!!!\n");
        }

        if (on_exit) {
            on_exit(rc);
        }
    });
}

void Dump(const std::shared_ptr<BaseSource>& source, const std::vector<fs::FsPath>& paths, const OnExit& on_exit, u32 location_flags) {
    DumpGetLocation("Select export location"_i18n, location_flags, [source, paths, on_exit](const DumpLocation& loc) {
        Dump(source, loc, paths, on_exit);
    });
}

void Dump(const std::shared_ptr<BaseSource>& source, const std::vector<fs::FsPath>& paths, const CustomTransfer& custom_transfer, const OnExit& on_exit, u32 location_flags) {
    DumpGetLocation("Select export location"_i18n, location_flags, [source, paths, on_exit, custom_transfer](const DumpLocation& loc) {
        Dump(source, loc, paths, on_exit, custom_transfer);
    }, custom_transfer);
}

void FileFuncWriter(WriteSource* writer, zlib_filefunc64_def* funcs) {
    *funcs = zlib_filefunc;
    funcs->opaque = writer;
}

} // namespace sphaira::dump
