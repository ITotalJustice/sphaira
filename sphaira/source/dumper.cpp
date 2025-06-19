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
#include "usb/tinfoil.hpp"

namespace sphaira::dump {
namespace {

struct DumpLocationEntry {
    const DumpLocationType type;
    const char* name;
};

constexpr DumpLocationEntry DUMP_LOCATIONS[]{
    { DumpLocationType_SdCard, "microSD card (/dumps/)" },
    { DumpLocationType_UsbS2S, "USB transfer (Switch 2 Switch)" },
    { DumpLocationType_DevNull, "/dev/null (Speed Test)" },
};

struct UsbTest final : usb::upload::Usb, yati::source::Stream {
    UsbTest(ui::ProgressBox* pbox, BaseSource* source) : Usb{UINT64_MAX} {
        m_pbox = pbox;
        m_source = source;
    }

    Result ReadChunk(void* buf, s64 size, u64* bytes_read) override {
        R_TRY(m_pull(buf, size, bytes_read));
        m_pull_offset += *bytes_read;
        R_SUCCEED();
    }

    Result Read(const std::string& path, void* buf, s64 off, s64 size, u64* bytes_read) override {
        if (m_pull) {
            return Stream::Read(buf, off, size, bytes_read);
        } else {
            return ReadInternal(path, buf, off, size, bytes_read);
        }
    }

    Result ReadInternal(const std::string& path, void* buf, s64 off, s64 size, u64* bytes_read) {
        if (m_path != path) {
            m_path = path;
            m_progress = 0;
            m_pull_offset = 0;
            Stream::Reset();
            m_size = m_source->GetSize(path);
            m_pbox->SetImage(m_source->GetIcon(path));
            m_pbox->SetTitle(m_source->GetName(path));
            m_pbox->NewTransfer(m_path);
        }

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

private:
    ui::ProgressBox* m_pbox{};
    BaseSource* m_source{};
    std::string m_path{};
    thread::PullCallback m_pull{};
    s64 m_offset{};
    s64 m_size{};
    s64 m_progress{};
    s64 m_pull_offset{};
};

Result DumpToFile(ui::ProgressBox* pbox, fs::Fs* fs, const fs::FsPath& root, BaseSource* source, std::span<const fs::FsPath> paths) {
    constexpr s64 BIG_FILE_SIZE = 1024ULL*1024ULL*1024ULL*4ULL;
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

        const auto flags = file_size >= BIG_FILE_SIZE ? FsCreateOption_BigFile : 0;
        R_TRY(fs->CreateFile(temp_path, file_size, flags));
        ON_SCOPE_EXIT(fs->DeleteFile(temp_path));

        {
            fs::File file;
            R_TRY(fs->OpenFile(temp_path, FsOpenMode_Write, &file));

            R_TRY(thread::Transfer(pbox, file_size,
                [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                    return source->Read(path, data, off, size, bytes_read);
                },
                [&](const void* data, s64 off, s64 size) -> Result {
                    const auto rc = file.Write(off, data, size, FsWriteOption_None);
                    if (is_file_based_emummc) {
                        svcSleepThread(2e+6); // 2ms
                    }
                    return rc;
                }
            ));
        }

        fs->DeleteFile(base_path);
        R_TRY(fs->RenameFile(temp_path, base_path));
    }

    R_SUCCEED();
}

Result DumpToFileNative(ui::ProgressBox* pbox, BaseSource* source, std::span<const fs::FsPath> paths) {
    fs::FsNativeSd fs{};
    return DumpToFile(pbox, &fs, "/", source, paths);
}

Result DumpToStdio(ui::ProgressBox* pbox, const location::StdioEntry& loc, BaseSource* source, std::span<const fs::FsPath> paths) {
    fs::FsStdio fs{};
    return DumpToFile(pbox, &fs, loc.mount, source, paths);
}

Result DumpToUsbS2SStream(ui::ProgressBox* pbox, UsbTest* usb, std::span<const fs::FsPath> paths) {
    auto source = usb->GetSource();

    for (auto& path : paths) {
        const auto file_size = source->GetSize(path);

        R_TRY(thread::TransferPull(pbox, file_size,
            [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                return usb->ReadInternal(path, data, off, size, bytes_read);
            },
            [&](thread::StartThreadCallback start, thread::PullCallback pull) -> Result {
                usb->SetPullCallback(pull);
                R_TRY(start());

                while (!pbox->ShouldExit()) {
                    R_TRY(usb->PollCommands());

                    if (usb->GetPullOffset() >= file_size) {
                        R_SUCCEED();
                    }
                }

                R_THROW(0xFFFF);
            }
        ));
    }

    R_SUCCEED();
}

Result DumpToUsbS2SRandom(ui::ProgressBox* pbox, UsbTest* usb) {
    while (!pbox->ShouldExit()) {
        R_TRY(usb->PollCommands());
    }

    R_THROW(0xFFFF);
}

Result DumpToUsbS2S(ui::ProgressBox* pbox, BaseSource* source, std::span<const fs::FsPath> paths) {
    std::vector<std::string> file_list;
    for (const auto& path : paths) {
        file_list.emplace_back(path);
    }

    // auto usb = std::make_unique<UsbTest>(pbox, entries);
    auto usb = std::make_unique<UsbTest>(pbox, source);
    constexpr u64 timeout = 1e+9;

    while (!pbox->ShouldExit()) {
        if (R_SUCCEEDED(usb->IsUsbConnected(timeout))) {
            pbox->NewTransfer("USB connected, sending file list"_i18n);
            u8 flags = usb::tinfoil::USBFlag_NONE;
            if (App::GetApp()->m_dump_usb_transfer_stream.Get()) {
                flags |= usb::tinfoil::USBFlag_STREAM;
            }

            if (R_SUCCEEDED(usb->WaitForConnection(timeout, flags, file_list))) {
                pbox->NewTransfer("Sent file list, waiting for command..."_i18n);

                Result rc;
                if (flags & usb::tinfoil::USBFlag_STREAM) {
                    rc = DumpToUsbS2SStream(pbox, usb.get(), paths);
                } else {
                    rc = DumpToUsbS2SRandom(pbox, usb.get());
                }

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

    R_THROW(0xFFFF);
}

Result DumpToDevNull(ui::ProgressBox* pbox, BaseSource* source, std::span<const fs::FsPath> paths) {
    for (auto path : paths) {
        R_TRY(pbox->ShouldExitResult());

        const auto file_size = source->GetSize(path);
        pbox->SetImage(source->GetIcon(path));
        pbox->SetTitle(source->GetName(path));
        pbox->NewTransfer(path);

        R_TRY(thread::Transfer(pbox, file_size,
            [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                return source->Read(path, data, off, size, bytes_read);
            },
            [&](const void* data, s64 off, s64 size) -> Result {
                R_SUCCEED();
            }
        ));
    }

    R_SUCCEED();
}

Result DumpToNetwork(ui::ProgressBox* pbox, const location::Entry& loc, BaseSource* source, std::span<const fs::FsPath> paths) {
    for (auto path : paths) {
        R_TRY(pbox->ShouldExitResult());

        const auto file_size = source->GetSize(path);
        pbox->SetImage(source->GetIcon(path));
        pbox->SetTitle(source->GetName(path));
        pbox->NewTransfer(path);

        R_TRY(thread::TransferPull(pbox, file_size,
            [&](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                return source->Read(path, data, off, size, bytes_read);
            },
            [&](thread::PullCallback pull) -> Result {
                s64 offset{};
                const auto result = curl::Api().FromMemory(
                    CURL_LOCATION_TO_API(loc),
                    curl::OnProgress{pbox->OnDownloadProgressCallback()},
                    curl::UploadInfo{
                        path, file_size,
                        [&](void *ptr, size_t size) -> size_t {
                            // curl will request past the size of the file, causing an error.
                            if (offset >= file_size) {
                                log_write("finished file upload\n");
                                return 0;
                            }

                            u64 bytes_read{};
                            if (R_FAILED(pull(ptr, size, &bytes_read))) {
                                log_write("failed to read in custom callback: %zd size: %zd\n", offset, size);
                                return 0;
                            }

                            offset += bytes_read;
                            return bytes_read;
                        }
                    }
                );

                R_UNLESS(result.success, Result_DumpFailedNetworkUpload);
                R_SUCCEED();
            }
        ));
    }

    R_SUCCEED();
}

} // namespace

void DumpGetLocation(const std::string& title, u32 location_flags, OnLocation on_loc) {
    DumpLocation out;
    ui::PopupList::Items items;
    std::vector<DumpEntry> dump_entries;

    out.network = location::Load();
    if (location_flags & (1 << DumpLocationType_Network)) {
        for (s32 i = 0; i < std::size(out.network); i++) {
            dump_entries.emplace_back(DumpLocationType_Network, i);
            items.emplace_back(out.network[i].name);
        }
    }

    out.stdio = location::GetStdio(true);
    if (location_flags & (1 << DumpLocationType_Stdio)) {
        for (s32 i = 0; i < std::size(out.stdio); i++) {
            dump_entries.emplace_back(DumpLocationType_Stdio, i);
            items.emplace_back(out.stdio[i].name);
        }
    }

    for (s32 i = 0; i < std::size(DUMP_LOCATIONS); i++) {
        if (location_flags & (1 << DUMP_LOCATIONS[i].type)) {
            dump_entries.emplace_back(DUMP_LOCATIONS[i].type, i);
            items.emplace_back(i18n::get(DUMP_LOCATIONS[i].name));
        }
    }

    App::Push(std::make_shared<ui::PopupList>(
        title, items, [dump_entries, out, on_loc](auto op_index) mutable {
            out.entry = dump_entries[*op_index];
            on_loc(out);
        }
    ));
}

void Dump(std::shared_ptr<BaseSource> source, const DumpLocation& location, const std::vector<fs::FsPath>& paths, OnExit on_exit) {
    App::Push(std::make_shared<ui::ProgressBox>(0, "Dumping"_i18n, "", [source, paths, location](auto pbox) -> Result {
        if (location.entry.type == DumpLocationType_Network) {
            R_TRY(DumpToNetwork(pbox, location.network[location.entry.index], source.get(), paths));
        } else if (location.entry.type == DumpLocationType_Stdio) {
            R_TRY(DumpToStdio(pbox, location.stdio[location.entry.index], source.get(), paths));
        } else if (location.entry.type == DumpLocationType_SdCard) {
            R_TRY(DumpToFileNative(pbox, source.get(), paths));
        } else if (location.entry.type == DumpLocationType_UsbS2S) {
            R_TRY(DumpToUsbS2S(pbox, source.get(), paths));
        } else if (location.entry.type == DumpLocationType_DevNull) {
            R_TRY(DumpToDevNull(pbox, source.get(), paths));
        }

        R_SUCCEED();
    }, [on_exit](Result rc){
        App::PushErrorBox(rc, "Dump failed!"_i18n);

        if (R_SUCCEEDED(rc)) {
            App::Notify("Dump successfull!"_i18n);
            log_write("dump successfull!!!\n");
        }

        on_exit(rc);
    }));
}

void Dump(std::shared_ptr<BaseSource> source, const std::vector<fs::FsPath>& paths, OnExit on_exit, u32 location_flags) {
    DumpGetLocation("Select dump location"_i18n, location_flags, [source, paths, on_exit](const DumpLocation& loc){
        Dump(source, loc, paths, on_exit);
    });
}

} // namespace sphaira::dump
