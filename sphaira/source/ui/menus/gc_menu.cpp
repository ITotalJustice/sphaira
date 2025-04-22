#include "ui/menus/gc_menu.hpp"
#include "yati/yati.hpp"
#include "app.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "ui/nvg_util.hpp"
#include "i18n.hpp"
#include <cstring>

namespace sphaira::ui::menu::gc {
namespace {

auto InRange(u64 off, u64 offset, u64 size) -> bool {
    return off < offset + size && off >= offset;
}

struct GcSource final : yati::source::Base {
    GcSource(const yati::container::Collections& collections, fs::FsNativeGameCard* fs);
    ~GcSource();
    Result Read(void* buf, s64 off, s64 size, u64* bytes_read);

    const yati::container::Collections& m_collections;
    fs::FsNativeGameCard* m_fs{};
    FsFile m_file{};
    s64 m_offset{};
    s64 m_size{};
};

GcSource::GcSource(const yati::container::Collections& collections, fs::FsNativeGameCard* fs)
: m_collections{collections}
, m_fs{fs} {
    m_offset = -1;
}

GcSource::~GcSource() {
    fsFileClose(&m_file);
}

Result GcSource::Read(void* buf, s64 off, s64 size, u64* bytes_read) {
    // check is we need to open a new file.
    if (!InRange(off, m_offset, m_size)) {
        fsFileClose(&m_file);
        m_file = {};

        // find new file based on the offset.
        bool found = false;
        for (auto& collection : m_collections) {
            if (InRange(off, collection.offset, collection.size)) {
                found = true;
                m_offset = collection.offset;
                m_size = collection.size;
                R_TRY(m_fs->OpenFile(fs::AppendPath("/", collection.name), FsOpenMode_Read, &m_file));
                break;
            }
        }

        // this will never fail, unless i break something in yati.
        R_UNLESS(found, 0x1);
    }

    return fsFileRead(&m_file, off - m_offset, buf, size, 0, bytes_read);
}

} // namespace

Menu::Menu() : MenuBase{"GameCard"_i18n} {
    SetAction(Button::B, Action{"Back"_i18n, [this](){
        SetPop();
    }});

    SetAction(Button::X, Action{"Refresh"_i18n, [this](){
        m_state = State::None;
    }});

    fsOpenDeviceOperator(std::addressof(m_dev_op));
}

Menu::~Menu() {
    // manually close this as it needs(?) to be closed before dev_op.
    m_fs.reset();
    fsDeviceOperatorClose(std::addressof(m_dev_op));
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);

    switch (m_state) {
        case State::None: {
            bool gc_inserted;
            if (R_FAILED(fsDeviceOperatorIsGameCardInserted(std::addressof(m_dev_op), std::addressof(gc_inserted)))) {
                m_state = State::Failed;
            } else {
                if (!gc_inserted) {
                    m_state = State::NotFound;
                } else {
                    if (R_FAILED(ScanGamecard())) {
                        m_state = State::Failed;
                    }
                }
            }
        }   break;

        case State::Progress:
        case State::Done:
        case State::NotFound:
        case State::Failed:
            break;
    }
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    switch (m_state) {
        case State::None:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Waiting for connection..."_i18n.c_str());
            break;

        case State::Progress:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Transferring data..."_i18n.c_str());
            break;

        case State::NotFound:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "No GameCard inserted, press X to refresh"_i18n.c_str());
            break;

        case State::Done:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Installed GameCard, press B to exit..."_i18n.c_str());
            break;

        case State::Failed:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Failed to scan GameCard..."_i18n.c_str());
            break;
    }
}

Result Menu::ScanGamecard() {
    m_state = State::None;
    m_fs.reset();
    m_collections.clear();

    FsGameCardHandle gc_handle;
    R_TRY(fsDeviceOperatorGetGameCardHandle(std::addressof(m_dev_op), std::addressof(gc_handle)));

    m_fs = std::make_unique<fs::FsNativeGameCard>(std::addressof(gc_handle), FsGameCardPartition_Secure, false);
    R_TRY(m_fs->GetFsOpenResult());

    FsDir dir;
    R_TRY(m_fs->OpenDirectory("/", FsDirOpenMode_ReadFiles, std::addressof(dir)));
    ON_SCOPE_EXIT(fsDirClose(std::addressof(dir)));

    s64 count;
    R_TRY(m_fs->DirGetEntryCount(std::addressof(dir), std::addressof(count)));

    std::vector<FsDirectoryEntry> buf(count);
    s64 total_entries;
    R_TRY(m_fs->DirRead(std::addressof(dir), std::addressof(total_entries), buf.size(), buf.data()));
    m_collections.reserve(total_entries);

    s64 offset{};
    for (s64 i = 0; i < total_entries; i++) {
        yati::container::CollectionEntry entry{};
        entry.name = buf[i].name;
        entry.offset = offset;
        entry.size = buf[i].file_size;
        m_collections.emplace_back(entry);
        offset += buf[i].file_size;
    }

    m_state = State::Progress;
    App::Push(std::make_shared<ui::ProgressBox>("Installing App"_i18n, [this](auto pbox) mutable -> bool {
        auto source = std::make_shared<GcSource>(m_collections, m_fs.get());
        return R_SUCCEEDED(yati::InstallFromCollections(pbox, source, m_collections));
    }, [this](bool result){
        if (result) {
            App::Notify("Gc install success!"_i18n);
            m_state = State::Done;
        } else {
            App::Notify("Gc install failed!"_i18n);
            m_state = State::Failed;
        }
    }));

    R_SUCCEED();
}

} // namespace sphaira::ui::menu::gc
