#include "app.hpp"
#include "log.hpp"
#include "fs.hpp"
#include "dumper.hpp"
#include "defines.hpp"
#include "i18n.hpp"
#include "image.hpp"
#include "swkbd.hpp"

#include "utils/utils.hpp"
#include "utils/nsz_dumper.hpp"

#include "ui/menus/game_menu.hpp"
#include "ui/menus/game_meta_menu.hpp"
#include "ui/menus/save_menu.hpp"
#include "ui/menus/gc_menu.hpp" // remove when gc event pr is merged.
#include "ui/sidebar.hpp"
#include "ui/error_box.hpp"
#include "ui/option_box.hpp"
#include "ui/progress_box.hpp"
#include "ui/popup_list.hpp"
#include "ui/nvg_util.hpp"

#include "yati/nx/ncm.hpp"
#include "yati/nx/nca.hpp"
#include "yati/nx/ns.hpp"
#include "yati/nx/es.hpp"
#include "yati/container/base.hpp"
#include "yati/container/nsp.hpp"

#include <utility>
#include <cstring>
#include <algorithm>
#include <minIni.h>

namespace sphaira::ui::menu::game {
namespace {

std::atomic_bool g_change_signalled{};

struct NspSource final : dump::BaseSource {
    NspSource(const std::vector<NspEntry>& entries) : m_entries{entries} {
        m_is_file_based_emummc = App::IsFileBaseEmummc();
    }

    Result Read(const std::string& path, void* buf, s64 off, s64 size, u64* bytes_read) override {
        const auto it = std::ranges::find_if(m_entries, [&path](auto& e){
            return path.find(e.path.s) != path.npos;
        });
        R_UNLESS(it != m_entries.end(), Result_GameBadReadForDump);

        const auto rc = it->Read(buf, off, size, bytes_read);
        if (m_is_file_based_emummc) {
            svcSleepThread(2e+6); // 2ms
        }
        return rc;
    }

    Result Read(const std::string& path, void* buf, s64 off, s64 size) {
        u64 bytes_read = 0;
        return Read(path, buf, off, size, &bytes_read);
    }

    auto GetName(const std::string& path) const -> std::string {
        const auto it = std::ranges::find_if(m_entries, [&path](auto& e){
            return path.find(e.path.s) != path.npos;
        });

        if (it != m_entries.end()) {
            return it->application_name;
        }

        return {};
    }

    auto GetSize(const std::string& path) const -> s64 {
        const auto it = std::ranges::find_if(m_entries, [&path](auto& e){
            return path.find(e.path.s) != path.npos;
        });

        if (it != m_entries.end()) {
            return it->nsp_size;
        }

        return 0;
    }

    auto GetIcon(const std::string& path) const -> int override {
        const auto it = std::ranges::find_if(m_entries, [&path](auto& e){
            return path.find(e.path.s) != path.npos;
        });

        if (it != m_entries.end()) {
            return it->icon;
        }

        return App::GetDefaultImage();
    }

    Result GetEntryFromPath(const std::string& path, NspEntry& out) const {
        const auto it = std::ranges::find_if(m_entries, [&path](auto& e){
            return path.find(e.path.s) != path.npos;
        });
        R_UNLESS(it != m_entries.end(), Result_GameBadReadForDump);

        out = *it;
        R_SUCCEED();
    }

private:
    std::vector<NspEntry> m_entries{};
    bool m_is_file_based_emummc{};
};

#ifdef ENABLE_NSZ
Result NszExport(ProgressBox* pbox, const keys::Keys& keys, dump::BaseSource* _source, dump::WriteSource* writer, const fs::FsPath& path) {
    auto source = (NspSource*)_source;

    NspEntry entry;
    R_TRY(source->GetEntryFromPath(path, entry));

    const auto nca_creator = [&entry](const nca::Header& header, const keys::KeyEntry& title_key, const utils::nsz::Collection& collection) {
        const auto content_id = ncm::GetContentIdFromStr(collection.name.c_str());
        return std::make_unique<nca::NcaReader>(
            header, &title_key, collection.size,
            std::make_shared<ncm::NcmSource>(&entry.cs, &content_id)
        );
    };

    auto& collections = entry.collections;
    s64 read_offset = entry.nsp_data.size();
    s64 write_offset = entry.nsp_data.size();

    R_TRY(utils::nsz::NszExport(pbox, nca_creator, read_offset, write_offset, collections, keys, source, writer, path));

    // zero base the offsets.
    for (auto& collection : collections) {
        collection.offset -= entry.nsp_data.size();
    }

    // build new nsp collection with the updated offsets and sizes.
    s64 nsp_size = 0;
    const auto nsp_data = yati::container::Nsp::Build(collections, nsp_size);
    R_TRY(writer->Write(nsp_data.data(), 0, nsp_data.size()));

    // update with actual size.
    R_TRY(writer->SetSize(nsp_size));

    R_SUCCEED();
}
#endif // ENABLE_NSZ

Result Notify(Result rc, const std::string& error_message) {
    if (R_FAILED(rc)) {
        App::Push<ui::ErrorBox>(rc,
            i18n::get(error_message)
        );
    } else {
        App::Notify("Success"_i18n);
    }

    return rc;
}

bool LoadControlImage(Entry& e, title::ThreadResultData* result) {
    if (!e.image && result && !result->icon.empty()) {
        TimeStamp ts;
        const auto image = ImageLoadFromMemory(result->icon, ImageFlag_JPEG);
        if (!image.data.empty()) {
            e.image = nvgCreateImageRGBA(App::GetVg(), image.w, image.h, 0, image.data.data());
            log_write("\t[image load] time taken: %.2fs %zums\n", ts.GetSecondsD(), ts.GetMs());
            return true;
        }
    }

    return false;
}

void LoadResultIntoEntry(Entry& e, title::ThreadResultData* result) {
    if (result) {
        e.status = result->status;
        e.lang = result->lang;
        e.status = result->status;
    }
}

void LoadControlEntry(Entry& e, bool force_image_load = false) {
    if (e.status != title::NacpLoadStatus::Loaded) {
        LoadResultIntoEntry(e, title::Get(e.app_id));
    }

    if (force_image_load && e.status == title::NacpLoadStatus::Loaded) {
        LoadControlImage(e, title::Get(e.app_id));
    }
}

void FreeEntry(NVGcontext* vg, Entry& e) {
    nvgDeleteImage(vg, e.image);
    e.image = 0;
}

void LaunchEntry(const Entry& e) {
    const auto rc = appletRequestLaunchApplication(e.app_id, nullptr);
    Notify(rc, "Failed to launch application"_i18n);
}

Result CreateSave(u64 app_id, AccountUid uid) {
    u64 actual_size;
    auto data = std::make_unique<NsApplicationControlData>();
    R_TRY(nsGetApplicationControlData(NsApplicationControlSource_Storage, app_id, data.get(), sizeof(NsApplicationControlData), &actual_size));

    FsSaveDataAttribute attr{};
    attr.application_id = app_id;
    attr.uid = uid;
    attr.save_data_type = FsSaveDataType_Account;

    FsSaveDataCreationInfo info{};
    info.save_data_size = data->nacp.user_account_save_data_size;
    info.journal_size = data->nacp.user_account_save_data_journal_size;
    info.available_size = data->nacp.user_account_save_data_size; // todo: check what this should be.
    info.owner_id = data->nacp.save_data_owner_id;
    info.save_data_space_id = FsSaveDataSpaceId_User;

    // https://switchbrew.org/wiki/Filesystem_services#CreateSaveDataFileSystem
    FsSaveDataMetaInfo meta{};
    meta.size = 0x40060;
    meta.type = FsSaveDataMetaType_Thumbnail;

    R_TRY(fsCreateSaveDataFileSystem(&attr, &info, &meta));

    R_SUCCEED();
}

} // namespace

Result NspEntry::Read(void* buf, s64 off, s64 size, u64* bytes_read) {
    if (off == nsp_size) {
        log_write("[NspEntry::Read] read at eof...\n");
        *bytes_read = 0;
        R_SUCCEED();
    }

    if (off < nsp_data.size()) {
        *bytes_read = size = ClipSize(off, size, nsp_data.size());
        std::memcpy(buf, nsp_data.data() + off, size);
        R_SUCCEED();
    }

    // adjust offset.
    off -= nsp_data.size();

    for (const auto& collection : collections) {
        if (InRange(off, collection.offset, collection.size)) {
            // adjust offset relative to the collection.
            off -= collection.offset;
            *bytes_read = size = ClipSize(off, size, collection.size);

            if (collection.name.ends_with(".nca")) {
                const auto id = ncm::GetContentIdFromStr(collection.name.c_str());
                return ncmContentStorageReadContentIdFile(&cs, buf, size, &id, off);
            } else if (collection.name.ends_with(".tik") || collection.name.ends_with(".cert")) {
                FsRightsId id;
                keys::parse_hex_key(&id, collection.name.c_str());

                const auto it = std::ranges::find_if(tickets, [&id](auto& e){
                    return !std::memcmp(&id, &e.id, sizeof(id));
                });
                R_UNLESS(it != tickets.end(), Result_GameBadReadForDump);

                const auto& data = collection.name.ends_with(".tik") ? it->tik_data : it->cert_data;
                std::memcpy(buf, data.data() + off, size);
                R_SUCCEED();
            }
        }
    }

    log_write("did not find collection...\n");
    return 0x1;
}

void SignalChange() {
    g_change_signalled = true;
}

Menu::Menu(u32 flags) : grid::Menu{"Games"_i18n, flags} {
    this->SetActions(
        std::make_pair(Button::L3, Action{[this](){
            if (m_entries.empty()) {
                return;
            }

            m_entries[m_index].selected ^= 1;

            if (m_entries[m_index].selected) {
                m_selected_count++;
            } else {
                m_selected_count--;
            }
        }}),
        std::make_pair(Button::R3, Action{[this](){
            if (m_entries.empty()) {
                return;
            }

            if (m_selected_count == m_entries.size()) {
                ClearSelection();
            } else {
                m_selected_count = m_entries.size();
                for (auto& e : m_entries) {
                    e.selected = true;
                }
            }
        }}),
        std::make_pair(Button::B, Action{"Back"_i18n, [this](){
            SetPop();
        }}),
        std::make_pair(Button::A, Action{"Launch"_i18n, [this](){
            if (m_entries.empty()) {
                return;
            }
            LaunchEntry(m_entries[m_index]);
        }}),
        std::make_pair(Button::X, Action{"Options"_i18n, [this](){
            auto options = std::make_unique<Sidebar>("Game Options"_i18n, Sidebar::Side::RIGHT);
            ON_SCOPE_EXIT(App::Push(std::move(options)));

            if (m_entries.size()) {
                options->Add<SidebarEntryCallback>("Sort By"_i18n, [this](){
                    auto options = std::make_unique<Sidebar>("Sort Options"_i18n, Sidebar::Side::RIGHT);
                    ON_SCOPE_EXIT(App::Push(std::move(options)));

                    SidebarEntryArray::Items sort_items;
                    sort_items.push_back("Updated"_i18n);

                    SidebarEntryArray::Items order_items;
                    order_items.push_back("Descending"_i18n);
                    order_items.push_back("Ascending"_i18n);

                    SidebarEntryArray::Items layout_items;
                    layout_items.push_back("List"_i18n);
                    layout_items.push_back("Icon"_i18n);
                    layout_items.push_back("Grid"_i18n);

                    options->Add<SidebarEntryArray>("Sort"_i18n, sort_items, [this](s64& index_out){
                        m_sort.Set(index_out);
                        SortAndFindLastFile(false);
                    }, m_sort.Get());

                    options->Add<SidebarEntryArray>("Order"_i18n, order_items, [this](s64& index_out){
                        m_order.Set(index_out);
                        SortAndFindLastFile(false);
                    }, m_order.Get());

                    options->Add<SidebarEntryArray>("Layout"_i18n, layout_items, [this](s64& index_out){
                        m_layout.Set(index_out);
                        OnLayoutChange();
                    }, m_layout.Get());

                    options->Add<SidebarEntryBool>("Hide forwarders"_i18n, m_hide_forwarders.Get(), [this](bool& v_out){
                        m_hide_forwarders.Set(v_out);
                        m_dirty = true;
                    });
                });

                options->Add<SidebarEntryCallback>("View application content"_i18n, [this](){
                    App::Push<meta::Menu>(m_entries[m_index]);
                });

                options->Add<SidebarEntryCallback>("Launch random game"_i18n, [this](){
                    const auto random_index = randomGet64() % std::size(m_entries);
                    auto& e = m_entries[random_index];
                    LoadControlEntry(e, true);

                    App::Push<OptionBox>(
                        i18n::Reorder("Launch ", e.GetName()) + '?',
                        "Back"_i18n, "Launch"_i18n, 1, [this, &e](auto op_index){
                            if (op_index && *op_index) {
                                LaunchEntry(e);
                            }
                        }, e.image
                    );
                });

                auto export_nsp = options->Add<SidebarEntryCallback>("Export NSP"_i18n, [this](){
                    ExportOptions(false);
                });
                export_nsp->Depends(App::IsApplication, "Not supported in Applet Mode"_i18n);

                auto export_nsz = options->Add<SidebarEntryCallback>("Export NSZ"_i18n, [this](){
                    ExportOptions(true);
                },  "Exports to NSZ (compressed NSP)"_i18n);
                export_nsz->Depends(App::IsApplication, "Not supported in Applet Mode"_i18n);

                options->Add<SidebarEntryCallback>("Export options"_i18n, [this](){
                    App::DisplayDumpOptions(false);
                });

                // completely deletes the application record and all data.
                options->Add<SidebarEntryCallback>("Delete"_i18n, [this](){
                    const auto buf = i18n::Reorder("Are you sure you want to delete ", m_entries[m_index].GetName()) + "?";
                    App::Push<OptionBox>(
                        buf,
                        "Back"_i18n, "Delete"_i18n, 0, [this](auto op_index){
                            if (op_index && *op_index) {
                                DeleteGames();
                            }
                        }, m_entries[m_index].image
                    );
                }, true);
            }

            options->Add<SidebarEntryCallback>("Advanced options"_i18n, [this](){
                auto options = std::make_unique<Sidebar>("Advanced Options"_i18n, Sidebar::Side::RIGHT);
                ON_SCOPE_EXIT(App::Push(std::move(options)));

                options->Add<SidebarEntryCallback>("Refresh"_i18n, [this](){
                    m_dirty = true;
                    App::PopToMenu();
                });

                options->Add<SidebarEntryCallback>("Create contents folder"_i18n, [this](){
                    const auto rc = fs::FsNativeSd().CreateDirectory(title::GetContentsPath(m_entries[m_index].app_id));
                    App::PushErrorBox(rc, "Folder create failed!"_i18n);

                    if (R_SUCCEEDED(rc)) {
                        App::Notify("Folder created!"_i18n);
                    }
                });

                options->Add<SidebarEntryCallback>("Create save"_i18n, [this](){
                    ui::PopupList::Items items{};
                    const auto accounts = App::GetAccountList();
                    for (auto& p : accounts) {
                        items.emplace_back(p.nickname);
                    }

                    App::Push<ui::PopupList>(
                        "Select user to create save for"_i18n, items, [this, accounts](auto op_index){
                            if (op_index) {
                                CreateSaves(accounts[*op_index].uid);
                            }
                        }
                    );
                });

                options->Add<SidebarEntryCallback>("Delete title cache"_i18n, [this](){
                    App::Push<OptionBox>(
                        "Are you sure you want to delete the title cache?"_i18n,
                        "Back"_i18n, "Delete"_i18n, 0, [this](auto op_index){
                            if (op_index && *op_index) {
                                m_dirty = true;
                                title::Clear();
                                App::PopToMenu();
                            }
                        }
                    );
                });
            });
        }})
    );

    OnLayoutChange();

    ns::Initialize();
    es::Initialize();
    title::Init();

    fsOpenGameCardDetectionEventNotifier(std::addressof(m_gc_event_notifier));
    fsEventNotifierGetEventHandle(std::addressof(m_gc_event_notifier), std::addressof(m_gc_event), true);
}

Menu::~Menu() {
    title::Exit();

    FreeEntries();
    ns::Exit();
    es::Exit();

    eventClose(std::addressof(m_gc_event));
    fsEventNotifierClose(std::addressof(m_gc_event_notifier));
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    if (g_change_signalled.exchange(false)) {
        m_dirty = true;
    }

    if (R_SUCCEEDED(eventWait(&m_gc_event, 0))) {
        m_dirty = true;
    }

    if (m_dirty) {
        App::Notify("Updating application record list"_i18n);
        SortAndFindLastFile(true);
    }

    MenuBase::Update(controller, touch);
    m_list->OnUpdate(controller, touch, m_index, m_entries.size(), [this](bool touch, auto i) {
        if (touch && m_index == i) {
            FireAction(Button::A);
        } else {
            App::PlaySoundEffect(SoundEffect::Focus);
            SetIndex(i);
        }
    });
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    if (m_entries.empty()) {
        gfx::drawTextArgs(vg, GetX() + GetW() / 2.f, GetY() + GetH() / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Empty..."_i18n.c_str());
        return;
    }

    // max images per frame, in order to not hit io / gpu too hard.
    const int image_load_max = 2;
    int image_load_count = 0;

    m_list->Draw(vg, theme, m_entries.size(), [this, &image_load_count](auto* vg, auto* theme, auto v, auto pos) {
        const auto& [x, y, w, h] = v;
        auto& e = m_entries[pos];

        if (e.status == title::NacpLoadStatus::None) {
            title::PushAsync(e.app_id);
            e.status = title::NacpLoadStatus::Progress;
        } else if (e.status == title::NacpLoadStatus::Progress) {
            LoadResultIntoEntry(e, title::GetAsync(e.app_id));
        }

        // lazy load image
        if (image_load_count < image_load_max) {
            if (LoadControlImage(e, title::GetAsync(e.app_id))) {
                image_load_count++;
            }
        }

        char title_id[33];
        std::snprintf(title_id, sizeof(title_id), "%016lX", e.app_id);

        const auto selected = pos == m_index;
        DrawEntry(vg, theme, m_layout.Get(), v, selected, e.image, e.GetName(), e.GetAuthor(), title_id);

        if (e.selected) {
            gfx::drawRect(vg, v, theme->GetColour(ThemeEntryID_FOCUS), 5);
            gfx::drawText(vg, x + w / 2, y + h / 2, 24.f, "\uE14B", nullptr, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_SELECTED));
        }
    });
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();
    if (m_entries.empty()) {
        ScanHomebrew();
    }
}

void Menu::SetIndex(s64 index) {
    m_index = index;
    if (!m_index) {
        m_list->SetYoff(0);
    }

    char title_id[33];
    std::snprintf(title_id, sizeof(title_id), "%016lX", m_entries[m_index].app_id);
    SetTitleSubHeading(title_id);
    this->SetSubHeading(std::to_string(m_index + 1) + " / " + std::to_string(m_entries.size()));
}

void Menu::ScanHomebrew() {
    constexpr auto ENTRY_CHUNK_COUNT = 1000;
    const auto hide_forwarders = m_hide_forwarders.Get();
    TimeStamp ts;

    App::SetBoostMode(true);
    ON_SCOPE_EXIT(App::SetBoostMode(false));

    FreeEntries();
    m_entries.reserve(ENTRY_CHUNK_COUNT);
    g_change_signalled = false;

    std::vector<NsApplicationRecord> record_list(ENTRY_CHUNK_COUNT);
    s32 offset{};
    while (true) {
        s32 record_count{};
        if (R_FAILED(nsListApplicationRecord(record_list.data(), record_list.size(), offset, &record_count))) {
            log_write("failed to list application records at offset: %d\n", offset);
        }

        // finished parsing all entries.
        if (!record_count) {
            break;
        }

        for (s32 i = 0; i < record_count; i++) {
            const auto& e = record_list[i];

            if (hide_forwarders && (e.application_id & 0x0500000000000000) == 0x0500000000000000) {
                continue;
            }

            m_entries.emplace_back(e.application_id, e.last_event);
        }

        offset += record_count;
    }

    m_is_reversed = false;
    m_dirty = false;
    log_write("games found: %zu time_taken: %.2f seconds %zu ms %zu ns\n", m_entries.size(), ts.GetSecondsD(), ts.GetMs(), ts.GetNs());
    this->Sort();
    SetIndex(0);
    ClearSelection();
}

void Menu::Sort() {
    // const auto sort = m_sort.Get();
    const auto order = m_order.Get();

    if (order == OrderType_Ascending) {
        if (!m_is_reversed) {
            std::ranges::reverse(m_entries);
            m_is_reversed = true;
        }
    } else {
        if (m_is_reversed) {
            std::ranges::reverse(m_entries);
            m_is_reversed = false;
        }
    }
}

void Menu::SortAndFindLastFile(bool scan) {
    const auto app_id = m_entries[m_index].app_id;
    if (scan) {
        ScanHomebrew();
    } else {
        Sort();
    }
    SetIndex(0);

    s64 index = -1;
    for (u64 i = 0; i < m_entries.size(); i++) {
        if (app_id == m_entries[i].app_id) {
            index = i;
            break;
        }
    }

    if (index >= 0) {
        const auto row = m_list->GetRow();
        const auto page = m_list->GetPage();
        // guesstimate where the position is
        if (index >= page) {
            m_list->SetYoff((((index - page) + row) / row) * m_list->GetMaxY());
        } else {
            m_list->SetYoff(0);
        }
        SetIndex(index);
    }
}

void Menu::FreeEntries() {
    auto vg = App::GetVg();

    for (auto&p : m_entries) {
        FreeEntry(vg, p);
    }

    m_entries.clear();
}

void Menu::OnLayoutChange() {
    m_index = 0;
    grid::Menu::OnLayoutChange(m_list, m_layout.Get());
}

void Menu::DeleteGames() {
    App::Push<ProgressBox>(0, "Deleting"_i18n, "", [this](auto pbox) -> Result {
        auto targets = GetSelectedEntries();

        for (s64 i = 0; i < std::size(targets); i++) {
            auto& e = targets[i];

            LoadControlEntry(e);
            pbox->SetTitle(e.GetName());
            pbox->UpdateTransfer(i + 1, std::size(targets));
            R_TRY(nsDeleteApplicationCompletely(e.app_id));
        }

        R_SUCCEED();
    }, [this](Result rc){
        App::PushErrorBox(rc, "Delete failed!"_i18n);

        ClearSelection();
        m_dirty = true;

        if (R_SUCCEEDED(rc)) {
            App::Notify("Delete successfull!"_i18n);
        }
    });
}

void Menu::ExportOptions(bool to_nsz) {
    auto options = std::make_unique<Sidebar>("Select content to export"_i18n, Sidebar::Side::RIGHT);
    ON_SCOPE_EXIT(App::Push(std::move(options)));

    options->Add<SidebarEntryCallback>("Export All"_i18n, [this, to_nsz](){
        DumpGames(title::ContentFlag_All, to_nsz);
    }, true);
    options->Add<SidebarEntryCallback>("Export Application"_i18n, [this, to_nsz](){
        DumpGames(title::ContentFlag_Application, to_nsz);
    }, true);
    options->Add<SidebarEntryCallback>("Export Patch"_i18n, [this, to_nsz](){
        DumpGames(title::ContentFlag_Patch, to_nsz);
    }, true);
    options->Add<SidebarEntryCallback>("Export AddOnContent"_i18n, [this, to_nsz](){
        DumpGames(title::ContentFlag_AddOnContent, to_nsz);
    }, true);
    options->Add<SidebarEntryCallback>("Export DataPatch"_i18n, [this, to_nsz](){
        DumpGames(title::ContentFlag_DataPatch, to_nsz);
    }, true);
}

void Menu::DumpGames(u32 flags, bool to_nsz) {
    auto targets = GetSelectedEntries();

    std::vector<NspEntry> nsp_entries;
    for (auto& e : targets) {
        BuildNspEntries(e, flags, nsp_entries, to_nsz);
    }

    DumpNsp(nsp_entries, to_nsz);
}

void Menu::CreateSaves(AccountUid uid) {
    App::Push<ProgressBox>(0, "Creating"_i18n, "", [this, uid](auto pbox) -> Result {
        auto targets = GetSelectedEntries();

        for (s64 i = 0; i < std::size(targets); i++) {
            auto& e = targets[i];

            LoadControlEntry(e);
            pbox->SetTitle(e.GetName());
            pbox->UpdateTransfer(i + 1, std::size(targets));
            const auto rc = CreateSave(e.app_id, uid);

            // don't error if the save already exists.
            if (R_FAILED(rc) && rc != FsError_PathAlreadyExists) {
                R_THROW(rc);
            }
        }

        R_SUCCEED();
    }, [this](Result rc){
        App::PushErrorBox(rc, "Save create failed!"_i18n);

        ClearSelection();
        save::SignalChange();

        if (R_SUCCEEDED(rc)) {
            App::Notify("Save create successfull!"_i18n);
        }
    });
}

Result GetMetaEntries(const Entry& e, title::MetaEntries& out, u32 flags) {
    return title::GetMetaEntries(e.app_id, out, flags);
}

Result GetNcmMetaFromMetaStatus(const NsApplicationContentMetaStatus& status, NcmMetaData& out) {
    out.cs = &title::GetNcmCs(status.storageID);
    out.db = &title::GetNcmDb(status.storageID);
    out.app_id = ncm::GetAppId(status.meta_type, status.application_id);

    auto id_min = status.application_id;
    auto id_max = status.application_id;
    // workaround N bug where they don't check the full range in the ID filter.
    // https://github.com/Atmosphere-NX/Atmosphere/blob/1d3f3c6e56b994b544fc8cd330c400205d166159/libraries/libstratosphere/source/ncm/ncm_on_memory_content_meta_database_impl.cpp#L22
    if (status.storageID == NcmStorageId_None || status.storageID == NcmStorageId_GameCard) {
        id_min -= 1;
        id_max += 1;
    }

    s32 meta_total;
    s32 meta_entries_written;
    R_TRY(ncmContentMetaDatabaseList(out.db, &meta_total, &meta_entries_written, &out.key, 1, (NcmContentMetaType)status.meta_type, out.app_id, id_min, id_max, NcmContentInstallType_Full));
    // log_write("ncmContentMetaDatabaseList(): AppId: %016lX Id: %016lX total: %d written: %d storageID: %u key.id %016lX\n", out.app_id, status.application_id, meta_total, meta_entries_written, status.storageID, out.key.id);
    R_UNLESS(meta_total == 1, Result_GameMultipleKeysFound);
    R_UNLESS(meta_entries_written == 1, Result_GameMultipleKeysFound);

    R_SUCCEED();
}

// deletes the array of entries (remove nca, remove ncm db, remove ns app records).
void DeleteMetaEntries(u64 app_id, int image, const std::string& name, const title::MetaEntries& entries) {
    App::Push<ProgressBox>(image, "Delete"_i18n, name, [app_id, entries](ProgressBox* pbox) -> Result {
        R_TRY(ns::Initialize());
        ON_SCOPE_EXIT(ns::Exit());

        // fetch current app records.
        std::vector<ncm::ContentStorageRecord> records;
        R_TRY(ns::GetApplicationRecords(app_id, records));

        // on exit, delete old record list and push the new one.
        ON_SCOPE_EXIT(
            R_TRY(ns::DeleteApplicationRecord(app_id));
            return ns::PushApplicationRecord(app_id, records.data(), records.size());
        )

        // on exit, set the new lowest version.
        ON_SCOPE_EXIT(
            ns::SetLowestLaunchVersion(app_id, records);
        )

        for (u32 i = 0; i < std::size(entries); i++) {
            const auto& status = entries[i];

            // check if the user wants to exit, only in-between each successful delete.
            R_TRY(pbox->ShouldExitResult());

            char transfer_str[33];
            std::snprintf(transfer_str, sizeof(transfer_str), "%016lX", status.application_id);
            pbox->NewTransfer(transfer_str).UpdateTransfer(i, std::size(entries));

            NcmMetaData meta;
            R_TRY(GetNcmMetaFromMetaStatus(status, meta));

            // only delete form non read-only storage.
            if (status.storageID == NcmStorageId_BuiltInUser || status.storageID == NcmStorageId_SdCard) {
                R_TRY(ncm::DeleteKey(meta.cs, meta.db, &meta.key));
            }

            // find and remove record.
            std::erase_if(records, [&meta](auto& e){
                return meta.key.id == e.key.id;
            });
        }

        R_SUCCEED();
    }, [](Result rc){
        App::PushErrorBox(rc, "Failed to delete meta entry"_i18n);
    });
}

auto BuildNspPath(const Entry& e, const NsApplicationContentMetaStatus& status, bool to_nsz) -> fs::FsPath {
    fs::FsPath name_buf = e.GetName();
    title::utilsReplaceIllegalCharacters(name_buf, true);

    char version[sizeof(NacpStruct::display_version) + 1]{};
    if (status.meta_type == NcmContentMetaType_Patch) {
        u64 program_id;
        fs::FsPath path;
        if (R_SUCCEEDED(title::GetControlPathFromStatus(status, &program_id, &path))) {
            char display_version[0x10];
            if (R_SUCCEEDED(nca::ParseControl(path, program_id, display_version, sizeof(display_version), nullptr, offsetof(NacpStruct, display_version)))) {
                std::snprintf(version, sizeof(version), "%s ", display_version);
            }
        }
    }

    const auto ext = to_nsz ? "nsz" : "nsp";

    fs::FsPath path;
    if (App::GetApp()->m_dump_app_folder.Get()) {
        std::snprintf(path, sizeof(path), "%s/%s %s[%016lX][v%u][%s].%s", name_buf.s, name_buf.s, version, status.application_id, status.version, ncm::GetMetaTypeShortStr(status.meta_type), ext);
    } else {
        std::snprintf(path, sizeof(path), "%s %s[%016lX][v%u][%s].%s", name_buf.s, version, status.application_id, status.version, ncm::GetMetaTypeShortStr(status.meta_type), ext);
    }

    return path;
}

Result BuildContentEntry(const NsApplicationContentMetaStatus& status, ContentInfoEntry& out, bool to_nsz) {
    NcmMetaData meta;
    R_TRY(GetNcmMetaFromMetaStatus(status, meta));

    std::vector<NcmContentInfo> infos;
    R_TRY(ncm::GetContentInfos(meta.db, &meta.key, infos));

    std::vector<NcmContentInfo> cnmt_infos;
    for (const auto& info : infos) {
        // check if we need to fetch tickets.
        NcmRightsId ncm_rights_id;
        R_TRY(ncmContentStorageGetRightsIdFromContentId(meta.cs, std::addressof(ncm_rights_id), std::addressof(info.content_id), FsContentAttributes_All));

        if (es::IsRightsIdValid(ncm_rights_id.rights_id)) {
            const auto it = std::ranges::find_if(out.ncm_rights_id, [&ncm_rights_id](auto& e){
                return !std::memcmp(&e, &ncm_rights_id, sizeof(ncm_rights_id));
            });

            if (it == out.ncm_rights_id.end()) {
                out.ncm_rights_id.emplace_back(ncm_rights_id);
            }
        }

        if (info.content_type == NcmContentType_Meta) {
            cnmt_infos.emplace_back(info);
        } else {
            out.content_infos.emplace_back(info);
        }
    }

    // append cnmt at the end of the list, following StandardNSP spec.
    out.content_infos.insert_range(out.content_infos.end(), cnmt_infos);
    out.status = status;
    R_SUCCEED();
}

Result BuildNspEntry(const Entry& e, const ContentInfoEntry& info, const keys::Keys& keys, NspEntry& out, bool to_nsz) {
    out.application_name = e.GetName();
    out.path = BuildNspPath(e, info.status, to_nsz);
    s64 offset{};

    for (auto& e : info.content_infos) {
        char nca_name[64];
        std::snprintf(nca_name, sizeof(nca_name), "%s%s", utils::hexIdToStr(e.content_id).str, e.content_type == NcmContentType_Meta ? ".cnmt.nca" : ".nca");

        u64 size;
        ncmContentInfoSizeToU64(std::addressof(e), std::addressof(size));

        out.collections.emplace_back(nca_name, offset, size);
        offset += size;
    }

    for (auto& ncm_rights_id : info.ncm_rights_id) {
        const auto rights_id = ncm_rights_id.rights_id;
        const auto key_gen = ncm_rights_id.key_generation;

        TikEntry entry{rights_id, key_gen};
        log_write("rights id is valid, fetching common ticket and cert\n");

        // todo: fetch array of tickets to know where the ticket is stored.
        if (R_FAILED(es::GetCommonTicketAndCertificate(rights_id, entry.tik_data, entry.cert_data))) {
            R_TRY(es::GetPersonalisedTicketAndCertificate(rights_id, entry.tik_data, entry.cert_data));
        }

        // patch fake ticket / convert personalised to common if needed.
        R_TRY(es::PatchTicket(entry.tik_data, entry.cert_data, key_gen, keys, App::GetApp()->m_dump_convert_to_common_ticket.Get()));

        char tik_name[64];
        std::snprintf(tik_name, sizeof(tik_name), "%s%s", utils::hexIdToStr(rights_id).str, ".tik");

        char cert_name[64];
        std::snprintf(cert_name, sizeof(cert_name), "%s%s", utils::hexIdToStr(rights_id).str, ".cert");

        out.collections.emplace_back(tik_name, offset, entry.tik_data.size());
        offset += entry.tik_data.size();

        out.collections.emplace_back(cert_name, offset, entry.cert_data.size());
        offset += entry.cert_data.size();

        out.tickets.emplace_back(entry);
    }

    out.nsp_data = yati::container::Nsp::Build(out.collections, out.nsp_size);
    out.cs = title::GetNcmCs(info.status.storageID);

    R_SUCCEED();
}

Result BuildNspEntries(Entry& e, const title::MetaEntries& meta_entries, std::vector<NspEntry>& out, bool to_nsz) {
    LoadControlEntry(e);

    keys::Keys keys;
    R_TRY(keys::parse_keys(keys, true));

    for (const auto& status : meta_entries) {
        ContentInfoEntry info;
        R_TRY(BuildContentEntry(status, info));

        NspEntry nsp;
        R_TRY(BuildNspEntry(e, info, keys, nsp, to_nsz));
        out.emplace_back(nsp).icon = e.image;
    }

    R_UNLESS(!out.empty(), Result_GameNoNspEntriesBuilt);
    R_SUCCEED();
}

Result BuildNspEntries(Entry& e, u32 flags, std::vector<NspEntry>& out, bool to_nsz) {
    title::MetaEntries meta_entries;
    R_TRY(GetMetaEntries(e, meta_entries, flags));

    return BuildNspEntries(e, meta_entries, out, to_nsz);
}

void DumpNsp(const std::vector<NspEntry>& entries, bool to_nsz) {
    std::vector<fs::FsPath> paths;
    for (auto& e : entries) {
        if (to_nsz) {
            paths.emplace_back(fs::AppendPath("/dumps/NSZ", e.path));
        } else {
            paths.emplace_back(fs::AppendPath("/dumps/NSP", e.path));
        }
    }

    auto source = std::make_shared<NspSource>(entries);

    if (to_nsz) {
#ifdef ENABLE_NSZ
        // todo: log keys error.
        keys::Keys keys;
        keys::parse_keys(keys, true);

        dump::Dump(source, paths, [keys](ProgressBox* pbox, dump::BaseSource* source, dump::WriteSource* writer, const fs::FsPath& path) {
            return NszExport(pbox, keys, source, writer, path);
        });
#endif // ENABLE_NSZ
    } else {
        dump::Dump(source, paths);
    }
}

} // namespace sphaira::ui::menu::game
