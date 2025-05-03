#include "app.hpp"
#include "log.hpp"
#include "fs.hpp"
#include "ui/menus/game_menu.hpp"
#include "ui/sidebar.hpp"
#include "ui/error_box.hpp"
#include "ui/option_box.hpp"
#include "ui/progress_box.hpp"
#include "ui/popup_list.hpp"
#include "ui/nvg_util.hpp"
#include "defines.hpp"
#include "i18n.hpp"
#include "yati/nx/ncm.hpp"

#include <utility>
#include <cstring>

namespace sphaira::ui::menu::game {
namespace {

// thank you Shchmue ^^
struct ApplicationOccupiedSizeEntry {
    u8 storageId;
    u8 padding[0x7];
    u64 sizeApplication;
    u64 sizePatch;
    u64 sizeAddOnContent;
};

struct ApplicationOccupiedSize {
    ApplicationOccupiedSizeEntry entry[4];
};

static_assert(sizeof(ApplicationOccupiedSize) == sizeof(NsApplicationOccupiedSize));

using MetaEntries = std::vector<NsApplicationContentMetaStatus>;

Result Notify(Result rc, const std::string& error_message) {
    if (R_FAILED(rc)) {
        App::Push(std::make_shared<ui::ErrorBox>(rc,
            i18n::get(error_message)
        ));
    } else {
        App::Notify("Success");
    }

    return rc;
}

Result GetMetaEntries(const Entry& e, MetaEntries& out) {
    s32 count;
    R_TRY(nsCountApplicationContentMeta(e.app_id, &count));

    out.resize(count);
    R_TRY(nsListApplicationContentMetaStatus(e.app_id, 0, out.data(), out.size(), &count));

    out.resize(count);
    R_SUCCEED();
}

// also sets the status to error.
void FakeNacpEntry(Entry& e) {
    e.status = NacpLoadStatus::Error;
    // fake the nacp entry
    std::strcpy(e.lang.name, "Corrupted");
    std::strcpy(e.lang.author, "Corrupted");
    std::strcpy(e.display_version, "0.0.0");
    e.control.reset();
}

bool LoadControlImage(Entry& e) {
    if (!e.image && e.control) {
        const auto jpeg_size = e.control_size - sizeof(NacpStruct);
        e.image = nvgCreateImageMem(App::GetVg(), 0, e.control->icon, jpeg_size);
        e.control.reset();
        return true;
    }

    return false;
}

void LoadControlEntry(Entry& e, bool force_image_load = false) {
    if (e.status == NacpLoadStatus::None) {
        e.control = std::make_unique<NsApplicationControlData>();
        if (R_FAILED(nsGetApplicationControlData(NsApplicationControlSource_Storage, e.app_id, e.control.get(), sizeof(NsApplicationControlData), &e.control_size))) {
            FakeNacpEntry(e);
        } else {
            NacpLanguageEntry* lang{};
            if (R_FAILED(nsGetApplicationDesiredLanguage(&e.control->nacp, &lang)) || !lang) {
                FakeNacpEntry(e);
            } else {
                e.lang = *lang;
                std::memcpy(e.display_version, e.control->nacp.display_version, sizeof(e.display_version));
                e.status = NacpLoadStatus::Loaded;

                if (force_image_load) {
                    LoadControlImage(e);
                }
            }
        }
    }
}

void FreeEntry(NVGcontext* vg, Entry& e) {
    nvgDeleteImage(vg, e.image);
    e.image = 0;
}

void LaunchEntry(const Entry& e) {
    const auto rc = appletRequestLaunchApplication(e.app_id, nullptr);
    Notify(rc, "Failed to launch application");
}

} // namespace

Menu::Menu() : MenuBase{"Games"_i18n} {
    this->SetActions(
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
            auto options = std::make_shared<Sidebar>("Game Options"_i18n, Sidebar::Side::RIGHT);
            ON_SCOPE_EXIT(App::Push(options));

            if (m_entries.size()) {
                options->Add(std::make_shared<SidebarEntryCallback>("Sort By"_i18n, [this](){
                    auto options = std::make_shared<Sidebar>("Sort Options"_i18n, Sidebar::Side::RIGHT);
                    ON_SCOPE_EXIT(App::Push(options));

                    SidebarEntryArray::Items sort_items;
                    sort_items.push_back("Updated"_i18n);

                    SidebarEntryArray::Items order_items;
                    order_items.push_back("Descending"_i18n);
                    order_items.push_back("Ascending"_i18n);

                    options->Add(std::make_shared<SidebarEntryArray>("Sort"_i18n, sort_items, [this, sort_items](s64& index_out){
                        m_sort.Set(index_out);
                        SortAndFindLastFile();
                    }, m_sort.Get()));

                    options->Add(std::make_shared<SidebarEntryArray>("Order"_i18n, order_items, [this, order_items](s64& index_out){
                        m_order.Set(index_out);
                        SortAndFindLastFile();
                    }, m_order.Get()));

                    options->Add(std::make_shared<SidebarEntryBool>("Hide forwarders"_i18n, m_hide_forwarders.Get(), [this](bool& v_out){
                        m_hide_forwarders.Set(v_out);
                        m_dirty = true;
                    }));
                }));

                #if 0
                options->Add(std::make_shared<SidebarEntryCallback>("Info"_i18n, [this](){

                }));
                #endif

                options->Add(std::make_shared<SidebarEntryCallback>("Launch random game"_i18n, [this](){
                    const auto random_index = randomGet64() % std::size(m_entries);
                    auto& e = m_entries[random_index];
                    LoadControlEntry(e, true);

                    App::Push(std::make_shared<OptionBox>(
                        "Launch "_i18n + e.GetName(),
                        "Back"_i18n, "Launch"_i18n, 1, [this, &e](auto op_index){
                            if (op_index && *op_index) {
                                LaunchEntry(e);
                            }
                        }, e.image
                    ));
                }));

                options->Add(std::make_shared<SidebarEntryCallback>("List meta records"_i18n, [this](){
                    MetaEntries meta_entries;
                    const auto rc = GetMetaEntries(m_entries[m_index], meta_entries);
                    if (R_FAILED(rc)) {
                        App::Push(std::make_shared<ui::ErrorBox>(rc,
                            i18n::get("Failed to list application meta entries")
                        ));
                        return;
                    }

                    if (meta_entries.empty()) {
                        App::Notify("No meta entries found...\n");
                        return;
                    }

                    PopupList::Items items;
                    for (auto& e : meta_entries) {
                        char buf[256];
                        std::snprintf(buf, sizeof(buf), "Type: %s Storage: %s [%016lX][v%u]", ncm::GetMetaTypeStr(e.meta_type), ncm::GetStorageIdStr(e.storageID), e.application_id, e.version);
                        items.emplace_back(buf);
                    }

                    App::Push(std::make_shared<PopupList>(
                        "Entries", items, [this, meta_entries](auto op_index){
                            #if 0
                            if (op_index) {
                                const auto& e = meta_entries[*op_index];
                            }
                            #endif
                        }
                    ));
                }));

                // completely deletes the application record and all data.
                options->Add(std::make_shared<SidebarEntryCallback>("Delete"_i18n, [this](){
                    const auto buf = "Are you sure you want to delete "_i18n + m_entries[m_index].GetName() + "?";
                    App::Push(std::make_shared<OptionBox>(
                        buf,
                        "Back"_i18n, "Delete"_i18n, 0, [this](auto op_index){
                            if (op_index && *op_index) {
                                const auto rc = nsDeleteApplicationCompletely(m_entries[m_index].app_id);
                                if (R_SUCCEEDED(Notify(rc, "Failed to delete application"))) {
                                    FreeEntry(App::GetVg(), m_entries[m_index]);
                                    m_entries.erase(m_entries.begin() + m_index);
                                    SetIndex(m_index ? m_index - 1 : 0);
                                }
                            }
                        }, m_entries[m_index].image
                    ));
                }, true));

                // removes installed data but keeps the record, basically archiving.
                options->Add(std::make_shared<SidebarEntryCallback>("Delete entity"_i18n, [this](){
                    const auto buf = "Are you sure you want to delete "_i18n + m_entries[m_index].GetName() + "?";
                    App::Push(std::make_shared<OptionBox>(
                        buf,
                        "Back"_i18n, "Delete"_i18n, 0, [this](auto op_index){
                            if (op_index && *op_index) {
                                const auto rc = nsDeleteApplicationEntity(m_entries[m_index].app_id);
                                Notify(rc, "Failed to delete application");
                            }
                        }, m_entries[m_index].image
                    ));
                }, true));
            }
        }})
    );

    const Vec4 v{75, 110, 370, 155};
    const Vec2 pad{10, 10};
    m_list = std::make_unique<List>(3, 9, m_pos, v, pad);
    nsInitialize();
}

Menu::~Menu() {
    FreeEntries();
    nsExit();
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);
    m_list->OnUpdate(controller, touch, m_index, m_entries.size(), [this](bool touch, auto i) {
        if (touch && m_index == i) {
            FireAction(Button::A);
        } else {
            App::PlaySoundEffect(SoundEffect_Focus);
            SetIndex(i);
        }
    });
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    // max images per frame, in order to not hit io / gpu too hard.
    const int image_load_max = 2;
    int image_load_count = 0;

    m_list->Draw(vg, theme, m_entries.size(), [this, &image_load_count](auto* vg, auto* theme, auto v, auto pos) {
        const auto& [x, y, w, h] = v;
        auto& e = m_entries[pos];

        if (e.status == NacpLoadStatus::None) {
            LoadControlEntry(e);
        }

        // lazy load image
        if (image_load_count < image_load_max) {
            if (LoadControlImage(e)) {
                image_load_count++;
            }
        }

        auto text_id = ThemeEntryID_TEXT;
        const auto selected = pos == m_index;
        if (selected) {
            text_id = ThemeEntryID_TEXT_SELECTED;
            gfx::drawRectOutline(vg, theme, 4.f, v);
        } else {
            DrawElement(v, ThemeEntryID_GRID);
        }

        const float image_size = 115;
        gfx::drawImage(vg, x + 20, y + 20, image_size, image_size, e.image ? e.image : App::GetDefaultImage(), 5);

        const auto text_off = 148;
        const auto text_x = x + text_off;
        const auto text_clip_w = w - 30.f - text_off;
        const float font_size = 18;
        m_scroll_name.Draw(vg, selected, text_x, y + 45, text_clip_w, font_size, NVG_ALIGN_LEFT, theme->GetColour(text_id), e.GetName());
        m_scroll_author.Draw(vg, selected, text_x, y + 80, text_clip_w, font_size, NVG_ALIGN_LEFT, theme->GetColour(text_id), e.GetAuthor());
        m_scroll_version.Draw(vg, selected, text_x, y + 115, text_clip_w, font_size, NVG_ALIGN_LEFT, theme->GetColour(text_id), e.GetDisplayVersion());
    });
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();
    if (m_dirty || m_entries.empty()) {
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

    FreeEntries();
    m_entries.reserve(ENTRY_CHUNK_COUNT);

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
            #if 0
            u8 unk_x09 = e.unk_x09;
            u64 unk_x0a;// = e.unk_x0a;
            u8 unk_x10 = e.unk_x10;
            u64 unk_x11;// = e.unk_x11;
            memcpy(&unk_x0a, e.unk_x0a, sizeof(e.unk_x0a));
            memcpy(&unk_x11, e.unk_x11, sizeof(e.unk_x11));
            log_write("ID: %016lx got type: %u unk_x09: %u unk_x0a: %zu unk_x10: %u unk_x11: %zu\n", e.application_id, e.type,
                unk_x09,
                unk_x0a,
                unk_x10,
                unk_x11
            );
            #endif
            if (hide_forwarders && (e.application_id & 0x0500000000000000) == 0x0500000000000000) {
                continue;
            }

            s64 size{};
            // code for sorting by size, it's too slow however...
            #if 0
            ApplicationOccupiedSize occupied_size;
            if (R_SUCCEEDED(nsCalculateApplicationOccupiedSize(e.application_id, (NsApplicationOccupiedSize*)&occupied_size))) {
                for (auto& s : occupied_size.entry) {
                    size += s.sizeApplication;
                    size += s.sizePatch;
                    size += s.sizeAddOnContent;
                }
            }
            #endif

            m_entries.emplace_back(e.application_id, size);
        }

        offset += record_count;
    }

    m_is_reversed = false;
    m_dirty = false;
    log_write("games found: %zu time_taken: %.2f seconds %zu ms %zu ns\n", m_entries.size(), ts.GetSecondsD(), ts.GetMs(), ts.GetNs());
    this->Sort();
    SetIndex(0);
}

void Menu::Sort() {
    // const auto sort = m_sort.Get();
    const auto order = m_order.Get();

    if (order == OrderType_Ascending) {
        if (!m_is_reversed) {
            std::reverse(m_entries.begin(), m_entries.end());
            m_is_reversed = true;
        }
    } else {
        if (m_is_reversed) {
            std::reverse(m_entries.begin(), m_entries.end());
            m_is_reversed = false;
        }
    }
}

void Menu::SortAndFindLastFile() {
    const auto app_id = m_entries[m_index].app_id;
    Sort();
    SetIndex(0);

    s64 index = -1;
    for (u64 i = 0; i < m_entries.size(); i++) {
        if (app_id == m_entries[i].app_id) {
            index = i;
            break;
        }
    }

    if (index >= 0) {
        // guesstimate where the position is
        if (index >= 9) {
            m_list->SetYoff((((index - 9) + 3) / 3) * m_list->GetMaxY());
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

} // namespace sphaira::ui::menu::game
