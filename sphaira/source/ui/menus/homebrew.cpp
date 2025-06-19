#include "app.hpp"
#include "log.hpp"
#include "fs.hpp"
#include "ui/menus/homebrew.hpp"
#include "ui/sidebar.hpp"
#include "ui/error_box.hpp"
#include "ui/option_box.hpp"
#include "ui/progress_box.hpp"
#include "ui/nvg_util.hpp"
#include "owo.hpp"
#include "defines.hpp"
#include "i18n.hpp"
#include "image.hpp"

#include <minIni.h>
#include <utility>
#include <algorithm>

namespace sphaira::ui::menu::homebrew {
namespace {

Menu* g_menu{};
constinit UEvent g_change_uevent;

auto GenerateStarPath(const fs::FsPath& nro_path) -> fs::FsPath {
    fs::FsPath out{};
    const auto dilem = std::strrchr(nro_path.s, '/');
    std::snprintf(out, sizeof(out), "%.*s.%s.star", int(dilem - nro_path.s + 1), nro_path.s, dilem + 1);
    return out;
}

void FreeEntry(NVGcontext* vg, NroEntry& e) {
    nvgDeleteImage(vg, e.image);
    e.image = 0;
}

} // namespace

void SignalChange() {
    ueventSignal(&g_change_uevent);
}

auto GetNroEntries() -> std::span<const NroEntry> {
    if (!g_menu) {
        return {};
    }

    return g_menu->GetHomebrewList();
}

Menu::Menu() : grid::Menu{"Homebrew"_i18n, MenuFlag_Tab} {
    g_menu = this;

    this->SetActions(
        std::make_pair(Button::A, Action{"Launch"_i18n, [this](){
            nro_launch(m_entries[m_index].path);
        }}),
        std::make_pair(Button::X, Action{"Options"_i18n, [this](){
            auto options = std::make_shared<Sidebar>("Homebrew Options"_i18n, Sidebar::Side::RIGHT);
            ON_SCOPE_EXIT(App::Push(options));

            if (m_entries.size()) {
                options->Add(std::make_shared<SidebarEntryCallback>("Sort By"_i18n, [this](){
                    auto options = std::make_shared<Sidebar>("Sort Options"_i18n, Sidebar::Side::RIGHT);
                    ON_SCOPE_EXIT(App::Push(options));

                    SidebarEntryArray::Items sort_items;
                    sort_items.push_back("Updated"_i18n);
                    sort_items.push_back("Alphabetical"_i18n);
                    sort_items.push_back("Size"_i18n);
                    sort_items.push_back("Updated (Star)"_i18n);
                    sort_items.push_back("Alphabetical (Star)"_i18n);
                    sort_items.push_back("Size (Star)"_i18n);

                    SidebarEntryArray::Items order_items;
                    order_items.push_back("Descending"_i18n);
                    order_items.push_back("Ascending"_i18n);

                    SidebarEntryArray::Items layout_items;
                    layout_items.push_back("List"_i18n);
                    layout_items.push_back("Icon"_i18n);
                    layout_items.push_back("Grid"_i18n);

                    options->Add(std::make_shared<SidebarEntryArray>("Sort"_i18n, sort_items, [this, sort_items](s64& index_out){
                        m_sort.Set(index_out);
                        SortAndFindLastFile();
                    }, m_sort.Get()));

                    options->Add(std::make_shared<SidebarEntryArray>("Order"_i18n, order_items, [this, order_items](s64& index_out){
                        m_order.Set(index_out);
                        SortAndFindLastFile();
                    }, m_order.Get()));

                    options->Add(std::make_shared<SidebarEntryArray>("Layout"_i18n, layout_items, [this](s64& index_out){
                        m_layout.Set(index_out);
                        OnLayoutChange();
                    }, m_layout.Get()));

                    options->Add(std::make_shared<SidebarEntryBool>("Hide Sphaira"_i18n, m_hide_sphaira.Get(), [this](bool& enable){
                        m_hide_sphaira.Set(enable);
                    }));
                }));

                #if 0
                options->Add(std::make_shared<SidebarEntryCallback>("Info"_i18n, [this](){

                }));
                #endif

                options->Add(std::make_shared<SidebarEntryCallback>("Delete"_i18n, [this](){
                    const auto buf = "Are you sure you want to delete "_i18n + m_entries[m_index].path.toString() + "?";
                    App::Push(std::make_shared<OptionBox>(
                        buf,
                        "Back"_i18n, "Delete"_i18n, 1, [this](auto op_index){
                            if (op_index && *op_index) {
                                if (R_SUCCEEDED(fs::FsNativeSd().DeleteFile(m_entries[m_index].path))) {
                                    FreeEntry(App::GetVg(), m_entries[m_index]);
                                    m_entries.erase(m_entries.begin() + m_index);
                                    SetIndex(m_index ? m_index - 1 : 0);
                                }
                            }
                        }, m_entries[m_index].image
                    ));
                }, true));

                if (App::GetInstallEnable()) {
                    options->Add(std::make_shared<SidebarEntryCallback>("Install Forwarder"_i18n, [this](){
                        if (App::GetInstallPrompt()) {
                            App::Push(std::make_shared<OptionBox>(
                                "WARNING: Installing forwarders will lead to a ban!"_i18n,
                                "Back"_i18n, "Install"_i18n, 0, [this](auto op_index){
                                    if (op_index && *op_index) {
                                        InstallHomebrew();
                                    }
                                }, m_entries[m_index].image
                            ));
                        } else {
                            InstallHomebrew();
                        }
                    }, true));
                }
            }
        }})
    );

    OnLayoutChange();
    ueventCreate(&g_change_uevent, true);
}

Menu::~Menu() {
    g_menu = {};
    FreeEntries();
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    if (R_SUCCEEDED(waitSingle(waiterForUEvent(&g_change_uevent), 0))) {
        m_dirty = true;
    }

    if (m_dirty) {
        SortAndFindLastFile(true);
    }

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
        auto& e = m_entries[pos];

        // lazy load image
        if (image_load_count < image_load_max) {
            if (!e.image && e.icon_size && e.icon_offset) {
                // NOTE: it seems that images can be any size. SuperTux uses a 1024x1024
                // ~300Kb image, which takes a few frames to completely load.
                // really, switch-tools should handle this by resizing the image before
                // adding it to the nro, as well as validate its a valid jpeg.
                const auto icon = nro_get_icon(e.path, e.icon_size, e.icon_offset);
                TimeStamp ts;
                if (!icon.empty()) {
                    const auto image = ImageLoadFromMemory(icon, ImageFlag_JPEG);
                    if (!image.data.empty()) {
                        e.image = nvgCreateImageRGBA(vg, image.w, image.h, 0, image.data.data());
                        log_write("\t[image load] time taken: %.2fs %zums\n", ts.GetSecondsD(), ts.GetMs());
                        image_load_count++;
                    } else {
                        // prevent loading of this icon again as it's already failed.
                        e.icon_offset = e.icon_size = 0;
                    }
                }
            }
        }


        bool has_star = false;
        if (IsStarEnabled()) {
            if (!e.has_star.has_value()) {
                e.has_star = fs::FsNativeSd().FileExists(GenerateStarPath(e.path));
            }
            has_star = e.has_star.value();
        }

        std::string name;
        if (has_star) {
            name = std::string("\u2605 ") + e.GetName();
        } else {
            name = e.GetName();
        }

        const auto selected = pos == m_index;
        DrawEntry(vg, theme, m_layout.Get(), v, selected, e.image, name.c_str(), e.GetAuthor(), e.GetDisplayVersion());
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

    if (IsStarEnabled()) {
        const auto star_path = GenerateStarPath(m_entries[m_index].path);
        if (fs::FsNativeSd().FileExists(star_path)) {
            SetAction(Button::R3, Action{"Unstar"_i18n, [this](){
                fs::FsNativeSd().DeleteFile(GenerateStarPath(m_entries[m_index].path));
                App::Notify("Unstarred "_i18n + m_entries[m_index].GetName());
                SortAndFindLastFile();
            }});
        } else {
            SetAction(Button::R3, Action{"Star"_i18n, [this](){
                fs::FsNativeSd().CreateFile(GenerateStarPath(m_entries[m_index].path));
                App::Notify("Starred "_i18n + m_entries[m_index].GetName());
                SortAndFindLastFile();
            }});
        }
    } else {
        RemoveAction(Button::R3);
    }

    // TimeCalendarTime caltime;
    // timeToCalendarTimeWithMyRule()
    // todo: fix GetFileTimeStampRaw being different to timeGetCurrentTime
    // log_write("name: %s hbini.ts: %lu file.ts: %lu smaller: %s\n", e.GetName(), e.hbini.timestamp, e.timestamp.modified, e.hbini.timestamp < e.timestamp.modified ? "true" : "false");

    SetTitleSubHeading(m_entries[m_index].path);
    this->SetSubHeading(std::to_string(m_index + 1) + " / " + std::to_string(m_entries.size()));
}

void Menu::InstallHomebrew() {
    const auto& nro = m_entries[m_index];
    InstallHomebrew(nro.path, nro_get_icon(nro.path, nro.icon_size, nro.icon_offset));
}

void Menu::ScanHomebrew() {
    TimeStamp ts;
    FreeEntries();
    nro_scan("/switch", m_entries, m_hide_sphaira.Get());
    log_write("nros found: %zu time_taken: %.2f\n", m_entries.size(), ts.GetSecondsD());

    struct IniUser {
        std::vector<NroEntry>& entires;
        Hbini* ini{};
        std::string last_section{};
    } ini_user{ m_entries };

    ini_browse([](const mTCHAR *Section, const mTCHAR *Key, const mTCHAR *Value, void *UserData) -> int {
        auto user = static_cast<IniUser*>(UserData);

        if (user->last_section != Section) {
            user->last_section = Section;
            user->ini = nullptr;

            for (auto& e : user->entires) {
                if (e.path == Section) {
                    user->ini = &e.hbini;
                    break;
                }
            }
        }

        if (user->ini) {
            if (!strcmp(Key, "timestamp")) {
                user->ini->timestamp = atoi(Value);
            }
        }

        // log_write("found: %s %s %s\n", Section, Key, Value);
        return 1;
    }, &ini_user, App::PLAYLOG_PATH);

    this->Sort();
    SetIndex(0);
    m_dirty = false;
}

void Menu::Sort() {
    if (IsStarEnabled()) {
        fs::FsNativeSd fs;
        fs::FsPath star_path;
        for (auto& p : m_entries) {
            p.has_star = fs.FileExists(GenerateStarPath(p.path));
        }
    }

    // returns true if lhs should be before rhs
    const auto sort = m_sort.Get();
    const auto order = m_order.Get();

    const auto sorter = [this, sort, order](const NroEntry& lhs, const NroEntry& rhs) -> bool {
        const auto name_cmp = [order](const NroEntry& lhs, const NroEntry& rhs) -> bool {
            auto r = strcasecmp(lhs.GetName(), rhs.GetName());
            if (!r) {
                r = strcasecmp(lhs.GetAuthor(), rhs.GetAuthor());
                if (!r) {
                    r = strcasecmp(lhs.path, rhs.path);
                }
            }

            if (order == OrderType_Descending) {
                return r < 0;
            } else {
                return r > 0;
            }
        };

        switch (sort) {
            case SortType_UpdatedStar:
                if (lhs.has_star.value() && !rhs.has_star.value()) {
                    return true;
                } else if (!lhs.has_star.value() && rhs.has_star.value()) {
                    return false;
                }
                [[fallthrough]];
            case SortType_Updated: {
                auto lhs_timestamp = lhs.hbini.timestamp;
                auto rhs_timestamp = rhs.hbini.timestamp;
                if (lhs.timestamp.is_valid && lhs_timestamp < lhs.timestamp.modified) {
                    lhs_timestamp = lhs.timestamp.modified;
                }
                if (rhs.timestamp.is_valid && rhs_timestamp < rhs.timestamp.modified) {
                    rhs_timestamp = rhs.timestamp.modified;
                }

                if (lhs_timestamp == rhs_timestamp) {
                    return name_cmp(lhs, rhs);
                } else if (order == OrderType_Descending) {
                    return lhs_timestamp > rhs_timestamp;
                } else {
                    return lhs_timestamp < rhs_timestamp;
                }
            } break;

            case SortType_SizeStar:
                if (lhs.has_star.value() && !rhs.has_star.value()) {
                    return true;
                } else if (!lhs.has_star.value() && rhs.has_star.value()) {
                    return false;
                }
                [[fallthrough]];
            case SortType_Size: {
                if (lhs.size == rhs.size) {
                    return name_cmp(lhs, rhs);
                } else if (order == OrderType_Descending) {
                    return lhs.size > rhs.size;
                } else {
                    return lhs.size < rhs.size;
                }
            } break;

            case SortType_AlphabeticalStar:
                if (lhs.has_star.value() && !rhs.has_star.value()) {
                    return true;
                } else if (!lhs.has_star.value() && rhs.has_star.value()) {
                    return false;
                }
                [[fallthrough]];
            case SortType_Alphabetical: {
                return name_cmp(lhs, rhs);
            } break;
        }

        std::unreachable();
    };

    std::sort(m_entries.begin(), m_entries.end(), sorter);
}

void Menu::SortAndFindLastFile(bool scan) {
    const auto path = m_entries[m_index].path;
    if (scan) {
        ScanHomebrew();
    } else {
        Sort();
    }
    SetIndex(0);

    s64 index = -1;
    for (u64 i = 0; i < m_entries.size(); i++) {
        if (path == m_entries[i].path) {
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

Result Menu::InstallHomebrew(const fs::FsPath& path, const std::vector<u8>& icon) {
    OwoConfig config{};
    config.nro_path = path.toString();
    R_TRY(nro_get_nacp(path, config.nacp));
    config.icon = icon;
    return App::Install(config);
}

Result Menu::InstallHomebrewFromPath(const fs::FsPath& path) {
    return InstallHomebrew(path, nro_get_icon(path));
}

} // namespace sphaira::ui::menu::homebrew
