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

#include <minIni.h>
#include <utility>

namespace sphaira::ui::menu::homebrew {
namespace {

auto GenerateStarPath(const fs::FsPath& nro_path) -> fs::FsPath {
    fs::FsPath out{};
    const auto dilem = std::strrchr(nro_path.s, '/');
    std::snprintf(out, sizeof(out), "%.*s.%s.star", dilem - nro_path.s + 1, nro_path.s, dilem + 1);
    return out;
}

} // namespace

Menu::Menu() : MenuBase{"Homebrew"_i18n} {
    this->SetActions(
        std::make_pair(Button::RIGHT, Action{[this](){
            if (m_index < (m_entries.size() - 1) && (m_index + 1) % 3 != 0) {
                SetIndex(m_index + 1);
                App::PlaySoundEffect(SoundEffect_Scroll);
                log_write("moved right\n");
            }
        }}),
        std::make_pair(Button::LEFT, Action{[this](){
            if (m_index != 0 && (m_index % 3) != 0) {
                SetIndex(m_index - 1);
                App::PlaySoundEffect(SoundEffect_Scroll);
                log_write("moved left\n");
            }
        }}),
        std::make_pair(Button::DOWN, Action{[this](){
            if (m_list->ScrollDown(m_index, 3, m_entries.size())) {
                SetIndex(m_index);
            }
        }}),
        std::make_pair(Button::UP, Action{[this](){
            if (m_list->ScrollUp(m_index, 3, m_entries.size())) {
                SetIndex(m_index);
            }
        }}),
        std::make_pair(Button::R2, Action{[this](){
            if (m_list->ScrollDown(m_index, 9, m_entries.size())) {
                SetIndex(m_index);
            }
        }}),
        std::make_pair(Button::L2, Action{[this](){
            if (m_list->ScrollUp(m_index, 9, m_entries.size())) {
                SetIndex(m_index);
            }
        }}),
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

                    options->Add(std::make_shared<SidebarEntryArray>("Sort"_i18n, sort_items, [this, sort_items](s64& index_out){
                        m_sort.Set(index_out);
                        SortAndFindLastFile();
                    }, m_sort.Get()));

                    options->Add(std::make_shared<SidebarEntryArray>("Order"_i18n, order_items, [this, order_items](s64& index_out){
                        m_order.Set(index_out);
                        SortAndFindLastFile();
                    }, m_order.Get()));

                    options->Add(std::make_shared<SidebarEntryBool>("Hide Sphaira"_i18n, m_hide_sphaira.Get(), [this](bool& enable){
                        m_hide_sphaira.Set(enable);
                    }, "Enabled"_i18n, "Disabled"_i18n));
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
                                    m_entries.erase(m_entries.begin() + m_index);
                                    SetIndex(m_index ? m_index - 1 : 0);
                                }
                            }
                        }
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
                                }
                            ));
                        } else {
                            InstallHomebrew();
                        }
                    }, true));
                }
            }
        }})
    );

    const Vec4 v{75, 110, 370, 155};
    const Vec2 pad{10, 10};
    m_list = std::make_unique<List>(3, 9, m_pos, v, pad);
}

Menu::~Menu() {
    auto vg = App::GetVg();

    for (auto&p : m_entries) {
        nvgDeleteImage(vg, p.image);
    }
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);
    m_list->OnUpdate(controller, touch, m_entries.size(), [this](auto i) {
        if (m_index == i) {
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

        // lazy load image
        if (image_load_count < image_load_max) {
            if (!e.image && e.icon_size && e.icon_offset) {
                // NOTE: it seems that images can be any size. SuperTux uses a 1024x1024
                // ~300Kb image, which takes a few frames to completely load.
                // really, switch-tools should handle this by resizing the image before
                // adding it to the nro, as well as validate its a valid jpeg.
                const auto icon = nro_get_icon(e.path, e.icon_size, e.icon_offset);
                if (!icon.empty()) {
                    e.image = nvgCreateImageMem(vg, 0, icon.data(), icon.size());
                    image_load_count++;
                }
            }
        }

        auto text_id = ThemeEntryID_TEXT;
        if (pos == m_index) {
            text_id = ThemeEntryID_TEXT_SELECTED;
            gfx::drawRectOutline(vg, theme, 4.f, v);
        } else {
            DrawElement(v, ThemeEntryID_GRID);
        }

        const float image_size = 115;
        gfx::drawImageRounded(vg, x + 20, y + 20, image_size, image_size, e.image ? e.image : App::GetDefaultImage());

        nvgSave(vg);
        nvgIntersectScissor(vg, x, y, w - 30.f, h); // clip
        {
            bool has_star = false;
            if (IsStarEnabled()) {
                if (!e.has_star.has_value()) {
                    e.has_star = fs::FsNativeSd().FileExists(GenerateStarPath(e.path));
                }
                has_star = e.has_star.value();
            }

            const float font_size = 18;
            gfx::drawTextArgs(vg, x + 148, y + 45, font_size, NVG_ALIGN_LEFT, theme->GetColour(text_id), "%s%s", has_star ? "\u2605 " : "", e.GetName());
            gfx::drawTextArgs(vg, x + 148, y + 80, font_size, NVG_ALIGN_LEFT, theme->GetColour(text_id), e.GetAuthor());
            gfx::drawTextArgs(vg, x + 148, y + 115, font_size, NVG_ALIGN_LEFT, theme->GetColour(text_id), e.GetDisplayVersion());
        }
        nvgRestore(vg);
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

    const auto& e = m_entries[m_index];

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
    InstallHomebrew(nro.path, nro.nacp, nro_get_icon(nro.path, nro.icon_size, nro.icon_offset));
}

void Menu::ScanHomebrew() {
    TimeStamp ts;
    nro_scan("/switch", m_entries, m_hide_sphaira.Get());
    log_write("nros found: %zu time_taken: %.2f\n", m_entries.size(), ts.GetSecondsD());

    struct IniUser {
        std::vector<NroEntry>& entires;
        Hbini* ini;
        std::string last_section;
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
            } else if (!strcmp(Key, "launch_count")) {
                user->ini->launch_count = atoi(Value);
            }
        }

        // log_write("found: %s %s %s\n", Section, Key, Value);
        return 1;
    }, &ini_user, App::PLAYLOG_PATH);

    this->Sort();
    SetIndex(0);
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
                auto r = strcasecmp(lhs.GetAuthor(), rhs.GetAuthor());
                if (!r) {
                    auto r = strcasecmp(lhs.path, rhs.path);
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
            case SortType_Alphabetical: {
                return name_cmp(lhs, rhs);
            } break;
        }

        std::unreachable();
    };

    std::sort(m_entries.begin(), m_entries.end(), sorter);
}

void Menu::SortAndFindLastFile() {
    const auto path = m_entries[m_index].path;
    Sort();
    SetIndex(0);

    s64 index = -1;
    for (u64 i = 0; i < m_entries.size(); i++) {
        if (path == m_entries[i].path) {
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

Result Menu::InstallHomebrew(const fs::FsPath& path, const NacpStruct& nacp, const std::vector<u8>& icon) {
    OwoConfig config{};
    config.nro_path = path.toString();
    config.nacp = nacp;
    config.icon = icon;
    return App::Install(config);
}

Result Menu::InstallHomebrewFromPath(const fs::FsPath& path) {
    NacpStruct nacp;
    R_TRY(nro_get_nacp(path, nacp))
    const auto icon = nro_get_icon(path);
    return InstallHomebrew(path, nacp, icon);
}

} // namespace sphaira::ui::menu::homebrew
