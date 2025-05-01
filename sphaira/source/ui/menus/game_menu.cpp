#include "app.hpp"
#include "log.hpp"
#include "fs.hpp"
#include "ui/menus/game_menu.hpp"
#include "ui/sidebar.hpp"
#include "ui/error_box.hpp"
#include "ui/option_box.hpp"
#include "ui/progress_box.hpp"
#include "ui/nvg_util.hpp"
#include "defines.hpp"
#include "i18n.hpp"

#include <utility>
#include <cstring>

namespace sphaira::ui::menu::game {
namespace {

Result Notify(Result rc, const std::string& error_message) {
    if (R_FAILED(rc)) {
        App::Push(std::make_shared<ui::ErrorBox>(rc,
            i18n::get(error_message.c_str())
        ));
    } else {
        App::Notify("Success");
    }

    return rc;
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
            LaunchEntry(m_entries[m_index]);
        }}),
        std::make_pair(Button::X, Action{"Options"_i18n, [this](){
            auto options = std::make_shared<Sidebar>("Homebrew Options"_i18n, Sidebar::Side::RIGHT);
            ON_SCOPE_EXIT(App::Push(options));

            if (m_entries.size()) {
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
        nvgSave(vg);
        nvgIntersectScissor(vg, text_x, y, text_clip_w, h); // clip
        {
            const float font_size = 18;
            m_scroll_name.Draw(vg, selected, text_x, y + 45, text_clip_w, font_size, NVG_ALIGN_LEFT, theme->GetColour(text_id), e.GetName());
            m_scroll_author.Draw(vg, selected, text_x, y + 80, text_clip_w, font_size, NVG_ALIGN_LEFT, theme->GetColour(text_id), e.GetAuthor());
            m_scroll_version.Draw(vg, selected, text_x, y + 115, text_clip_w, font_size, NVG_ALIGN_LEFT, theme->GetColour(text_id), e.GetDisplayVersion());
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

    char title_id[33];
    std::snprintf(title_id, sizeof(title_id), "%016lX", m_entries[m_index].app_id);
    SetTitleSubHeading(title_id);
    this->SetSubHeading(std::to_string(m_index + 1) + " / " + std::to_string(m_entries.size()));
}

void Menu::ScanHomebrew() {
    constexpr auto ENTRY_CHUNK_COUNT = 1000;
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
            // log_write("ID: %016lx got type: %u\n", record_list[i].application_id, record_list[i].type);
            m_entries.emplace_back(record_list[i].application_id);
        }

        offset += record_count;
    }

    log_write("games found: %zu time_taken: %.2f seconds %zu ms %zu ns\n", m_entries.size(), ts.GetSecondsD(), ts.GetMs(), ts.GetNs());
    SetIndex(0);
}

void Menu::FreeEntries() {
    auto vg = App::GetVg();

    for (auto&p : m_entries) {
        FreeEntry(vg, p);
    }

    m_entries.clear();
}

} // namespace sphaira::ui::menu::game
