#include "ui/menus/game_meta_menu.hpp"
#include "ui/menus/game_nca_menu.hpp"

#include "ui/nvg_util.hpp"
#include "ui/sidebar.hpp"
#include "ui/option_box.hpp"

#include "yati/nx/ns.hpp"
#include "yati/nx/nca.hpp"
#include "yati/nx/ncm.hpp"
#include "yati/nx/es.hpp"

#include "utils/utils.hpp"

#include "title_info.hpp"
#include "app.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "i18n.hpp"
#include "image.hpp"

#include <cstring>
#include <algorithm>

namespace sphaira::ui::menu::game::meta {
namespace {

#define SYSVER_MAJOR(x)   (((x) >> 26) & 0x003F)
#define SYSVER_MINOR(x)   (((x) >> 20) & 0x003F)
#define SYSVER_MICRO(x)   (((x) >> 16) & 0x003F)
#define SYSVER_RELSTEP(x) (((x) >> 00) & 0xFFFF)

constexpr const char* TICKET_STR[] = {
    [TicketType_None] = "None",
    [TicketType_Common] = "Common",
    [TicketType_Personalised] = "Personalised",
    [TicketType_Missing] = "Missing",
};

constexpr u64 MINI_NACP_OFFSET = offsetof(NacpStruct, display_version);

Result GetMiniNacpFromContentId(NcmContentStorage* cs, const NcmContentMetaKey& key, const NcmContentId& id, MiniNacp& out) {
    u64 program_id;
    fs::FsPath path;
    R_TRY(ncm::GetFsPathFromContentId(cs, key, id, &program_id, &path));

    return nca::ParseControl(path, program_id, &out, sizeof(out), nullptr, MINI_NACP_OFFSET);
}

} // namespace

Menu::Menu(Entry& entry) : MenuBase{entry.GetName(), MenuFlag_None}, m_entry{entry} {
    this->SetActions(
        std::make_pair(Button::L2, Action{"Select"_i18n, [this](){
            // if both set, select all.
            if (App::GetApp()->m_controller.GotHeld(Button::R2)) {
                const auto set = m_selected_count != m_entries.size();

                for (u32 i = 0; i < m_entries.size(); i++) {
                    auto& e = GetEntry(i);
                    if (e.selected != set) {
                        e.selected = set;
                        if (set) {
                            m_selected_count++;
                        } else {
                            m_selected_count--;
                        }
                    }
                }
            } else {
                GetEntry().selected ^= 1;
                if (GetEntry().selected) {
                    m_selected_count++;
                } else {
                    m_selected_count--;
                }
            }
        }}),
        std::make_pair(Button::A, Action{"View Content"_i18n, [this](){
            App::Push<meta_nca::Menu>(m_entry, GetEntry());
        }}),
        std::make_pair(Button::B, Action{"Back"_i18n, [this](){
            SetPop();
        }}),
        std::make_pair(Button::X, Action{"Options"_i18n, [this](){
            auto options = std::make_unique<Sidebar>("Content Options"_i18n, Sidebar::Side::RIGHT);
            ON_SCOPE_EXIT(App::Push(std::move(options)));

            if (!m_entries.empty()) {
                options->Add<SidebarEntryCallback>("Export NSP"_i18n, [this](){
                    DumpGames(false);
                });

                options->Add<SidebarEntryCallback>("Export NSZ"_i18n, [this](){
                    DumpGames(true);
                });

                options->Add<SidebarEntryCallback>("Export options"_i18n, [this](){
                    App::DisplayDumpOptions(false);
                });

                options->Add<SidebarEntryCallback>("Delete"_i18n, [this](){
                    App::Push<OptionBox>(
                        "Are you sure you want to delete the selected entries?"_i18n,
                        "Back"_i18n, "Delete"_i18n, 0, [this](auto op_index){
                            if (op_index && *op_index) {
                                DeleteGames();
                            }
                        }
                    );
                }, true);

                if (ncm::HasRequiredSystemVersion(GetEntry().status.meta_type)) {
                    options->Add<SidebarEntryCallback>("Reset required system version"_i18n, [this](){
                        App::Push<OptionBox>(
                            "Are you sure you want to reset required system version?"_i18n,
                            "Back"_i18n, "Reset"_i18n, 0, [this](auto op_index){
                                if (op_index && *op_index) {
                                    const auto rc = ResetRequiredSystemVersion(GetEntry());
                                    App::PushErrorBox(rc, "Failed to reset required system version"_i18n);
                                }
                            }
                        );
                    });
                }
            }
        }})
    );

    // todo: maybe width is broken here?
    const Vec4 v{485, GetY() + 1.f + 42.f, 720, 60};
    m_list = std::make_unique<List>(1, 8, m_pos, v);

    es::Initialize();
    ON_SCOPE_EXIT(es::Exit());

    // pre-fetch all ticket rights ids.
    es::GetCommonTickets(m_common_tickets);
    es::GetPersonalisedTickets(m_personalised_tickets);

    char subtitle[128];
    std::snprintf(subtitle, sizeof(subtitle), "by %s", entry.GetAuthor());
    SetTitleSubHeading(subtitle);

    Scan();
}

Menu::~Menu() {
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    if (m_dirty) {
        m_dirty = false;
        Scan();
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

    // draw left-side grid background.
    gfx::drawRect(vg, 30, 90, 375, 555, theme->GetColour(ThemeEntryID_GRID));

    // draw the game icon (maybe remove this or reduce it's size).
    const auto& e = m_entries[m_index];
    gfx::drawImage(vg, 90, 130, 256, 256, m_entry.image ? m_entry.image : App::GetDefaultImage());

    nvgSave(vg);
        nvgIntersectScissor(vg, 50, 90, 325, 555);

        char req_vers_buf[128];
        const auto ver = e.content_meta.extened.application.required_system_version;
        switch (e.status.meta_type) {
            case NcmContentMetaType_Application:  std::snprintf(req_vers_buf, sizeof(req_vers_buf), "Required System Version: %u.%u.%u"_i18n.c_str(), SYSVER_MAJOR(ver), SYSVER_MINOR(ver), SYSVER_MICRO(ver)); break;
            case NcmContentMetaType_Patch:        std::snprintf(req_vers_buf, sizeof(req_vers_buf), "Required System Version: %u.%u.%u"_i18n.c_str(), SYSVER_MAJOR(ver), SYSVER_MINOR(ver), SYSVER_MICRO(ver)); break;
            case NcmContentMetaType_AddOnContent: std::snprintf(req_vers_buf, sizeof(req_vers_buf), "Required Application Version: v%u"_i18n.c_str(), ver >> 16); break;
        }

        if (e.missing_count) {
            gfx::drawTextArgs(vg, 50, 415, 18.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "Content Count: %u (%u missing)"_i18n.c_str(), e.content_meta.header.content_count, e.missing_count);
        } else {
            gfx::drawTextArgs(vg, 50, 415, 18.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "Content Count: %u"_i18n.c_str(), e.content_meta.header.content_count);
        }

        gfx::drawTextArgs(vg, 50, 455, 18.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "Ticket: %s"_i18n.c_str(), i18n::get(TICKET_STR[e.ticket_type]).c_str());
        gfx::drawTextArgs(vg, 50, 495, 18.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "Key Generation: %u (%s)"_i18n.c_str(), e.key_gen, nca::GetKeyGenStr(e.key_gen));
        gfx::drawTextArgs(vg, 50, 535, 18.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "%s", req_vers_buf);

        if (e.status.meta_type == NcmContentMetaType_Application || e.status.meta_type == NcmContentMetaType_Patch) {
            gfx::drawTextArgs(vg, 50, 575, 18.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "Display Version: %s"_i18n.c_str(), e.nacp.display_version);
        }
    nvgRestore(vg);

    // exit early if we have no entries (maybe?)
    if (m_entries.empty()) {
        // todo: center this.
        gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Empty..."_i18n.c_str());
        return;
    }

    constexpr float text_xoffset{15.f};

    m_list->Draw(vg, theme, m_entries.size(), [this](auto* vg, auto* theme, auto& v, auto i) {
        const auto& [x, y, w, h] = v;
        auto& e = m_entries[i];

        auto text_id = ThemeEntryID_TEXT;
        if (m_index == i) {
            text_id = ThemeEntryID_TEXT_SELECTED;
            gfx::drawRectOutline(vg, theme, 4.f, v);
        } else {
            if (i != m_entries.size() - 1) {
                gfx::drawRect(vg, x, y + h, w, 1.f, theme->GetColour(ThemeEntryID_LINE_SEPARATOR));
            }
        }

        gfx::drawTextArgs(vg, x + text_xoffset, y + (h / 2.f), 20.f, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, theme->GetColour(text_id), "%s", i18n::get(ncm::GetReadableMetaTypeStr(e.status.meta_type)).c_str());
        gfx::drawTextArgs(vg, x + text_xoffset + 150, y + (h / 2.f), 20.f, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, theme->GetColour(text_id), "%016lX", e.status.application_id);
        gfx::drawTextArgs(vg, x + text_xoffset + 400, y + (h / 2.f), 20.f, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, theme->GetColour(text_id), "v%u (%u)", e.status.version >> 16, e.status.version);

        if (!e.checked) {
            e.checked = true;
            GetNcmSizeOfMetaStatus(e);
        }

        gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f) + 3, 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT_INFO), "%s", i18n::get(ncm::GetReadableStorageIdStr(e.status.storageID)).c_str());
        gfx::drawTextArgs(vg, x + w - text_xoffset, y + (h / 2.f) - 3, 16.f, NVG_ALIGN_RIGHT | NVG_ALIGN_BOTTOM, theme->GetColour(ThemeEntryID_TEXT_INFO), "%s", utils::formatSizeStorage(e.size).c_str());

        if (e.selected) {
            gfx::drawText(vg, x + text_xoffset - 80 / 2, y + (h / 2.f) - (24.f / 2), 24.f, "\uE14B", nullptr, NVG_ALIGN_CENTER | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT_SELECTED));
        }
    });
}

void Menu::SetIndex(s64 index) {
    m_index = index;
    if (!m_index) {
        m_list->SetYoff(0);
    }

    UpdateSubheading();
}

void Menu::Scan() {
    m_dirty = false;
    m_index = 0;
    m_selected_count = 0;
    m_entries.clear();

    // todo: log errors here.
    title::MetaEntries meta_entries;
    if (R_SUCCEEDED(title::GetMetaEntries(m_entry.app_id, meta_entries))) {
        m_entries.reserve(meta_entries.size());
        for (const auto& e : meta_entries) {
            m_entries.emplace_back(e);
        }
    }

    SetIndex(0);
}

void Menu::UpdateSubheading() {
    const auto index = m_entries.empty() ? 0 : m_index + 1;
    this->SetSubHeading(std::to_string(index) + " / " + std::to_string(m_entries.size()));
}

Result Menu::GetNcmSizeOfMetaStatus(MetaEntry& entry) const {
    entry.size = 0;
    entry.missing_count = 0;

    NcmMetaData meta;
    R_TRY(GetNcmMetaFromMetaStatus(entry.status, meta));

    // get the content meta header.
    R_TRY(ncm::GetContentMeta(meta.db, &meta.key, entry.content_meta));

    // fetch all the content infos.
    std::vector<NcmContentInfo> infos;
    R_TRY(ncm::GetContentInfos(meta.db, &meta.key, entry.content_meta.header, infos));

    // calculate the size and fetch the rights id (if possible).
    NcmRightsId rights_id{};
    bool has_nacp{};

    for (const auto& info : infos) {
        u64 size;
        ncmContentInfoSizeToU64(&info, &size);
        entry.size += size;

        // try and load nacp.
        if (!has_nacp && info.content_type == NcmContentType_Control) {
            // try and load from nca.
            if (R_SUCCEEDED(GetMiniNacpFromContentId(meta.cs, meta.key, info.content_id, entry.nacp))) {
                has_nacp = true;
            } else {
                // fallback to ns
                std::vector<u8> buf(sizeof(NsApplicationControlData));
                u64 actual_size;
                if (R_SUCCEEDED(nsGetApplicationControlData(NsApplicationControlSource_Storage, meta.app_id, (NsApplicationControlData*)buf.data(), buf.size(), &actual_size))) {
                    has_nacp = true;
                    std::memcpy(&entry.nacp, buf.data() + MINI_NACP_OFFSET, sizeof(entry.nacp));
                }
            }
        }

        // ensure that we have the content id.
        bool has;
        R_TRY(ncmContentMetaDatabaseHasContent(meta.db, &has, &meta.key, &info.content_id));

        if (!has) {
            entry.missing_count++;
        }

        if (!es::IsRightsIdValid(rights_id.rights_id)) {
            // todo: check if this gets the key gen if standard crypto is used.
            if (R_SUCCEEDED(ncmContentStorageGetRightsIdFromContentId(meta.cs, &rights_id, &info.content_id, FsContentAttributes_All))) {
                entry.key_gen = std::max(entry.key_gen, rights_id.key_generation);
            }
        }
    }

    // if we found a valid rights id, find the ticket type.
    if (es::IsRightsIdValid(rights_id.rights_id)) {
        if (es::IsRightsIdFound(rights_id.rights_id, m_common_tickets)) {
            entry.ticket_type = TicketType_Common;
        } else if (es::IsRightsIdFound(rights_id.rights_id, m_personalised_tickets)) {
            entry.ticket_type = TicketType_Personalised;
        } else {
            entry.ticket_type = TicketType_Missing;
        }
    } else {
        entry.ticket_type = TicketType_None;
    }

    R_SUCCEED();
}

void Menu::DumpGames(bool to_nsz) {
    const auto entries = GetSelectedEntries();
    App::PopToMenu();

    std::vector<NspEntry> nsps;
    BuildNspEntries(m_entry, entries, nsps, to_nsz);

    DumpNsp(nsps, to_nsz);
}

void Menu::DeleteGames() {
    m_dirty = true;
    const auto entries = GetSelectedEntries();
    App::PopToMenu();

    DeleteMetaEntries(m_entry.app_id, m_entry.image, m_entry.GetName(), entries);
}

Result Menu::ResetRequiredSystemVersion(MetaEntry& entry) const {
    entry.checked = false;

    NcmMetaData meta;
    R_TRY(GetNcmMetaFromMetaStatus(entry.status, meta));

    return ncm::SetRequiredSystemVersion(meta.db, &meta.key, 0);
}

} // namespace sphaira::ui::menu::game::meta
