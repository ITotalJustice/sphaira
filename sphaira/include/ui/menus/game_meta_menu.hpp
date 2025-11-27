#pragma once

#include "ui/menus/menu_base.hpp"
#include "ui/menus/game_menu.hpp"
#include "ui/list.hpp"
#include "yati/nx/ncm.hpp"
#include <span>
#include <memory>

namespace sphaira::ui::menu::game::meta {

enum TicketType : u8 {
    TicketType_None,
    TicketType_Common,
    TicketType_Personalised,
    TicketType_Missing,
};

struct MiniNacp {
    char display_version[0x10];
};

struct MetaEntry {
    NsApplicationContentMetaStatus status{};
    ncm::ContentMeta content_meta{};
    // small version of nacp to speed up loading.
    MiniNacp nacp{};
    // total size of all ncas.
    s64 size{};
    // set to the key gen (if possible), only if title key encrypted.
    u8 key_gen{};
    // set to the ticket type.
    u8 ticket_type{TicketType_None};
    // set if it has missing ncas.
    u8 missing_count{};
    // set if selected.
    bool selected{};
    // set if we have checked the above meta data.
    bool checked{};
};

struct Menu final : MenuBase {
    Menu(Entry& entry);
    ~Menu();

    auto GetShortTitle() const -> const char* override { return "Meta"; };
    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;

private:
    void SetIndex(s64 index);
    void Scan();
    void UpdateSubheading();

    auto GetSelectedEntries() const {
        title::MetaEntries out;
        for (auto& e : m_entries) {
            if (e.selected) {
                out.emplace_back(e.status);
            }
        }

        if (!m_entries.empty() && out.empty()) {
            out.emplace_back(m_entries[m_index].status);
        }

        return out;
    }

    void ClearSelection() {
        for (auto& e : m_entries) {
            e.selected = false;
        }

        m_selected_count = 0;
    }

    auto GetEntry(u32 index) -> MetaEntry& {
        return m_entries[index];
    }

    auto GetEntry(u32 index) const -> const MetaEntry& {
        return m_entries[index];
    }

    auto GetEntry() -> MetaEntry& {
        return GetEntry(m_index);
    }

    auto GetEntry() const -> const MetaEntry& {
        return GetEntry(m_index);
    }

    void DumpGames(bool to_nsz);
    void DeleteGames();
    Result ResetRequiredSystemVersion(MetaEntry& entry) const;
    Result GetNcmSizeOfMetaStatus(MetaEntry& entry) const;

private:
    Entry& m_entry;
    std::vector<MetaEntry> m_entries{};
    s64 m_index{};
    s64 m_selected_count{};
    std::unique_ptr<List> m_list{};
    bool m_dirty{};

    std::vector<FsRightsId> m_common_tickets{};
    std::vector<FsRightsId> m_personalised_tickets{};
};

} // namespace sphaira::ui::menu::game::meta
