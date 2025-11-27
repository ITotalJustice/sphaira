#if 0
#pragma once

#include "ui/menus/menu_base.hpp"
#include "ui/scrollable_text.hpp"
#include "ui/scrolling_text.hpp"
#include "ui/list.hpp"
#include "option.hpp"
#include <span>

namespace sphaira::ui::menu::themezer {

enum class ImageDownloadState {
    None, // not started
    Progress, // Download started
    Done, // finished downloading
    Failed, // attempted to download but failed
};

struct LazyImage {
    ~LazyImage();
    int image{};
    int w{}, h{};
    bool tried_cache{};
    bool cached{};
    ImageDownloadState state{ImageDownloadState::None};
};

enum MenuState {
    MenuState_Normal,
    MenuState_Search,
    MenuState_Creator,
};

enum ListType {
    ListType_Pack, // list complete packs
    ListType_Target, // list types
};

enum class PageLoadState {
    None,
    Loading,
    Done,
    Error,
};

struct Creator {
    std::string id{};
    std::string display_name{};
};

struct Details {
    std::string name{};
};

struct Preview {
    std::string thumb{};
    LazyImage lazy_image{};
};

struct DownloadPack {
    std::string filename{};
    std::string url{};
    std::string mimetype{};
};

using DownloadTheme = DownloadPack;

struct ThemeEntry {
    std::string id{};
    Preview preview{};
};

struct PackListEntry {
    std::string id{};
    Creator creator{};
    Details details{};
    std::vector<ThemeEntry> themes{};
};

struct Pagination {
    u64 page{};
    u64 limit{};
    u64 page_count{};
    u64 item_count{};
};

struct PackList {
    std::vector<PackListEntry> packList{};
    Pagination pagination{};
};

struct Config {
    // these index into a string array
    u32 target_index{};
    u32 sort_index{};
    u32 order_index{};
    // search query, if empty, its not used
    std::string query{};
    // this is actually an array of creator ids, but we don't support that feature
    // if empty, its not used
    std::string creator{};
    // defaults
    u32 page{1};
    u32 limit{18};
    bool nsfw{false};

    void SetQuery(const std::string& new_query) {
        query = new_query;
    }

    void RemoveQuery() {
        query.clear();
    }

    void SetCreator(const Creator& new_creator) {
        creator = new_creator.id;
    }

    void RemoveCreator() {
        creator.clear();
    }
};

struct Menu; // fwd

struct PageEntry {
    std::vector<PackListEntry> m_packList{};
    Pagination m_pagination{};
    PageLoadState m_ready{PageLoadState::None};
};

struct Menu final : MenuBase {
    Menu(u32 flags);
    ~Menu();

    auto GetShortTitle() const -> const char* override { return "Themezer"; };
    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

private:
    void SetIndex(s64 index) {
        m_index = index;
        if (!m_index) {
            m_list->SetYoff(0);
        }
    }

    void InvalidateAllPages();
    void PackListDownload();
    void DisplayOptions();

private:
    static constexpr inline const char* INI_SECTION = "themezer";
    static constexpr inline u32 MAX_ON_PAGE = 16; // same as website

    std::vector<PageEntry> m_pages{};
    s64 m_page_index{};
    s64 m_page_index_max{1};

    std::string m_search{};

    s64 m_index{}; // where i am in the array
    std::unique_ptr<List> m_list{};

    ScrollingText m_scroll_name{};
    ScrollingText m_scroll_author{};

    // options
    option::OptionLong m_sort{INI_SECTION, "sort", 0};
    option::OptionLong m_order{INI_SECTION, "order", 0};
    option::OptionBool m_nsfw{INI_SECTION, "nsfw", false};

    bool m_checked_for_nro{};
};

} // namespace sphaira::ui::menu::themezer
#endif
