#pragma once

#include "ui/menus/menu_base.hpp"
#include "ui/scrollable_text.hpp"
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

// all commented out entries are those that we don't query for.
// this saves time not only processing the json, but also the download
// of said json.
// by reducing the fields to only what we need, the size is 4-5x smaller.

struct Creator {
    std::string id;
    std::string display_name;
};

struct Details {
    std::string name;
    // std::string description;
};

struct Preview {
    // std::string original;
    std::string thumb;
    LazyImage lazy_image;
};

struct DownloadPack {
    std::string filename;
    std::string url;
    std::string mimetype;
};

using DownloadTheme = DownloadPack;

struct ThemeEntry {
    std::string id;
    // Creator creator;
    // Details details;
    // std::string last_updated;
    // u64 dl_count;
    // u64 like_count;
    // std::vector<std::string> categories;
    // std::string target;
    Preview preview;
};

// struct Pack {
//     std::string id;
//     Creator creator;
//     Details details;
//     std::string last_updated;
//     std::vector<std::string> categories;
//     u64 dl_count;
//     u64 like_count;
//     std::vector<ThemeEntry> themes;
// };

struct PackListEntry {
    std::string id;
    Creator creator;
    Details details;
    // std::string last_updated;
    // std::vector<std::string> categories;
    // u64 dl_count;
    // u64 like_count;
    std::vector<ThemeEntry> themes;
};

struct Pagination {
    u64 page;
    u64 limit;
    u64 page_count;
    u64 item_count;
};

struct PackList {
    std::vector<PackListEntry> packList;
    Pagination pagination;
};

struct Config {
    // these index into a string array
    u32 target_index{};
    u32 sort_index{};
    u32 order_index{};
    // search query, if empty, its not used
    std::string query;
    // this is actually an array of creator ids, but we don't support that feature
    // if empty, its not used
    std::string creator;
    // defaults
    u32 page{1};
    u32 limit{18};
    bool nsfw{false};

    void SetQuery(std::string new_query) {
        query = new_query;
    }

    void RemoveQuery() {
        query.clear();
    }

    void SetCreator(Creator new_creator) {
        creator = new_creator.id;
    }

    void RemoveCreator() {
        creator.clear();
    }
};

struct Menu; // fwd

struct PageEntry {
    std::vector<PackListEntry> m_packList;
    Pagination m_pagination{};
    PageLoadState m_ready{PageLoadState::None};
};

struct Menu final : MenuBase {
    Menu();
    ~Menu();

    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

    void SetIndex(std::size_t index) {
        m_index = index;
    }

    // void SetSearch(const std::string& term);
    // void SetAuthor();

    void InvalidateAllPages();
    void PackListDownload();
    void OnPackListDownload();

private:
    static constexpr inline const char* INI_SECTION = "themezer";
    static constexpr inline u32 MAX_ON_PAGE = 16; // same as website

    std::vector<PageEntry> m_pages;
    std::size_t m_page_index{};
    std::size_t m_page_index_max{1};

    std::string m_search{};

    std::size_t m_start{};
    std::size_t m_index{}; // where i am in the array

    // options
    option::OptionLong m_sort{INI_SECTION, "sort", 0};
    option::OptionLong m_order{INI_SECTION, "order", 0};
    option::OptionBool m_nsfw{INI_SECTION, "nsfw", false};
};

} // namespace sphaira::ui::menu::themezer
