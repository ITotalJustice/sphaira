#pragma once

#include "ui/menus/menu_base.hpp"
#include "ui/list.hpp"
#include "fs.hpp"
#include "option.hpp"
#include <vector>
#include <string>

namespace sphaira::ui::menu::gh {

struct AssetEntry {
    std::string name{};
    std::string path{};
    std::string pre_install_message{};
    std::string post_install_message{};
};

struct Entry {
    fs::FsPath json_path{};
    std::string url{};
    std::string owner{};
    std::string repo{};
    std::string tag{};
    std::string pre_install_message{};
    std::string post_install_message{};
    std::vector<AssetEntry> assets{};
};

struct GhApiAsset {
    std::string name{};
    std::string content_type{};
    u64 size{};
    u64 download_count{};
    std::string updated_at{};
    std::string browser_download_url{};
};

struct GhApiEntry {
    std::string tag_name{};
    std::string name{};
    std::string published_at{};
    bool prerelease{};
    std::vector<GhApiAsset> assets{};
};

struct Menu final : MenuBase {
    Menu(u32 flags);
    ~Menu();

    auto GetShortTitle() const -> const char* override { return "GitHub"; };
    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

private:
    void SetIndex(s64 index);
    void Scan();
    void LoadEntriesFromPath(const fs::FsPath& path);

    auto GetEntry() -> Entry& {
        return m_entries[m_index];
    }

    auto GetEntry() const -> const Entry& {
        return m_entries[m_index];
    }

    void Sort();
    void UpdateSubheading();

private:
    std::vector<Entry> m_entries{};
    s64 m_index{};
    std::unique_ptr<List> m_list{};
};

// creates a popup box on another thread.
void DownloadEntries(const Entry& entry);

// parses the params into entry struct and calls DonwloadEntries
bool Download(const std::string& url, const std::vector<AssetEntry>& assets = {}, const std::string& tag = {}, const std::string& pre_install_message = {}, const std::string& post_install_message = {});

// calls the above function by pushing the asset to an array.
inline bool Download(const std::string& url, const AssetEntry& asset, const std::string& tag = {}, const std::string& pre_install_message = {}, const std::string& post_install_message = {}) {
    std::vector<AssetEntry> assets;
    assets.emplace_back(asset);

    return Download(url, assets, tag, pre_install_message, post_install_message);
}

} // namespace sphaira::ui::menu::gh
