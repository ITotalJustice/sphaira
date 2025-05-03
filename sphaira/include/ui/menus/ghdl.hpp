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
    std::string browser_download_url{};
};

struct GhApiEntry {
    std::string tag_name{};
    std::string name{};
    std::vector<GhApiAsset> assets{};
};

struct Menu final : MenuBase {
    Menu();
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

} // namespace sphaira::ui::menu::gh
