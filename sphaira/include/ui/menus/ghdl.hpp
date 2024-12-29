#pragma once

#include "ui/menus/menu_base.hpp"
#include "fs.hpp"
#include "option.hpp"
#include <vector>
#include <string>

namespace sphaira::ui::menu::gh {

struct AssetEntry {
    std::string name;
    std::string path;
};

struct Entry {
    fs::FsPath json_path;
    std::string owner;
    std::string repo;
    std::vector<AssetEntry> assets;
};

struct GhApiAsset {
    std::string name;
    std::string content_type;
    u64 size;
    u64 download_count;
    std::string browser_download_url;
};

struct GhApiEntry {
    std::string tag_name;
    std::string name;
    std::vector<GhApiAsset> assets;
};

struct Menu final : MenuBase {
    Menu();
    ~Menu();

    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

private:
    void SetIndex(std::size_t index);
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
    std::vector<Entry> m_entries;
    std::size_t m_index{};
    std::size_t m_index_offset{};
};

} // namespace sphaira::ui::menu::gh
