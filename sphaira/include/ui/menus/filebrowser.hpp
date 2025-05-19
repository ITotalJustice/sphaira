#pragma once

#include "ui/menus/menu_base.hpp"
#include "ui/list.hpp"
#include "nro.hpp"
#include "fs.hpp"
#include "option.hpp"
// #include <optional>
#include <span>

namespace sphaira::ui::menu::filebrowser {

enum class FsType {
    Sd,
    ImageNand,
    ImageSd,
};

enum class SelectedType {
    None,
    Copy,
    Cut,
    Delete,
};

enum SortType {
    SortType_Size,
    SortType_Alphabetical,
};

enum OrderType {
    OrderType_Descending,
    OrderType_Ascending,
};

// roughly 1kib in size per entry
struct FileEntry : FsDirectoryEntry {
    std::string extension{}; // if any
    std::string internal_name{}; // if any
    std::string internal_extension{}; // if any
    s64 file_count{-1}; // number of files in a folder, non-recursive
    s64 dir_count{-1}; // number folders in a folder, non-recursive
    FsTimeStampRaw time_stamp{};
    bool checked_extension{}; // did we already search for an ext?
    bool checked_internal_extension{}; // did we already search for an ext?
    bool selected{}; // is this file selected?

    auto IsFile() const -> bool {
        return type == FsDirEntryType_File;
    }

    auto IsDir() const -> bool {
        return !IsFile();
    }

    auto IsHidden() const -> bool {
        return name[0] == '.';
    }

    auto GetName() const -> std::string {
        return name;
    }

    auto GetExtension() const -> std::string {
        return extension;
    }

    auto GetInternalName() const -> std::string {
        if (!internal_name.empty()) {
            return internal_name;
        }
        return GetName();
    }

    auto GetInternalExtension() const -> std::string {
        if (!internal_extension.empty()) {
            return internal_extension;
        }
        return GetExtension();
    }

    auto IsSelected() const -> bool {
        return selected;
    }
};

struct FileAssocEntry {
    fs::FsPath path{}; // ini name
    std::string name{}; // ini name
    std::vector<std::string> ext{}; // list of ext
    std::vector<std::string> database{}; // list of systems
    bool use_base_name{}; // if set, uses base name (rom.zip) otherwise uses internal name (rom.gba)

    auto IsExtension(std::string_view extension, std::string_view internal_extension) const -> bool {
        for (const auto& assoc_ext : ext) {
            if (extension.length() == assoc_ext.length() && !strncasecmp(assoc_ext.data(), extension.data(), assoc_ext.length())) {
                return true;
            }
            if (internal_extension.length() == assoc_ext.length() && !strncasecmp(assoc_ext.data(), internal_extension.data(), assoc_ext.length())) {
                return true;
            }
        }
        return false;
    }
};

struct LastFile {
    fs::FsPath name{};
    s64 index{};
    float offset{};
    s64 entries_count{};
};

struct FsDirCollection {
    fs::FsPath path{};
    fs::FsPath parent_name{};
    std::vector<FsDirectoryEntry> files{};
    std::vector<FsDirectoryEntry> dirs{};
};

using FsDirCollections = std::vector<FsDirCollection>;

struct Menu final : MenuBase {
    Menu(const std::vector<NroEntry>& nro_entries);
    ~Menu();

    auto GetShortTitle() const -> const char* override { return "Files"; };
    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

    static auto GetNewPath(const fs::FsPath& root_path, const fs::FsPath& file_path) -> fs::FsPath {
        return fs::AppendPath(root_path, file_path);
    }

private:
    void SetIndex(s64 index);
    void InstallForwarder();

    void InstallFiles();
    void UnzipFiles(fs::FsPath folder);
    void ZipFiles(fs::FsPath zip_path);
    void UploadFiles();

    auto Scan(const fs::FsPath& new_path, bool is_walk_up = false) -> Result;

    void LoadAssocEntriesPath(const fs::FsPath& path);
    void LoadAssocEntries();
    auto FindFileAssocFor() -> std::vector<FileAssocEntry>;

    auto GetNewPath(const FileEntry& entry) const -> fs::FsPath {
        return GetNewPath(m_path, entry.name);
    }

    auto GetNewPath(s64 index) const -> fs::FsPath {
        return GetNewPath(m_path, GetEntry(index).name);
    }

    auto GetNewPathCurrent() const -> fs::FsPath {
        return GetNewPath(m_index);
    }

    auto GetSelectedEntries() const -> std::vector<FileEntry> {
        std::vector<FileEntry> out;

        if (!m_selected_count) {
            out.emplace_back(GetEntry());
        } else {
            for (auto&e : m_entries) {
                if (e.IsSelected()) {
                    out.emplace_back(e);
                }
            }
        }

        return out;
    }

    void AddSelectedEntries(SelectedType type) {
        auto entries = GetSelectedEntries();
        if (entries.empty()) {
            // log_write("%s with no selected files\n", __PRETTY_FUNCTION__);
            return;
        }

        m_selected_type = type;
        m_selected_files = entries;
        m_selected_path = m_path;
    }

    void ResetSelection() {
        m_selected_files.clear();
        m_selected_count = 0;
        m_selected_type = SelectedType::None;
        m_selected_path = {};
    }

    auto HasTypeInSelectedEntries(FsDirEntryType type) const -> bool {
        if (!m_selected_count) {
            return GetEntry().type == type;
        } else {
            for (auto&p : m_selected_files) {
                if (p.type == type) {
                    return true;
                }
            }

            return false;
        }
    }

    auto GetEntry(u32 index) -> FileEntry& {
        return m_entries[m_entries_current[index]];
    }

    auto GetEntry(u32 index) const -> const FileEntry& {
        return m_entries[m_entries_current[index]];
    }

    auto GetEntry() -> FileEntry& {
        return GetEntry(m_index);
    }

    auto GetEntry() const -> const FileEntry& {
        return GetEntry(m_index);
    }

    void Sort();
    void SortAndFindLastFile();
    void SetIndexFromLastFile(const LastFile& last_file);
    void UpdateSubheading();

    void OnDeleteCallback();
    void OnPasteCallback();
    void OnRenameCallback();
    auto CheckIfUpdateFolder() -> Result;

    auto get_collection(const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollection& out, bool inc_file, bool inc_dir, bool inc_size) -> Result;
    auto get_collections(const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollections& out) -> Result;

    void SetFs(const fs::FsPath& new_path, u32 new_type);

private:
    static constexpr inline const char* INI_SECTION = "filebrowser";

    const std::vector<NroEntry>& m_nro_entries;
    std::unique_ptr<fs::FsNative> m_fs{};
    FsType m_fs_type{};
    fs::FsPath m_path{};
    std::vector<FileEntry> m_entries{};
    std::vector<u32> m_entries_index{}; // files not including hidden
    std::vector<u32> m_entries_index_hidden{}; // includes hidden files
    std::vector<u32> m_entries_index_search{}; // files found via search
    std::span<u32> m_entries_current{};

    std::unique_ptr<List> m_list{};
    std::optional<fs::FsPath> m_daybreak_path{};

    // search options
    // show files [X]
    // show folders [X]
    // recursive (slow) [ ]

    std::vector<FileAssocEntry> m_assoc_entries{};
    std::vector<FileEntry> m_selected_files{};

    // this keeps track of the highlighted file before opening a folder
    // if the user presses B to go back to the previous dir
    // this vector is popped, then, that entry is checked if it still exists
    // if it does, the index becomes that file.
    std::vector<LastFile> m_previous_highlighted_file{};
    fs::FsPath m_selected_path{};
    s64 m_index{};
    s64 m_selected_count{};
    SelectedType m_selected_type{SelectedType::None};

    option::OptionLong m_sort{INI_SECTION, "sort", SortType::SortType_Alphabetical};
    option::OptionLong m_order{INI_SECTION, "order", OrderType::OrderType_Descending};
    option::OptionBool m_show_hidden{INI_SECTION, "show_hidden", false};
    option::OptionBool m_folders_first{INI_SECTION, "folders_first", true};
    option::OptionBool m_hidden_last{INI_SECTION, "hidden_last", false};
    option::OptionBool m_ignore_read_only{INI_SECTION, "ignore_read_only", false};
    option::OptionLong m_mount{INI_SECTION, "mount", 0};

    bool m_loaded_assoc_entries{};
    bool m_is_update_folder{};
};

} // namespace sphaira::ui::menu::filebrowser
