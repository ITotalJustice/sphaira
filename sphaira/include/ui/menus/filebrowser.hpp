#pragma once

#include "ui/menus/menu_base.hpp"
#include "ui/scrolling_text.hpp"
#include "ui/progress_box.hpp"
#include "ui/list.hpp"
#include "fs.hpp"
#include "option.hpp"
#include "hasher.hpp"
// #include <optional>
#include <span>

namespace sphaira::ui::menu::filebrowser {

enum FsEntryFlag {
    FsEntryFlag_None,
    // write protected.
    FsEntryFlag_ReadOnly = 1 << 0,
    // supports file assoc.
    FsEntryFlag_Assoc = 1 << 1,
};

enum class FsType {
    Sd,
    ImageNand,
    ImageSd,
    Stdio,
};

enum class SelectedType {
    None,
    Copy,
    Cut,
    Delete,
};

enum class ViewSide {
    Left,
    Right,
};

enum SortType {
    SortType_Size,
    SortType_Alphabetical,
};

enum OrderType {
    OrderType_Descending,
    OrderType_Ascending,
};

struct FsEntry {
    fs::FsPath name{};
    fs::FsPath root{};
    FsType type{};
    u32 flags{FsEntryFlag_None};

    auto IsReadOnly() const -> bool {
        return flags & FsEntryFlag_ReadOnly;
    }

    auto IsAssoc() const -> bool {
        return flags & FsEntryFlag_Assoc;
    }

    auto IsSame(const FsEntry& e) const {
        return root == e.root && type == e.type;
    }
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

void SignalChange();

struct Menu;

struct FsView final : Widget {
    friend class Menu;

    FsView(Menu* menu, ViewSide side);
    FsView(Menu* menu, const fs::FsPath& path, const FsEntry& entry, ViewSide side);
    ~FsView();

    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

    static auto GetNewPath(const fs::FsPath& root_path, const fs::FsPath& file_path) -> fs::FsPath {
        return fs::AppendPath(root_path, file_path);
    }

    auto GetFs() {
        return m_fs.get();
    }

    auto& GetFsEntry() const {
        return m_fs_entry;
    }

    void SetSide(ViewSide side);

    static Result DeleteAllCollections(ProgressBox* pbox, fs::Fs* fs, const FsDirCollections& collections, u32 mode = FsDirOpenMode_ReadDirs|FsDirOpenMode_ReadFiles);
    static auto get_collection(fs::Fs* fs, const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollection& out, bool inc_file, bool inc_dir, bool inc_size) -> Result;
    static auto get_collections(fs::Fs* fs, const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollections& out, bool inc_size = false) -> Result;

private:
    void SetIndex(s64 index);
    void InstallForwarder();

    void InstallFiles();
    void UnzipFiles(fs::FsPath folder);
    void ZipFiles(fs::FsPath zip_path);
    void UploadFiles();

    auto Scan(const fs::FsPath& new_path, bool is_walk_up = false) -> Result;

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

    auto IsSd() const -> bool {
        return m_fs_entry.type == FsType::Sd;
    }

    void Sort();
    void SortAndFindLastFile(bool scan = false);
    void SetIndexFromLastFile(const LastFile& last_file);

    void OnDeleteCallback();
    void OnPasteCallback();
    void OnRenameCallback();
    auto CheckIfUpdateFolder() -> Result;

    auto get_collection(const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollection& out, bool inc_file, bool inc_dir, bool inc_size) -> Result;
    auto get_collections(const fs::FsPath& path, const fs::FsPath& parent_name, FsDirCollections& out, bool inc_size = false) -> Result;

    void SetFs(const fs::FsPath& new_path, const FsEntry& new_entry);

    auto GetNative() -> fs::FsNative* {
        return (fs::FsNative*)m_fs.get();
    }

    void DisplayHash(hash::Type type);

    void DisplayOptions();
    void DisplayAdvancedOptions();

private:
    Menu* m_menu{};
    ViewSide m_side{};

    std::unique_ptr<fs::Fs> m_fs{};
    FsEntry m_fs_entry{};
    fs::FsPath m_path{};
    std::vector<FileEntry> m_entries{};
    std::vector<u32> m_entries_index{}; // files not including hidden
    std::vector<u32> m_entries_index_hidden{}; // includes hidden files
    std::vector<u32> m_entries_index_search{}; // files found via search
    std::span<u32> m_entries_current{};

    std::unique_ptr<List> m_list{};
    std::optional<fs::FsPath> m_daybreak_path{};

    // this keeps track of the highlighted file before opening a folder
    // if the user presses B to go back to the previous dir
    // this vector is popped, then, that entry is checked if it still exists
    // if it does, the index becomes that file.
    std::vector<LastFile> m_previous_highlighted_file{};
    s64 m_index{};
    s64 m_selected_count{};
    ScrollingText m_scroll_name{};

    bool m_is_update_folder{};
};

// contains all selected files for a command, such as copy, delete, cut etc.
struct SelectedStash {
    void Add(std::shared_ptr<FsView> view, SelectedType type, const std::vector<FileEntry>& files, const fs::FsPath& path) {
        if (files.empty()) {
            Reset();
        } else {
            m_view = view;
            m_type = type;
            m_files = files;
            m_path = path;
        }
    }

    auto SameFs(FsView* view) -> bool {
        if (m_view && view->GetFsEntry().IsSame(m_view->GetFsEntry())) {
            return true;
        } else {
            return false;
        }
    }

    auto Type() const -> SelectedType {
        return m_type;
    }

    auto Empty() const -> bool {
        return m_files.empty();
    }

    void Reset() {
        m_view = {};
        m_type = {};
        m_files = {};
        m_path = {};
    }

// private:
    std::shared_ptr<FsView> m_view{};
    std::vector<FileEntry> m_files{};
    fs::FsPath m_path{};
    SelectedType m_type{SelectedType::None};
};

struct Menu final : MenuBase {
    friend class FsView;

    Menu(u32 flags);
    ~Menu();

    auto GetShortTitle() const -> const char* override { return "Files"; };
    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

    static auto GetNewPath(const fs::FsPath& root_path, const fs::FsPath& file_path) -> fs::FsPath {
        return fs::AppendPath(root_path, file_path);
    }

private:
    auto IsSplitScreen() const {
        return m_split_screen;
    }

    void SetSplitScreen(bool enable);

    void RefreshViews();

    void LoadAssocEntriesPath(const fs::FsPath& path);
    void LoadAssocEntries();
    auto FindFileAssocFor() -> std::vector<FileAssocEntry>;

    void AddSelectedEntries(SelectedType type) {
        auto entries = view->GetSelectedEntries();
        if (entries.empty()) {
            return;
        }

        m_selected.Add(view, type, entries, view->m_path);
    }

    void ResetSelection() {
        m_selected.Reset();
    }

    void UpdateSubheading();

    void PromptIfShouldExit();

private:
    static constexpr inline const char* INI_SECTION = "filebrowser";

    std::shared_ptr<FsView> view{};
    std::shared_ptr<FsView> view_left{};
    std::shared_ptr<FsView> view_right{};

    std::vector<FileAssocEntry> m_assoc_entries{};
    SelectedStash m_selected{};

    // this keeps track of the highlighted file before opening a folder
    // if the user presses B to go back to the previous dir
    // this vector is popped, then, that entry is checked if it still exists
    // if it does, the index becomes that file.
    std::vector<LastFile> m_previous_highlighted_file{};
    s64 m_index{};
    s64 m_selected_count{};

    option::OptionLong m_sort{INI_SECTION, "sort", SortType::SortType_Alphabetical};
    option::OptionLong m_order{INI_SECTION, "order", OrderType::OrderType_Descending};
    option::OptionBool m_show_hidden{INI_SECTION, "show_hidden", false};
    option::OptionBool m_folders_first{INI_SECTION, "folders_first", true};
    option::OptionBool m_hidden_last{INI_SECTION, "hidden_last", false};
    option::OptionBool m_ignore_read_only{INI_SECTION, "ignore_read_only", false};

    bool m_loaded_assoc_entries{};
    bool m_is_update_folder{};
    bool m_split_screen{};
};

} // namespace sphaira::ui::menu::filebrowser
