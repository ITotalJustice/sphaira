#pragma once

#include "ui/menus/filebrowser.hpp"

namespace sphaira::ui::menu::filebrowser::picker {

using Callback = std::function<bool(const fs::FsPath& path)>;

struct Menu final : Base {
    explicit Menu(const Callback& cb, const std::vector<std::string>& filter = {}, const fs::FsPath& path = {});

private:
    void OnClick(FsView* view, const FsEntry& fs_entry, const FileEntry& entry, const fs::FsPath& path) override;

private:
    const Callback m_callback;
};

} // namespace sphaira::ui::menu::filebrowser::picker
