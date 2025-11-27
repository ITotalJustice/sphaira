#include "ui/menus/file_picker.hpp"
#include "i18n.hpp"

namespace sphaira::ui::menu::filebrowser::picker {

Menu::Menu(const Callback& cb, const std::vector<std::string>& filter, const fs::FsPath& path)
: Base{MenuFlag_None, FsOption_Picker}
, m_callback{cb} {
    SetFilter(filter);
    SetTitle("File Picker"_i18n);
}

void Menu::OnClick(FsView* view, const FsEntry& fs_entry, const FileEntry& entry, const fs::FsPath& path) {
    if (entry.type == FsDirEntryType_Dir) {
        view->Scan(path);
    } else {
        for (auto& e : m_filter) {
            if (IsExtension(e, entry.GetExtension())) {
                if (m_callback(path)) {
                    SetPop();
                }
                break;
            }
        }
    }
}

} // namespace sphaira::ui::menu::filebrowser::picker
