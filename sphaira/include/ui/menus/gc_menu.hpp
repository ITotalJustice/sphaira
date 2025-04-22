#pragma once

#include "ui/menus/menu_base.hpp"
#include "yati/container/base.hpp"
#include "yati/source/base.hpp"

namespace sphaira::ui::menu::gc {

enum class State {
    // no gamecard inserted.
    None,
    // set whilst transfer is in progress.
    Progress,
    // set when the transfer is finished.
    Done,
    // set when no gamecard is inserted.
    NotFound,
    // failed to parse gamecard.
    Failed,
};

struct Menu final : MenuBase {
    Menu();
    ~Menu();

    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;

    Result ScanGamecard();

private:
    std::unique_ptr<fs::FsNativeGameCard> m_fs{};
    FsDeviceOperator m_dev_op{};
    yati::container::Collections m_collections{};
    State m_state{State::None};
};

} // namespace sphaira::ui::menu::gc
