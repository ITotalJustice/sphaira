#pragma once

#include "ui/widget.hpp"
#include "nro.hpp"
#include <string>

namespace sphaira::ui::menu {

struct MenuBase : Widget {
    MenuBase(std::string title);
    virtual ~MenuBase();

    virtual void Update(Controller* controller, TouchInfo* touch);
    virtual void Draw(NVGcontext* vg, Theme* theme);

    auto IsMenu() const -> bool override {
        return true;
    }

    void SetTitle(std::string title);
    void SetTitleSubHeading(std::string sub_heading);
    void SetSubHeading(std::string sub_heading);

private:
    std::string m_title;
    std::string m_title_sub_heading;
    std::string m_sub_heading;
    AppletType m_applet_type;
};

} // namespace sphaira::ui::menu
