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
    void UpdateVars();

private:
    std::string m_title{};
    std::string m_title_sub_heading{};
    std::string m_sub_heading{};

protected:
    struct tm m_tm{};
    TimeStamp m_poll_timestamp{};
    u32 m_battery_percetange{};
    PsmChargerType m_charger_type{};
    NifmInternetConnectionType m_type{};
    NifmInternetConnectionStatus m_status{};
    u32 m_strength{};
    u32 m_ip{};
};

} // namespace sphaira::ui::menu
