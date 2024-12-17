#pragma once

#include "ui/object.hpp"
#include <vector>
#include <memory>
#include <map>
#include <unordered_map>

namespace sphaira::ui {

struct Widget : public Object {
    virtual ~Widget() = default;

    virtual void Update(Controller* controller, TouchInfo* touch);
    virtual void Draw(NVGcontext* vg, Theme* theme);

    virtual void OnFocusGained() {
        m_focus = true;
    }

    virtual void OnFocusLost() {
        m_focus = false;
    }

    virtual auto HasFocus() const -> bool {
        return m_focus;
    }

    auto HasAction(Button button) const -> bool;
    void SetAction(Button button, Action action);
    void SetActions(std::same_as<std::pair<Button, Action>> auto ...args) {
        const std::array list = {args...};
        for (const auto& [button, action] : list) {
            SetAction(button, action);
        }
    }

    auto GetActions() const {
        return m_actions;
    }

    void RemoveAction(Button button);

    void RemoveActions() {
        m_actions.clear();
    }

    void SetPop(bool pop = true) {
        m_pop = pop;
    }

    auto ShouldPop() const -> bool {
        return m_pop;
    }

    using Actions = std::map<Button, Action>;
    // using Actions = std::unordered_map<Button, Action>;
    Actions m_actions;
    bool m_focus{false};
    bool m_pop{false};
};

} // namespace sphaira::ui
