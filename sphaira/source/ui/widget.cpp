#include "ui/widget.hpp"
#include "ui/nvg_util.hpp"
#include "app.hpp"

namespace sphaira::ui {

void Widget::Update(Controller* controller, TouchInfo* touch) {
    for (const auto& [button, action] : m_actions) {
        if ((action.m_type & ActionType::DOWN) && controller->GotDown(button)) {
            if (static_cast<u64>(button) & static_cast<u64>(Button::ANY_BUTTON)) {
                App::PlaySoundEffect(SoundEffect_Focus);
            }
            action.Invoke(true);
        }
        else if ((action.m_type & ActionType::UP) && controller->GotUp(button)) {
            action.Invoke(false);
        }
        else if ((action.m_type & ActionType::HELD) && controller->GotHeld(button)) {
            action.Invoke(true);
        }
    }
}

void Widget::Draw(NVGcontext* vg, Theme* theme) {
    Actions draw_actions;

    for (const auto& [button, action] : m_actions) {
        if (!action.IsHidden()) {
            draw_actions.emplace(button, action);
        }
    }

    gfx::drawButtons(vg, draw_actions, theme->elements[ThemeEntryID_TEXT].colour);
}

auto Widget::HasAction(Button button) const -> bool {
    return m_actions.contains(button);
}

void Widget::SetAction(Button button, Action action) {
    m_actions.insert_or_assign(button, action);
}

void Widget::RemoveAction(Button button) {
    if (auto it = m_actions.find(button); it != m_actions.end()) {
        m_actions.erase(it);
    }
}

} // namespace sphaira::ui
