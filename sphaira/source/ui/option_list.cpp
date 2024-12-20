#include "ui/option_list.hpp"
#include "app.hpp"
#include "ui/nvg_util.hpp"
#include "i18n.hpp"

namespace sphaira::ui {

OptionList::OptionList(Options options)
: m_options{std::move(options)} {
    SetAction(Button::A, Action{"Select"_i18n, [this](){
        const auto& [_, func] = m_options[m_index];
        func();
        SetPop();
    }});

    SetAction(Button::B, Action{"Back"_i18n, [this](){
        SetPop();
    }});
}

auto OptionList::Update(Controller* controller, TouchInfo* touch) -> void {

}

auto OptionList::OnLayoutChange() -> void {

}

auto OptionList::Draw(NVGcontext* vg, Theme* theme) -> void {

}

} // namespace sphaira::ui
