#pragma once

#include "ui/menus/menu_base.hpp"
#include <span>

namespace sphaira::ui::menu::irs {

enum Rotation {
    Rotation_0,
    Rotation_90,
    Rotation_180,
    Rotation_270,
};

enum Colour {
    Colour_Grey,
    Colour_Ironbow,
    Colour_Green,
    Colour_Red,
    Colour_Blue,
};

struct Entry {
    IrsIrCameraHandle m_handle{};
    IrsIrCameraStatus status{};
    bool m_update_needed{};
};

struct Menu final : MenuBase {
    Menu();
    ~Menu();

    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

private:
    void PollCameraStatus(bool statup = false);
    void LoadDefaultConfig();
    void UpdateConfig(const IrsImageTransferProcessorExConfig* config);
    void ResetImage();
    void UpdateImage();
    void updateColourArray();
    auto GetEntryName(s64 i) -> std::string;

private:
    Result m_init_rc{};

    IrsImageTransferProcessorExConfig m_config{};
    IrsMomentProcessorConfig m_moment_config{};
    IrsClusteringProcessorConfig m_clustering_config{};
    IrsTeraPluginProcessorConfig m_tera_config{};
    IrsIrLedProcessorConfig m_led_config{};
    IrsAdaptiveClusteringProcessorConfig m_adaptive_config{};
    IrsHandAnalysisConfig m_hand_config{};

    Entry m_entries[IRS_MAX_CAMERAS]{};
    u32 m_irs_width{};
    u32 m_irs_height{};
    std::vector<u32> m_rgba{};
    std::vector<u8> m_irs_buffer{};
    IrsImageTransferProcessorState m_prev_state{};
    Rotation m_rotation{Rotation_90};
    Colour m_colour{Colour_Grey};
    int m_image{};
    s64 m_index{};
};

} // namespace sphaira::ui::menu::irs
