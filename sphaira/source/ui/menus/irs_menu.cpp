#include "ui/menus/irs_menu.hpp"
#include "ui/sidebar.hpp"
#include "ui/popup_list.hpp"
#include "app.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "ui/nvg_util.hpp"
#include "i18n.hpp"
#include <cstring>
#include <array>

namespace sphaira::ui::menu::irs {
namespace {

// from trial and error
constexpr u32 GAIN_MIN = 1;
constexpr u32 GAIN_MAX = 16;

consteval auto generte_iron_palette_table() {
    std::array<u32, 256> array{};

    const u32 iron_palette[] = {
        0xff000014, 0xff000025, 0xff00002a, 0xff000032, 0xff000036, 0xff00003e, 0xff000042, 0xff00004f,
        0xff010055, 0xff010057, 0xff02005c, 0xff03005e, 0xff040063, 0xff050065, 0xff070069, 0xff0a0070,
        0xff0b0073, 0xff0d0075, 0xff0d0076, 0xff100078, 0xff120079, 0xff15007c, 0xff17007d, 0xff1c0081,
        0xff200084, 0xff220085, 0xff260087, 0xff280089, 0xff2c008a, 0xff2e008b, 0xff32008d, 0xff38008f,
        0xff390090, 0xff3c0092, 0xff3e0093, 0xff410094, 0xff420095, 0xff450096, 0xff470096, 0xff4c0097,
        0xff4f0097, 0xff510097, 0xff540098, 0xff560098, 0xff5a0099, 0xff5c0099, 0xff5f009a, 0xff64009b,
        0xff66009b, 0xff6a009b, 0xff6c009c, 0xff6f009c, 0xff70009c, 0xff73009d, 0xff75009d, 0xff7a009d,
        0xff7e009d, 0xff7f009d, 0xff83009d, 0xff84009d, 0xff87009d, 0xff89009d, 0xff8b009d, 0xff91009c,
        0xff93009c, 0xff96009b, 0xff98009b, 0xff9b009b, 0xff9c009b, 0xff9f009b, 0xffa0009b, 0xffa4009b,
        0xffa7009a, 0xffa8009a, 0xffaa0099, 0xffab0099, 0xffae0198, 0xffaf0198, 0xffb00198, 0xffb30196,
        0xffb40296, 0xffb60295, 0xffb70395, 0xffb90495, 0xffba0495, 0xffbb0593, 0xffbc0593, 0xffbf0692,
        0xffc00791, 0xffc00791, 0xffc10990, 0xffc20a8f, 0xffc30b8e, 0xffc40c8d, 0xffc60d8b, 0xffc81088,
        0xffc91187, 0xffca1385, 0xffcb1385, 0xffcc1582, 0xffcd1681, 0xffce187e, 0xffcf187c, 0xffd11b78,
        0xffd21c75, 0xffd21d74, 0xffd32071, 0xffd4216f, 0xffd5236b, 0xffd52469, 0xffd72665, 0xffd92a60,
        0xffda2b5e, 0xffdb2e5a, 0xffdb2f57, 0xffdd3051, 0xffdd314e, 0xffde3347, 0xffdf3444, 0xffe0373a,
        0xffe03933, 0xffe13a30, 0xffe23c2a, 0xffe33d26, 0xffe43f20, 0xffe4411d, 0xffe5431b, 0xffe64616,
        0xffe74715, 0xffe74913, 0xffe84a12, 0xffe84c0f, 0xffe94d0e, 0xffea4e0c, 0xffea4f0c, 0xffeb520a,
        0xffec5409, 0xffec5608, 0xffec5808, 0xffed5907, 0xffed5b06, 0xffee5c06, 0xffee5d05, 0xffef6004,
        0xffef6104, 0xfff06303, 0xfff06403, 0xfff16603, 0xfff16603, 0xfff16803, 0xfff16902, 0xfff16b02,
        0xfff26d01, 0xfff26e01, 0xfff37001, 0xfff37101, 0xfff47300, 0xfff47400, 0xfff47600, 0xfff47a00,
        0xfff57b00, 0xfff57e00, 0xfff57f00, 0xfff68100, 0xfff68200, 0xfff78400, 0xfff78500, 0xfff88800,
        0xfff88900, 0xfff88a00, 0xfff88c00, 0xfff98d00, 0xfff98e00, 0xfff98f00, 0xfff99100, 0xfffa9400,
        0xfffa9500, 0xfffb9800, 0xfffb9900, 0xfffb9c00, 0xfffc9d00, 0xfffca000, 0xfffca100, 0xfffda400,
        0xfffda700, 0xfffda800, 0xfffdab00, 0xfffdac00, 0xfffdae00, 0xfffeaf00, 0xfffeb100, 0xfffeb400,
        0xfffeb500, 0xfffeb800, 0xfffeb900, 0xfffeba00, 0xfffebb00, 0xfffebd00, 0xfffebe00, 0xfffec200,
        0xfffec400, 0xfffec500, 0xfffec700, 0xfffec800, 0xfffeca01, 0xfffeca01, 0xfffecc02, 0xfffecf04,
        0xfffecf04, 0xfffed106, 0xfffed308, 0xfffed50a, 0xfffed60a, 0xfffed80c, 0xfffed90d, 0xffffdb10,
        0xffffdc14, 0xffffdd16, 0xffffde1b, 0xffffdf1e, 0xffffe122, 0xffffe224, 0xffffe328, 0xffffe531,
        0xffffe635, 0xffffe73c, 0xffffe83f, 0xffffea46, 0xffffeb49, 0xffffec50, 0xffffed54, 0xffffee5f,
        0xffffef67, 0xfffff06a, 0xfffff172, 0xfffff177, 0xfffff280, 0xfffff285, 0xfffff38e, 0xfffff49a,
        0xfffff59e, 0xfffff5a6, 0xfffff6aa, 0xfffff7b3, 0xfffff7b6, 0xfffff8bd, 0xfffff8c1, 0xfffff9ca,
        0xfffffad1, 0xfffffad4, 0xfffffcdb, 0xfffffcdf, 0xfffffde5, 0xfffffde8, 0xfffffeee, 0xfffffff6
    };

    for (u32 i = 0; i < 256; i++) {
        const auto c = iron_palette[i];
        array[i] = RGBA8_MAXALPHA((c >> 16) & 0xFF, (c >> 8) & 0xFF, (c >> 0) & 0xFF);
    }

    return array;
}

// ARGB Ironbow palette
constexpr auto iron_palette = generte_iron_palette_table();

void irsConvertConfigExToNormal(const IrsImageTransferProcessorExConfig* ex, IrsImageTransferProcessorConfig* nor) {
    std::memcpy(nor, ex, sizeof(*nor));
}

void irsConvertConfigNormalToEx(const IrsImageTransferProcessorConfig* nor, IrsImageTransferProcessorExConfig* ex) {
    std::memcpy(ex, nor, sizeof(*nor));
}

} // namespace

Menu::Menu(u32 flags) : MenuBase{"Irs"_i18n, flags} {
    SetAction(Button::B, Action{"Back"_i18n, [this](){
        SetPop();
    }});

    SetAction(Button::X, Action{"Options"_i18n, [this](){
        auto options = std::make_unique<Sidebar>("Options"_i18n, Sidebar::Side::RIGHT);
        ON_SCOPE_EXIT(App::Push(std::move(options)));

        SidebarEntryArray::Items controller_str;
        for (u32 i = 0; i < IRS_MAX_CAMERAS; i++) {
            controller_str.emplace_back(GetEntryName(i));
        }

        SidebarEntryArray::Items rotation_str;
        rotation_str.emplace_back("0 (Sideways)"_i18n);
        rotation_str.emplace_back("90 (Flat)"_i18n);
        rotation_str.emplace_back("180 (-Sideways)"_i18n);
        rotation_str.emplace_back("270 (Upside down)"_i18n);

        SidebarEntryArray::Items colour_str;
        colour_str.emplace_back("Grey"_i18n);
        colour_str.emplace_back("Ironbow"_i18n);
        colour_str.emplace_back("Green"_i18n);
        colour_str.emplace_back("Red"_i18n);
        colour_str.emplace_back("Blue"_i18n);

        SidebarEntryArray::Items light_target_str;
        light_target_str.emplace_back("All leds"_i18n);
        light_target_str.emplace_back("Bright group"_i18n);
        light_target_str.emplace_back("Dim group"_i18n);
        light_target_str.emplace_back("None"_i18n);

        SidebarEntryArray::Items gain_str;
        for (u32 i = GAIN_MIN; i <= GAIN_MAX; i++) {
            gain_str.emplace_back(std::to_string(i));
        }

        SidebarEntryArray::Items is_negative_image_used_str;
        is_negative_image_used_str.emplace_back("Normal image"_i18n);
        is_negative_image_used_str.emplace_back("Negative image"_i18n);

        SidebarEntryArray::Items format_str;
        format_str.emplace_back("320\u00D7240");
        format_str.emplace_back("160\u00D7120");
        format_str.emplace_back("80\u00D760");
        if (hosversionAtLeast(4,0,0)) {
            format_str.emplace_back("40\u00D730");
            format_str.emplace_back("20\u00D715");
        }

        options->Add<SidebarEntryArray>("Controller"_i18n, controller_str, [this](s64& index){
            irsStopImageProcessor(m_entries[m_index].m_handle);
            m_index = index;
            UpdateConfig(&m_config);
        }, m_index);

        options->Add<SidebarEntryArray>("Rotation"_i18n, rotation_str, [this](s64& index){
            m_rotation = (Rotation)index;
        }, m_rotation);

        options->Add<SidebarEntryArray>("Colour"_i18n, colour_str, [this](s64& index){
            m_colour = (Colour)index;
            updateColourArray();
        }, m_colour);

        options->Add<SidebarEntryArray>("Light Target"_i18n, light_target_str, [this](s64& index){
            m_config.light_target = index;
            UpdateConfig(&m_config);
        }, m_config.light_target);

        options->Add<SidebarEntryArray>("Gain"_i18n, gain_str, [this](s64& index){
            m_config.gain = GAIN_MIN + index;
            UpdateConfig(&m_config);
        }, m_config.gain - GAIN_MIN);

        options->Add<SidebarEntryArray>("Negative Image"_i18n, is_negative_image_used_str, [this](s64& index){
            m_config.is_negative_image_used = index;
            UpdateConfig(&m_config);
        }, m_config.is_negative_image_used);

        options->Add<SidebarEntryArray>("Format"_i18n, format_str, [this](s64& index){
            m_config.orig_format = index;
            m_config.trimming_format = index;
            UpdateConfig(&m_config);
        }, m_config.orig_format);

        if (hosversionAtLeast(4,0,0)) {
            options->Add<SidebarEntryArray>("Trimming Format"_i18n, format_str, [this](s64& index){
            // you cannot set trim a larger region than the source
            if (index < m_config.orig_format) {
                index = m_config.orig_format;
            } else {
                m_config.trimming_format = index;
                UpdateConfig(&m_config);
            }
        }, m_config.orig_format);

            options->Add<SidebarEntryBool>("External Light Filter"_i18n, m_config.is_external_light_filter_enabled, [this](bool& enable){
                m_config.is_external_light_filter_enabled = enable;
                UpdateConfig(&m_config);
            });
        }

        options->Add<SidebarEntryCallback>("Load Default"_i18n, [this](){
            LoadDefaultConfig();
        }, true);
    }});

    if (R_FAILED(m_init_rc = irsInitialize())) {
        return;
    }

    static_assert(IRS_MAX_CAMERAS >= 9, "max cameras has gotten smaller!");

    // open all handles
    irsGetIrCameraHandle(&m_entries[0].m_handle, HidNpadIdType_No1);
    irsGetIrCameraHandle(&m_entries[1].m_handle, HidNpadIdType_No2);
    irsGetIrCameraHandle(&m_entries[2].m_handle, HidNpadIdType_No3);
    irsGetIrCameraHandle(&m_entries[3].m_handle, HidNpadIdType_No4);
    irsGetIrCameraHandle(&m_entries[4].m_handle, HidNpadIdType_No5);
    irsGetIrCameraHandle(&m_entries[5].m_handle, HidNpadIdType_No6);
    irsGetIrCameraHandle(&m_entries[6].m_handle, HidNpadIdType_No7);
    irsGetIrCameraHandle(&m_entries[7].m_handle, HidNpadIdType_No8);
    irsGetIrCameraHandle(&m_entries[8].m_handle, HidNpadIdType_Handheld);
    // get status of all handles
    PollCameraStatus(true);
    // load default config
    LoadDefaultConfig();
}

Menu::~Menu() {
    ResetImage();

    for (auto& e : m_entries) {
        irsStopImageProcessor(e.m_handle);
    }

    // this closes all handles
    irsExit();
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);
    PollCameraStatus();
    SetTitleSubHeading(GetEntryName(m_index));
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    IrsImageTransferProcessorState state;
    const auto rc = irsGetImageTransferProcessorState(m_entries[m_index].m_handle, m_irs_buffer.data(), m_irs_buffer.size(), &state);
    if (R_SUCCEEDED(rc) && state.sampling_number != m_prev_state.sampling_number) {
        m_prev_state = state;
        SetSubHeading("Ambient Noise Level: "_i18n + std::to_string(m_prev_state.ambient_noise_level));
        updateColourArray();
    }

    if (m_image) {
        float cx{}, cy{};
        float w{}, h{};
        float angle{};

        switch (m_rotation) {
            case Rotation_0: {
                const auto scale_x = m_pos.w / float(m_irs_width);
                const auto scale_y = m_pos.h / float(m_irs_height);
                const auto scale = std::min(scale_x, scale_y);
                w = m_irs_width * scale;
                h = m_irs_height * scale;
                cx = (m_pos.x + m_pos.w / 2.F) - w / 2.F;
                cy = (m_pos.y + m_pos.h / 2.F) - h / 2.F;
                angle = 0;
            }   break;
            case Rotation_90: {
                const auto scale_x = m_pos.w / float(m_irs_height);
                const auto scale_y = m_pos.h / float(m_irs_width);
                const auto scale = std::min(scale_x, scale_y);
                w = m_irs_width * scale;
                h = m_irs_height * scale;
                cx = (m_pos.x + m_pos.w / 2.F) + h / 2.F;
                cy = (m_pos.y + m_pos.h / 2.F) - w / 2.F;
                angle = 90;
            }   break;
            case Rotation_180: {
                const auto scale_x = m_pos.w / float(m_irs_width);
                const auto scale_y = m_pos.h / float(m_irs_height);
                const auto scale = std::min(scale_x, scale_y);
                w = m_irs_width * scale;
                h = m_irs_height * scale;
                cx = (m_pos.x + m_pos.w / 2.F) + w / 2.F;
                cy = (m_pos.y + m_pos.h / 2.F) + h / 2.F;
                angle = 180;
            }   break;
            case Rotation_270: {
                const auto scale_x = m_pos.w / float(m_irs_height);
                const auto scale_y = m_pos.h / float(m_irs_width);
                const auto scale = std::min(scale_x, scale_y);
                w = m_irs_width * scale;
                h = m_irs_height * scale;
                cx = (m_pos.x + m_pos.w / 2.F) - h / 2.F;
                cy = (m_pos.y + m_pos.h / 2.F) + w / 2.F;
                angle = 270;
            }   break;
        }

        nvgSave(vg);
        nvgTranslate(vg, cx, cy);
        const auto paint = nvgImagePattern(vg, 0, 0, w, h, 0, m_image, 1.f);
        nvgRotate(vg, nvgDegToRad(angle));
        nvgBeginPath(vg);
        nvgRect(vg, 0, 0, w, h);
        nvgFillPaint(vg, paint);
        nvgFill(vg);
        nvgRestore(vg);
    }
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();

    if (m_entries[m_index].status != IrsIrCameraStatus_Available) {
        // poll to get first available handle.
        PollCameraStatus(false);

        // find the first available entry and connect to that.
        for (s64 i = 0; i < std::size(m_entries); i++) {
            if (m_entries[i].status == IrsIrCameraStatus_Available) {
                m_index = i;
                UpdateConfig(&m_config);
                break;
            }
        }
    }
}

void Menu::PollCameraStatus(bool statup) {
    int index = 0;
    for (auto& e : m_entries) {
        IrsIrCameraStatus status;
        if (R_FAILED(irsGetIrCameraStatus(e.m_handle, &status))) {
            log_write("failed to get ir status\n");
            continue;
        }

        if (e.status != status || statup) {
            e.status = status;
            e.m_update_needed = false;

            log_write("status changed\n");
            switch (e.status) {
                case IrsIrCameraStatus_Available:
                    if (hosversionAtLeast(4,0,0)) {
                        // calling this breaks the handle, kinda
                        #if 0
                        if (R_FAILED(irsCheckFirmwareUpdateNecessity(e.m_handle, &e.m_update_needed))) {
                            log_write("failed to check if update needed: %u\n", e.m_update_needed);
                        } else {
                            if (e.m_update_needed) {
                                log_write("update needed\n");
                            } else {
                                log_write("no update needed\n");
                            }
                        }
                        #endif
                    }
                    log_write("irs index: %d status: IrsIrCameraStatus_Available\n", index);
                    break;
                case IrsIrCameraStatus_Unsupported:
                    log_write("irs index: %d status: IrsIrCameraStatus_Unsupported\n", index);
                    break;
                case IrsIrCameraStatus_Unconnected:
                    log_write("irs index: %d status: IrsIrCameraStatus_Unconnected\n", index);
                    break;
            }
        }

        index++;
    }
}

void Menu::ResetImage() {
    if (m_image) {
        nvgDeleteImage(App::GetVg(), m_image);
        m_image = 0;
    }
}

void Menu::UpdateImage() {
    ResetImage();
    m_image = nvgCreateImageRGBA(App::GetVg(), m_irs_width, m_irs_height, NVG_IMAGE_NEAREST, (const unsigned char*)m_rgba.data());
}

void Menu::LoadDefaultConfig() {
    IrsImageTransferProcessorExConfig ex_config;

    if (hosversionAtLeast(4,0,0)) {
        irsGetDefaultImageTransferProcessorExConfig(&ex_config);
    } else {
        IrsImageTransferProcessorConfig nor_config;
        irsGetDefaultImageTransferProcessorConfig(&nor_config);
        irsConvertConfigNormalToEx(&nor_config, &ex_config);
    }

    irsGetMomentProcessorDefaultConfig(&m_moment_config);
    irsGetClusteringProcessorDefaultConfig(&m_clustering_config);
    irsGetIrLedProcessorDefaultConfig(&m_led_config);

    m_tera_config = {};
    m_adaptive_config = {};
    m_hand_config = {};

    UpdateConfig(&ex_config);
}

void Menu::UpdateConfig(const IrsImageTransferProcessorExConfig* config) {
    m_config = *config;
    irsStopImageProcessor(m_entries[m_index].m_handle);

    if (R_FAILED(irsRunMomentProcessor(m_entries[m_index].m_handle, &m_moment_config))) {
        log_write("failed to irsRunMomentProcessor\n");
    } else {
        log_write("did irsRunMomentProcessor\n");
    }

    if (R_FAILED(irsRunClusteringProcessor(m_entries[m_index].m_handle, &m_clustering_config))) {
        log_write("failed to irsRunClusteringProcessor\n");
    } else {
        log_write("did irsRunClusteringProcessor\n");
    }

    if (R_FAILED(irsRunPointingProcessor(m_entries[m_index].m_handle))) {
        log_write("failed to irsRunPointingProcessor\n");
    } else {
        log_write("did irsRunPointingProcessor\n");
    }

    if (R_FAILED(irsRunTeraPluginProcessor(m_entries[m_index].m_handle, &m_tera_config))) {
        log_write("failed to irsRunTeraPluginProcessor\n");
    } else {
        log_write("did irsRunTeraPluginProcessor\n");
    }

    if (R_FAILED(irsRunIrLedProcessor(m_entries[m_index].m_handle, &m_led_config))) {
        log_write("failed to irsRunIrLedProcessor\n");
    } else {
        log_write("did irsRunIrLedProcessor\n");
    }

    if (R_FAILED(irsRunAdaptiveClusteringProcessor(m_entries[m_index].m_handle, &m_adaptive_config))) {
        log_write("failed to irsRunAdaptiveClusteringProcessor\n");
    } else {
        log_write("did irsRunAdaptiveClusteringProcessor\n");
    }

    if (R_FAILED(irsRunHandAnalysis(m_entries[m_index].m_handle, &m_hand_config))) {
        log_write("failed to irsRunHandAnalysis\n");
    } else {
        log_write("did irsRunHandAnalysis\n");
    }

    if (hosversionAtLeast(4,0,0)) {
        m_init_rc = irsRunImageTransferExProcessor(m_entries[m_index].m_handle, &m_config, 0x10000000);
    } else {
        IrsImageTransferProcessorConfig nor;
        irsConvertConfigExToNormal(&m_config, &nor);
        m_init_rc = irsRunImageTransferProcessor(m_entries[m_index].m_handle, &nor, 0x10000000);
    }

    if (R_FAILED(m_init_rc)) {
        log_write("irs failed to set config!\n");
    }

    auto format = m_config.orig_format;
    log_write("IRS CONFIG\n");
    log_write("\texposure_time: %lu\n", m_config.exposure_time);
    log_write("\tlight_target: %u\n", m_config.light_target);
    log_write("\tgain: %u\n", m_config.gain);
    log_write("\tis_negative_image_used: %u\n", m_config.is_negative_image_used);
    log_write("\tlight_target: %u\n", m_config.light_target);
    if (hosversionAtLeast(4,0,0)) {
        format = m_config.trimming_format;
        log_write("\ttrimming_format: %u\n", m_config.trimming_format);
        log_write("\ttrimming_start_x: %u\n", m_config.trimming_start_x);
        log_write("\ttrimming_start_y: %u\n", m_config.trimming_start_y);
        log_write("\tis_external_light_filter_enabled: %u\n", m_config.is_external_light_filter_enabled);
    }

    switch (format) {
        case IrsImageTransferProcessorFormat_320x240:
            log_write("\tsetting format: %s\n", "IrsImageTransferProcessorFormat_320x240");
            m_irs_width = 320;
            m_irs_height = 240;
            break;
        case IrsImageTransferProcessorFormat_160x120:
            log_write("\tsetting format: %s\n", "IrsImageTransferProcessorFormat_160x120");
            m_irs_width = 160;
            m_irs_height = 120;
            break;
        case IrsImageTransferProcessorFormat_80x60:
            log_write("\tsetting format: %s\n", "IrsImageTransferProcessorFormat_80x60");
            m_irs_width = 80;
            m_irs_height = 60;
            break;
        case IrsImageTransferProcessorFormat_40x30:
            log_write("\tsetting format: %s\n", "IrsImageTransferProcessorFormat_40x30");
            m_irs_width = 40;
            m_irs_height = 30;
            break;
        case IrsImageTransferProcessorFormat_20x15:
            log_write("\tsetting format: %s\n", "IrsImageTransferProcessorFormat_20x15");
            m_irs_width = 20;
            m_irs_height = 15;
            break;
    }

    m_rgba.resize(m_irs_width * m_irs_height);
    m_irs_buffer.resize(m_irs_width * m_irs_height);
    m_prev_state.sampling_number = UINT64_MAX;
    std::fill(m_irs_buffer.begin(), m_irs_buffer.end(), 0);
    updateColourArray();
}

void Menu::updateColourArray() {
    const auto ir_width = m_irs_width;
    const auto ir_height = m_irs_height;
    const auto colour = m_colour;

    for (u32 y = 0; y < ir_height; y++) {
        for (u32 x = 0; x < ir_width; x++) {
            const u32 pos = y * ir_width + x;
            const u32 pos2 = y * ir_width + x;

            switch (colour) {
                case Colour_Grey:
                    m_rgba[pos] = RGBA8_MAXALPHA(m_irs_buffer[pos2], m_irs_buffer[pos2], m_irs_buffer[pos2]);
                    break;
                case Colour_Ironbow:
                    m_rgba[pos] = iron_palette[m_irs_buffer[pos2]];
                    break;
                case Colour_Green:
                    m_rgba[pos] = RGBA8_MAXALPHA(0, m_irs_buffer[pos2], 0);
                    break;
                case Colour_Red:
                    m_rgba[pos] = RGBA8_MAXALPHA(m_irs_buffer[pos2], 0, 0);
                    break;
                case Colour_Blue:
                    m_rgba[pos] = RGBA8_MAXALPHA(0, 0, m_irs_buffer[pos2]);
                    break;
            }
        }
    }

    UpdateImage();
}

auto Menu::GetEntryName(s64 i) -> std::string {
    const auto& e = m_entries[i];
    std::string text = i18n::Reorder("Pad ", (i == 8 ? "HandHeld"_i18n : std::to_string(i)));
    switch (e.status) {
        case IrsIrCameraStatus_Available:
            text += " (Available)"_i18n;
            break;
        case IrsIrCameraStatus_Unsupported:
            text += " (Unsupported)"_i18n;
            break;
        case IrsIrCameraStatus_Unconnected:
            text += " (Unconnected)"_i18n;
            break;
    }
    return text;
}

} // namespace sphaira::ui::menu::irs
