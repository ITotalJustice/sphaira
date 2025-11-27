#pragma once

#include "base.hpp"

auto GetUsbDsStateStr(UsbState state) -> const char*;
auto GetUsbDsSpeedStr(UsbDeviceSpeed speed) -> const char*;

namespace sphaira::usb {

// Device Host
struct UsbDs final : Base {
    using Base::Base;
    ~UsbDs();

    Result Init() override;
    Result IsUsbConnected(u64 timeout) override;
    Result GetSpeed(UsbDeviceSpeed* out, u16* max_packet_size);

private:
    Result WaitUntilConfigured(u64 timeout);
    Event *GetCompletionEvent(UsbSessionEndpoint ep) override;
    Result WaitTransferCompletion(UsbSessionEndpoint ep, u64 timeout) override;
    Result TransferAsync(UsbSessionEndpoint ep, void *buffer, u32 remaining, u32 size, u32 *out_urb_id) override;
    Result GetTransferResult(UsbSessionEndpoint ep, u32 urb_id, u32 *out_requested_size, u32 *out_transferred_size) override;

private:
    UsbDsInterface* m_interface{};
    UsbDsEndpoint* m_endpoints[2]{};
    u16 m_max_packet_size{};
};

} // namespace sphaira::usb
