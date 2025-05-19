#pragma once

#include "base.hpp"

namespace sphaira::usb {

struct UsbHs final : Base {
    UsbHs(u8 index, const UsbHsInterfaceFilter& filter, u64 transfer_timeout);
    ~UsbHs();

    Result Init() override;
    Result IsUsbConnected(u64 timeout) override;

private:
    Event *GetCompletionEvent(UsbSessionEndpoint ep) override;
    Result WaitTransferCompletion(UsbSessionEndpoint ep, u64 timeout) override;
    Result TransferAsync(UsbSessionEndpoint ep, void *buffer, u32 size, u32 *out_xfer_id) override;
    Result GetTransferResult(UsbSessionEndpoint ep, u32 xfer_id, u32 *out_requested_size, u32 *out_transferred_size) override;

    Result Connect();
    void Close();

private:
    u8 m_index{};
    UsbHsInterfaceFilter m_filter{};
    UsbHsInterface m_interface{};
    UsbHsClientIfSession m_s{};
    UsbHsClientEpSession m_endpoints[2]{};
    Event m_event{};
    bool m_connected{};
};

} // namespace sphaira::usb
