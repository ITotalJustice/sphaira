#pragma once

#include "base.hpp"
#include "fs.hpp"
#include <switch.h>

namespace sphaira::yati::source {

struct Usb final : Base {
    enum { USBModule = 523 };

    enum : Result {
        Result_BadMagic = MAKERESULT(USBModule, 0),
        Result_BadVersion = MAKERESULT(USBModule, 1),
        Result_BadCount = MAKERESULT(USBModule, 2),
        Result_BadTransferSize = MAKERESULT(USBModule, 3),
        Result_BadTotalSize = MAKERESULT(USBModule, 4),
    };

    Usb(u64 transfer_timeout);
    ~Usb();

    Result Read(void* buf, s64 off, s64 size, u64* bytes_read) override;
    Result Finished() const;

    Result Init();
    Result WaitForConnection(u64 timeout, u32& speed, u32& count);
    Result GetFileInfo(std::string& name_out, u64& size_out);

private:
    enum UsbSessionEndpoint {
        UsbSessionEndpoint_In = 0,
        UsbSessionEndpoint_Out = 1,
    };

    Result SendCommand(s64 off, s64 size) const;
    Result InternalRead(void* buf, s64 off, s64 size) const;

    bool GetConfigured() const;
    Event *GetCompletionEvent(UsbSessionEndpoint ep) const;
    Result WaitTransferCompletion(UsbSessionEndpoint ep, u64 timeout) const;
    Result TransferAsync(UsbSessionEndpoint ep, void *buffer, u32 size, u32 *out_urb_id) const;
    Result GetTransferResult(UsbSessionEndpoint ep, u32 urb_id, u32 *out_requested_size, u32 *out_transferred_size) const;
    Result TransferPacketImpl(bool read, void *page, u32 size, u32 *out_size_transferred, u64 timeout) const;

private:
    UsbDsInterface* m_interface{};
    UsbDsEndpoint* m_endpoints[2]{};
    u64 m_transfer_timeout{};
};

} // namespace sphaira::yati::source
