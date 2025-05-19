#pragma once

#include "base.hpp"
#include "fs.hpp"
#include "usb/usbds.hpp"

#include <string>
#include <memory>
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
    Result Finished(u64 timeout);

    Result IsUsbConnected(u64 timeout) {
        return m_usb->IsUsbConnected(timeout);
    }

    Result WaitForConnection(u64 timeout, std::vector<std::string>& out_names);
    void SetFileNameForTranfser(const std::string& name);

    void SignalCancel() override {
        m_usb->Cancel();
    }

private:
    Result SendCmdHeader(u32 cmdId, size_t dataSize, u64 timeout);
    Result SendFileRangeCmd(u64 offset, u64 size, u64 timeout);

private:
    std::unique_ptr<usb::UsbDs> m_usb;
    std::string m_transfer_file_name{};
};

} // namespace sphaira::yati::source
