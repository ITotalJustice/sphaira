#pragma once

#include "usb/usbhs.hpp"

#include <string>
#include <memory>
#include <span>
#include <switch.h>

namespace sphaira::usb::upload {

struct Usb {
    Usb(u64 transfer_timeout);
    virtual ~Usb();

    virtual Result Read(const std::string& path, void* buf, s64 off, s64 size, u64* bytes_read) = 0;

    Result IsUsbConnected(u64 timeout) {
        return m_usb->IsUsbConnected(timeout);
    }

    // waits for connection and then sends file list.
    Result WaitForConnection(u64 timeout, u8 flags, std::span<const std::string> names);

    // polls for command, executes transfer if possible.
    // will return Result_Exit if exit command is recieved.
    Result PollCommands();

private:
    Result FileRangeCmd(u64 data_size);

private:
    std::unique_ptr<usb::UsbHs> m_usb;
};

} // namespace sphaira::usb::upload
