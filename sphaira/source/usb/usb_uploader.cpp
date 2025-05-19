// The USB protocol was taken from Tinfoil, by Adubbz.

#include "usb/usb_uploader.hpp"
#include "usb/tinfoil.hpp"
#include "log.hpp"
#include "defines.hpp"

namespace sphaira::usb::upload {
namespace {

namespace tinfoil = usb::tinfoil;

const UsbHsInterfaceFilter FILTER{
    .Flags = UsbHsInterfaceFilterFlags_idVendor |
             UsbHsInterfaceFilterFlags_idProduct |
             UsbHsInterfaceFilterFlags_bcdDevice_Min |
             UsbHsInterfaceFilterFlags_bcdDevice_Max |
             UsbHsInterfaceFilterFlags_bDeviceClass |
             UsbHsInterfaceFilterFlags_bDeviceSubClass |
             UsbHsInterfaceFilterFlags_bDeviceProtocol |
             UsbHsInterfaceFilterFlags_bInterfaceClass |
             UsbHsInterfaceFilterFlags_bInterfaceSubClass |
             UsbHsInterfaceFilterFlags_bInterfaceProtocol,
    .idVendor = 0x057e,
    .idProduct = 0x3000,
    .bcdDevice_Min = 0x0100,
    .bcdDevice_Max = 0x0100,
    .bDeviceClass = 0x00,
    .bDeviceSubClass = 0x00,
    .bDeviceProtocol = 0x00,
    .bInterfaceClass = USB_CLASS_VENDOR_SPEC,
    .bInterfaceSubClass = USB_CLASS_VENDOR_SPEC,
    .bInterfaceProtocol = USB_CLASS_VENDOR_SPEC,
};

constexpr u8 INDEX = 0;

} // namespace

Usb::Usb(u64 transfer_timeout) {
    m_usb = std::make_unique<usb::UsbHs>(INDEX, FILTER, transfer_timeout);
    m_usb->Init();
}

Usb::~Usb() {
}

Result Usb::WaitForConnection(u64 timeout, std::span<const std::string> names) {
    R_TRY(m_usb->IsUsbConnected(timeout));

    std::string names_list;
    for (auto& name : names) {
        names_list += name + '\n';
    }

    tinfoil::TUSHeader header{};
    header.magic = tinfoil::Magic_List0;
    header.nspListSize = names_list.length();

    R_TRY(m_usb->TransferAll(false, &header, sizeof(header), timeout));
    R_TRY(m_usb->TransferAll(false, names_list.data(), names_list.length(), timeout));

    R_SUCCEED();
}

Result Usb::PollCommands() {
    tinfoil::USBCmdHeader header;
    R_TRY(m_usb->TransferAll(true, &header, sizeof(header)));
    R_UNLESS(header.magic == tinfoil::Magic_Command0, Result_BadMagic);

    if (header.cmdId == tinfoil::USBCmdId::EXIT) {
        return Result_Exit;
    } else if (header.cmdId == tinfoil::USBCmdId::FILE_RANGE) {
        return FileRangeCmd(header.dataSize);
    } else {
        return Result_BadCommand;
    }
}

Result Usb::FileRangeCmd(u64 data_size) {
    tinfoil::FileRangeCmdHeader header;
    R_TRY(m_usb->TransferAll(true, &header, sizeof(header)));

    std::string path(header.nspNameLen, '\0');
    R_TRY(m_usb->TransferAll(true, path.data(), header.nspNameLen));

    // send response header.
    R_TRY(m_usb->TransferAll(false, &header, sizeof(header)));

    s64 curr_off = 0x0;
    s64 end_off = header.size;
    s64 read_size = header.size;

    // use transfer buffer directly to avoid copy overhead.
    auto& buf = m_usb->GetTransferBuffer();
    buf.resize(header.size);

    while (curr_off < end_off) {
        if (curr_off + read_size >= end_off) {
            read_size = end_off - curr_off;
        }

        u64 bytes_read;
        R_TRY(Read(path, buf.data(), header.offset + curr_off, read_size, &bytes_read));
        R_TRY(m_usb->TransferAll(false, buf.data(), bytes_read));
        curr_off += bytes_read;
    }

    R_SUCCEED();
}

} // namespace sphaira::usb::upload
