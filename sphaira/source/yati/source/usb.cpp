/*
 * Copyright (c) Atmosphère-NX
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms and conditions of the GNU General Public License,
 * version 2, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

// The USB transfer code was taken from Haze (part of Atmosphere).
// The USB protocol was taken from Tinfoil, by Adubbz.

#include "yati/source/usb.hpp"
#include "log.hpp"
#include <ranges>

namespace sphaira::yati::source {
namespace {

enum USBCmdType : u8 {
    REQUEST = 0,
    RESPONSE = 1
};

enum USBCmdId : u32 {
    EXIT = 0,
    FILE_RANGE = 1
};

struct NX_PACKED USBCmdHeader {
    u32 magic;
    USBCmdType type;
    u8 padding[0x3] = {0};
    u32 cmdId;
    u64 dataSize;
    u8 reserved[0xC] = {0};
};

struct FileRangeCmdHeader {
    u64 size;
    u64 offset;
    u64 nspNameLen;
    u64 padding;
};

struct TUSHeader {
    u32 magic; // TUL0 (Tinfoil Usb List 0)
    u32 nspListSize;
    u64 padding;
};

static_assert(sizeof(TUSHeader) == 0x10, "TUSHeader must be 0x10!");
static_assert(sizeof(USBCmdHeader) == 0x20, "USBCmdHeader must be 0x20!");

} // namespace

Usb::Usb(u64 transfer_timeout) {
    m_open_result = usbDsInitialize();
    m_transfer_timeout = transfer_timeout;
    // this avoids allocations during transfers.
    m_aligned.reserve(1024 * 1024 * 16);
}

Usb::~Usb() {
    if (R_SUCCEEDED(GetOpenResult())) {
        usbDsExit();
    }
}

Result Usb::Init() {
    log_write("doing USB init\n");
    R_TRY(m_open_result);

    SetSysSerialNumber serial_number;
    R_TRY(setsysInitialize());
    ON_SCOPE_EXIT(setsysExit());
    R_TRY(setsysGetSerialNumber(&serial_number));

    u8 iManufacturer, iProduct, iSerialNumber;
    static const u16 supported_langs[1] = {0x0409};
    // Send language descriptor
    R_TRY(usbDsAddUsbLanguageStringDescriptor(NULL, supported_langs, sizeof(supported_langs)/sizeof(u16)));
    // Send manufacturer
    R_TRY(usbDsAddUsbStringDescriptor(&iManufacturer, "Nintendo"));
    // Send product
    R_TRY(usbDsAddUsbStringDescriptor(&iProduct, "Nintendo Switch"));
    // Send serial number
    R_TRY(usbDsAddUsbStringDescriptor(&iSerialNumber, serial_number.number));

    // Send device descriptors
    struct usb_device_descriptor device_descriptor = {
        .bLength = USB_DT_DEVICE_SIZE,
        .bDescriptorType = USB_DT_DEVICE,
        .bcdUSB = 0x0110,
        .bDeviceClass = 0x00,
        .bDeviceSubClass = 0x00,
        .bDeviceProtocol = 0x00,
        .bMaxPacketSize0 = 0x40,
        .idVendor = 0x057e,
        .idProduct = 0x3000,
        .bcdDevice = 0x0100,
        .iManufacturer = iManufacturer,
        .iProduct = iProduct,
        .iSerialNumber = iSerialNumber,
        .bNumConfigurations = 0x01
    };

    // Full Speed is USB 1.1
    R_TRY(usbDsSetUsbDeviceDescriptor(UsbDeviceSpeed_Full, &device_descriptor));

    // High Speed is USB 2.0
    device_descriptor.bcdUSB = 0x0200;
    R_TRY(usbDsSetUsbDeviceDescriptor(UsbDeviceSpeed_High, &device_descriptor));

    // Super Speed is USB 3.0
    device_descriptor.bcdUSB = 0x0300;
    // Upgrade packet size to 512
    device_descriptor.bMaxPacketSize0 = 0x09;
    R_TRY(usbDsSetUsbDeviceDescriptor(UsbDeviceSpeed_Super, &device_descriptor));

    // Define Binary Object Store
    const u8 bos[0x16] = {
        0x05,                     // .bLength
        USB_DT_BOS,               // .bDescriptorType
        0x16, 0x00,               // .wTotalLength
        0x02,                     // .bNumDeviceCaps

        // USB 2.0
        0x07,                     // .bLength
        USB_DT_DEVICE_CAPABILITY, // .bDescriptorType
        0x02,                     // .bDevCapabilityType
        0x02, 0x00, 0x00, 0x00,   // dev_capability_data

        // USB 3.0
        0x0A,                     // .bLength
        USB_DT_DEVICE_CAPABILITY, // .bDescriptorType
        0x03,                     /* .bDevCapabilityType */
        0x00,                     /* .bmAttributes */
        0x0E, 0x00,               /* .wSpeedSupported */
        0x03,                     /* .bFunctionalitySupport */
        0x00,                     /* .bU1DevExitLat */
        0x00, 0x00                /* .bU2DevExitLat */
    };

    R_TRY(usbDsSetBinaryObjectStore(bos, sizeof(bos)));

    struct usb_interface_descriptor interface_descriptor = {
        .bLength = USB_DT_INTERFACE_SIZE,
        .bDescriptorType = USB_DT_INTERFACE,
        .bInterfaceNumber = USBDS_DEFAULT_InterfaceNumber, // set below
        .bNumEndpoints = static_cast<u8>(std::size(m_endpoints)),
        .bInterfaceClass = USB_CLASS_VENDOR_SPEC,
        .bInterfaceSubClass = USB_CLASS_VENDOR_SPEC,
        .bInterfaceProtocol = USB_CLASS_VENDOR_SPEC,
    };

    struct usb_endpoint_descriptor endpoint_descriptor_in = {
        .bLength = USB_DT_ENDPOINT_SIZE,
        .bDescriptorType = USB_DT_ENDPOINT,
        .bEndpointAddress = USB_ENDPOINT_IN,
        .bmAttributes = USB_TRANSFER_TYPE_BULK,
    };

    struct usb_endpoint_descriptor endpoint_descriptor_out = {
        .bLength = USB_DT_ENDPOINT_SIZE,
        .bDescriptorType = USB_DT_ENDPOINT,
        .bEndpointAddress = USB_ENDPOINT_OUT,
        .bmAttributes = USB_TRANSFER_TYPE_BULK,
    };

    const struct usb_ss_endpoint_companion_descriptor endpoint_companion = {
        .bLength = sizeof(struct usb_ss_endpoint_companion_descriptor),
        .bDescriptorType = USB_DT_SS_ENDPOINT_COMPANION,
        .bMaxBurst = 0x0F,
        .bmAttributes = 0x00,
        .wBytesPerInterval = 0x00,
    };

    R_TRY(usbDsRegisterInterface(&m_interface));

    interface_descriptor.bInterfaceNumber = m_interface->interface_index;
    endpoint_descriptor_in.bEndpointAddress += interface_descriptor.bInterfaceNumber + 1;
    endpoint_descriptor_out.bEndpointAddress += interface_descriptor.bInterfaceNumber + 1;

    // Full Speed Config
    endpoint_descriptor_in.wMaxPacketSize = 0x40;
    endpoint_descriptor_out.wMaxPacketSize = 0x40;
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Full, &interface_descriptor, USB_DT_INTERFACE_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Full, &endpoint_descriptor_in, USB_DT_ENDPOINT_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Full, &endpoint_descriptor_out, USB_DT_ENDPOINT_SIZE));

    // High Speed Config
    endpoint_descriptor_in.wMaxPacketSize = 0x200;
    endpoint_descriptor_out.wMaxPacketSize = 0x200;
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_High, &interface_descriptor, USB_DT_INTERFACE_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_High, &endpoint_descriptor_in, USB_DT_ENDPOINT_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_High, &endpoint_descriptor_out, USB_DT_ENDPOINT_SIZE));

    // Super Speed Config
    endpoint_descriptor_in.wMaxPacketSize = 0x400;
    endpoint_descriptor_out.wMaxPacketSize = 0x400;
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Super, &interface_descriptor, USB_DT_INTERFACE_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Super, &endpoint_descriptor_in, USB_DT_ENDPOINT_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Super, &endpoint_companion, USB_DT_SS_ENDPOINT_COMPANION_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Super, &endpoint_descriptor_out, USB_DT_ENDPOINT_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Super, &endpoint_companion, USB_DT_SS_ENDPOINT_COMPANION_SIZE));

    //Setup endpoints.
    R_TRY(usbDsInterface_RegisterEndpoint(m_interface, &m_endpoints[UsbSessionEndpoint_In], endpoint_descriptor_in.bEndpointAddress));
    R_TRY(usbDsInterface_RegisterEndpoint(m_interface, &m_endpoints[UsbSessionEndpoint_Out], endpoint_descriptor_out.bEndpointAddress));

    R_TRY(usbDsInterface_EnableInterface(m_interface));
    R_TRY(usbDsEnable());

    log_write("success USB init\n");
    R_SUCCEED();
}

Result Usb::IsUsbConnected(u64 timeout) const {
    return usbDsWaitReady(timeout);
}

Result Usb::WaitForConnection(u64 timeout, std::vector<std::string>& out_names) {
    TUSHeader header;
    R_TRY(TransferAll(true, &header, sizeof(header), timeout));
    R_UNLESS(header.magic == 0x304C5554, Result_BadMagic);
    R_UNLESS(header.nspListSize > 0, Result_BadCount);
    log_write("USB got header\n");

    std::vector<char> names(header.nspListSize);
    R_TRY(TransferAll(true, names.data(), names.size(), timeout));

    out_names.clear();
    for (const auto& name : std::views::split(names, '\n')) {
        if (!name.empty()) {
            out_names.emplace_back(name.data(), name.size());
        }
    }

    for (auto& name : out_names) {
        log_write("got name: %s\n", name.c_str());
    }

    R_UNLESS(!out_names.empty(), Result_BadCount);
    log_write("USB SUCCESS\n");
    R_SUCCEED();
}

void Usb::SetFileNameForTranfser(const std::string& name) {
    m_transfer_file_name = name;
}

Event *Usb::GetCompletionEvent(UsbSessionEndpoint ep) const {
    return std::addressof(m_endpoints[ep]->CompletionEvent);
}

Result Usb::WaitTransferCompletion(UsbSessionEndpoint ep, u64 timeout) const {
    auto event = GetCompletionEvent(ep);
    const auto rc = eventWait(event, timeout);

    if (R_FAILED(rc)) {
        R_TRY(usbDsEndpoint_Cancel(m_endpoints[ep]));
        eventClear(event);
    }

    return rc;
}

Result Usb::TransferAsync(UsbSessionEndpoint ep, void *buffer, u32 size, u32 *out_urb_id) const {
    return usbDsEndpoint_PostBufferAsync(m_endpoints[ep], buffer, size, out_urb_id);
}

Result Usb::GetTransferResult(UsbSessionEndpoint ep, u32 urb_id, u32 *out_requested_size, u32 *out_transferred_size) const {
    UsbDsReportData report_data;

    R_TRY(eventClear(std::addressof(m_endpoints[ep]->CompletionEvent)));
    R_TRY(usbDsEndpoint_GetReportData(m_endpoints[ep], std::addressof(report_data)));
    R_TRY(usbDsParseReportData(std::addressof(report_data), urb_id, out_requested_size, out_transferred_size));

    R_SUCCEED();
}

Result Usb::TransferPacketImpl(bool read, void *page, u32 size, u32 *out_size_transferred, u64 timeout) const {
    u32 urb_id;

    /* If we're not configured yet, wait to become configured first. */
    R_TRY(IsUsbConnected(timeout));

    /* Select the appropriate endpoint and begin a transfer. */
    const auto ep = read ? UsbSessionEndpoint_Out : UsbSessionEndpoint_In;
    R_TRY(TransferAsync(ep, page, size, std::addressof(urb_id)));

    /* Try to wait for the event. */
    R_TRY(WaitTransferCompletion(ep, timeout));

    /* Return what we transferred. */
    return GetTransferResult(ep, urb_id, nullptr, out_size_transferred);
}

// while it may seem like a bad idea to transfer data to a buffer and copy it
// in practice, this has no impact on performance.
// the switch is *massively* bottlenecked by slow io (nand and sd).
// so making usb transfers zero-copy provides no benefit other than increased
// code complexity and the increase of future bugs if/when sphaira is forked
// an changes are made.
// yati already goes to great lengths to be zero-copy during installing
// by swapping buffers and inflating in-place.
Result Usb::TransferAll(bool read, void *data, u32 size, u64 timeout) {
    auto buf = static_cast<u8*>(data);
    m_aligned.resize((size + 0xFFF) & ~0xFFF);

    while (size) {
        if (!read) {
            std::memcpy(m_aligned.data(), buf, size);
        }

        u32 out_size_transferred;
        R_TRY(TransferPacketImpl(read, m_aligned.data(), size, &out_size_transferred, timeout));

        if (read) {
            std::memcpy(buf, m_aligned.data(), out_size_transferred);
        }

        buf += out_size_transferred;
        size -= out_size_transferred;
    }

    R_SUCCEED();
}

Result Usb::SendCmdHeader(u32 cmdId, size_t dataSize) {
    USBCmdHeader header{
        .magic = 0x30435554, // TUC0 (Tinfoil USB Command 0)
        .type = USBCmdType::REQUEST,
        .cmdId = cmdId,
        .dataSize = dataSize,
    };

    return TransferAll(false, &header, sizeof(header), m_transfer_timeout);
}

Result Usb::SendFileRangeCmd(u64 off, u64 size) {
    FileRangeCmdHeader fRangeHeader;
    fRangeHeader.size = size;
    fRangeHeader.offset = off;
    fRangeHeader.nspNameLen = m_transfer_file_name.size();
    fRangeHeader.padding = 0;

    R_TRY(SendCmdHeader(USBCmdId::FILE_RANGE, sizeof(fRangeHeader) + fRangeHeader.nspNameLen));
    R_TRY(TransferAll(false, &fRangeHeader, sizeof(fRangeHeader), m_transfer_timeout));
    R_TRY(TransferAll(false, m_transfer_file_name.data(), m_transfer_file_name.size(), m_transfer_timeout));

    USBCmdHeader responseHeader;
    R_TRY(TransferAll(true, &responseHeader, sizeof(responseHeader), m_transfer_timeout));

    R_SUCCEED();
}

Result Usb::Finished() {
    return SendCmdHeader(USBCmdId::EXIT, 0);
}

Result Usb::Read(void* buf, s64 off, s64 size, u64* bytes_read) {
    R_TRY(GetOpenResult());
    R_TRY(SendFileRangeCmd(off, size));
    R_TRY(TransferAll(true, buf, size, m_transfer_timeout));
    *bytes_read = size;
    R_SUCCEED();
}

} // namespace sphaira::yati::source
