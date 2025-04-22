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

// Most of the usb transfer code was taken from Haze.
#include "yati/source/usb.hpp"
#include "log.hpp"

namespace sphaira::yati::source {
namespace {

constexpr u32 MAGIC = 0x53504841;
constexpr u32 VERSION = 2;

struct SendHeader {
    u32 magic;
    u32 version;
};

struct RecvHeader {
    u32 magic;
    u32 version;
    u32 bcdUSB;
    u32 count;
};

} // namespace

Usb::Usb(u64 transfer_timeout) {
    m_open_result = usbDsInitialize();
    m_transfer_timeout = transfer_timeout;
}

Usb::~Usb() {
    if (R_SUCCEEDED(GetOpenResult())) {
        usbDsExit();
    }
}

Result Usb::Init() {
    log_write("doing USB init\n");
    R_TRY(m_open_result);

    u8 iManufacturer, iProduct, iSerialNumber;
    static const u16 supported_langs[1] = {0x0409};
    // Send language descriptor
    R_TRY(usbDsAddUsbLanguageStringDescriptor(NULL, supported_langs, sizeof(supported_langs)/sizeof(u16)));
    // Send manufacturer
    R_TRY(usbDsAddUsbStringDescriptor(&iManufacturer, "Nintendo"));
    // Send product
    R_TRY(usbDsAddUsbStringDescriptor(&iProduct, "Nintendo Switch"));
    // Send serial number
    R_TRY(usbDsAddUsbStringDescriptor(&iSerialNumber, "SerialNumber"));

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
        .wMaxPacketSize = 0x40,
    };

    struct usb_endpoint_descriptor endpoint_descriptor_out = {
        .bLength = USB_DT_ENDPOINT_SIZE,
        .bDescriptorType = USB_DT_ENDPOINT,
        .bEndpointAddress = USB_ENDPOINT_OUT,
        .bmAttributes = USB_TRANSFER_TYPE_BULK,
        .wMaxPacketSize = 0x40,
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

Result Usb::WaitForConnection(u64 timeout, u32& speed, u32& count) {
    const SendHeader send_header{
        .magic = MAGIC,
        .version = VERSION,
    };

    alignas(0x1000) u8 aligned[0x1000]{};
    std::memcpy(aligned, std::addressof(send_header), sizeof(send_header));

    // send header.
    u32 transferredSize;
    R_TRY(TransferPacketImpl(false, aligned, sizeof(send_header), &transferredSize, timeout));

    // receive header.
    struct RecvHeader recv_header{};
    R_TRY(TransferPacketImpl(true, aligned, sizeof(recv_header), &transferredSize, timeout));

    // copy data into header struct.
    std::memcpy(&recv_header, aligned, sizeof(recv_header));

    // validate received header.
    R_UNLESS(recv_header.magic == MAGIC, Result_BadMagic);
    R_UNLESS(recv_header.version == VERSION, Result_BadVersion);
    R_UNLESS(recv_header.count > 0, Result_BadCount);

    count = recv_header.count;
    speed = recv_header.bcdUSB;
    R_SUCCEED();
}

Result Usb::GetFileInfo(std::string& name_out, u64& size_out) {
    struct {
        u64 size;
        u64 name_length;
    } file_info_meta;

    alignas(0x1000) u8 aligned[0x1000]{};

    // receive meta.
    u32 transferredSize;
    R_TRY(TransferPacketImpl(true, aligned, sizeof(file_info_meta), &transferredSize, m_transfer_timeout));
    std::memcpy(&file_info_meta, aligned, sizeof(file_info_meta));
    R_UNLESS(file_info_meta.name_length < sizeof(aligned), 0x1);

    R_TRY(TransferPacketImpl(true, aligned, file_info_meta.name_length, &transferredSize, m_transfer_timeout));
    name_out.resize(file_info_meta.name_length);
    std::memcpy(name_out.data(), aligned, name_out.size());

    size_out = file_info_meta.size;
    R_SUCCEED();
}

bool Usb::GetConfigured() const {
    UsbState usb_state;
    usbDsGetState(std::addressof(usb_state));
    return usb_state == UsbState_Configured;
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
    // R_TRY(usbDsWaitReady(timeout));
    if (!GetConfigured()) {
        R_TRY(eventWait(usbDsGetStateChangeEvent(), timeout));
        R_TRY(eventClear(usbDsGetStateChangeEvent()));
        R_THROW(0xEA01);
    }

    /* Select the appropriate endpoint and begin a transfer. */
    const auto ep = read ? UsbSessionEndpoint_Out : UsbSessionEndpoint_In;
    R_TRY(TransferAsync(ep, page, size, std::addressof(urb_id)));

    /* Try to wait for the event. */
    R_TRY(WaitTransferCompletion(ep, timeout));

    /* Return what we transferred. */
    return GetTransferResult(ep, urb_id, nullptr, out_size_transferred);
}

Result Usb::SendCommand(s64 off, s64 size) const {
    struct {
        u32 hash;
        u32 magic;
        s64 off;
        s64 size;
    } meta{0, 0, off, size};

    alignas(0x1000) static u8 aligned[0x1000]{};
    std::memcpy(aligned, std::addressof(meta), sizeof(meta));

    u32 transferredSize;
    return TransferPacketImpl(false, aligned, sizeof(meta), &transferredSize, m_transfer_timeout);
}

Result Usb::Finished() const {
    return SendCommand(0, 0);
}

Result Usb::InternalRead(void* _buf, s64 off, s64 size) const {
    u8* buf = (u8*)_buf;
    alignas(0x1000) u8 aligned[0x1000]{};
    const auto stored_size = size;
    s64 total = 0;

    while (size) {
        auto read_size = size;
        auto read_buf = buf;

        if (u64(buf) & 0xFFF) {
            read_size = std::min<u64>(size, sizeof(aligned) - (u64(buf) & 0xFFF));
            read_buf = aligned;
            log_write("unaligned read %zd %zd read_size: %zd align: %zd\n", off, size, read_size, u64(buf) & 0xFFF);
        } else if (read_size & 0xFFF) {
            if (read_size <= 0xFFF) {
                log_write("unaligned small read %zd %zd read_size: %zd align: %zd\n", off, size, read_size, u64(buf) & 0xFFF);
                read_buf = aligned;
            } else {
                log_write("unaligned big read %zd %zd read_size: %zd align: %zd\n", off, size, read_size, u64(buf) & 0xFFF);
                // read as much as possible into buffer, the rest will
                // be handled in a second read which will be aligned size aligned.
                read_size = read_size & ~0xFFF;
            }
        }

        R_TRY(SendCommand(off, read_size));

        u32 transferredSize{};
        R_TRY(TransferPacketImpl(true, read_buf, read_size, &transferredSize, m_transfer_timeout));
        R_UNLESS(transferredSize <= read_size, Result_BadTransferSize);

        if (read_buf == aligned) {
            std::memcpy(buf, aligned, transferredSize);
        }

        if (transferredSize < read_size) {
            log_write("reading less than expected! %u vs %zd stored: %zd\n", transferredSize, read_size, stored_size);
        }

        off += transferredSize;
        buf += transferredSize;
        size -= transferredSize;
        total += transferredSize;
    }

    R_UNLESS(total == stored_size, Result_BadTotalSize);
    R_SUCCEED();
}

Result Usb::Read(void* buf, s64 off, s64 size, u64* bytes_read) {
    R_TRY(GetOpenResult());
    R_TRY(InternalRead(buf, off, size));
    *bytes_read = size;
    R_SUCCEED();
}

} // namespace sphaira::yati::source
