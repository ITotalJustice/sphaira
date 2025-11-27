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

#include "usb/base.hpp"
#include "log.hpp"
#include "defines.hpp"
#include "app.hpp"
#include <ranges>
#include <cstring>

namespace sphaira::usb {
namespace {

constexpr u64 TRANSFER_ALIGN = 0x1000;
constexpr u64 TRANSFER_MAX = 1024*1024*16;
static_assert(!(TRANSFER_MAX % TRANSFER_ALIGN));

} // namespace

Base::Base(u64 transfer_timeout) {
    App::SetAutoSleepDisabled(true);

    m_transfer_timeout = transfer_timeout;
    ueventCreate(GetCancelEvent(), false);
    m_aligned = std::make_unique<u8*>(new(std::align_val_t{TRANSFER_ALIGN}) u8[TRANSFER_MAX]);
}

Base::~Base() {
    App::SetAutoSleepDisabled(false);
}

Result Base::TransferPacketImpl(bool read, void *page, u32 remaining, u32 size, u32 *out_size_transferred, u64 timeout) {
    u32 xfer_id;

    /* If we're not configured yet, wait to become configured first. */
    R_TRY(IsUsbConnected(timeout));

    /* Select the appropriate endpoint and begin a transfer. */
    const auto ep = read ? UsbSessionEndpoint_Out : UsbSessionEndpoint_In;
    R_TRY(TransferAsync(ep, page, remaining, size, std::addressof(xfer_id)));

    /* Try to wait for the event. */
    R_TRY(WaitTransferCompletion(ep, timeout));

    /* Return what we transferred. */
    return GetTransferResult(ep, xfer_id, nullptr, out_size_transferred);
}

// while it may seem like a bad idea to transfer data to a buffer and copy it
// in practice, this has no impact on performance.
// the switch is *massively* bottlenecked by slow io (nand and sd).
// so making usb transfers zero-copy provides no benefit other than increased
// code complexity and the increase of future bugs if/when sphaira is forked
// an changes are made.
// yati already goes to great lengths to be zero-copy during installing
// by swapping buffers and inflating in-place.
Result Base::TransferAll(bool read, void *data, u32 size, u64 timeout) {
    auto buf = static_cast<u8*>(data);
    auto transfer_buf = *m_aligned;

    R_UNLESS(!((u64)transfer_buf & 0xFFF), Result_UsbBadBufferAlign);
    R_UNLESS(size <= TRANSFER_MAX, Result_UsbBadTransferSize);

    while (size) {
        if (!read) {
            std::memcpy(transfer_buf, buf, size);
        }

        u32 out_size_transferred;
        R_TRY(TransferPacketImpl(read, transfer_buf, size, size, &out_size_transferred, timeout));
        R_UNLESS(out_size_transferred > 0, Result_UsbEmptyTransferSize);
        R_UNLESS(out_size_transferred <= size, Result_UsbOverflowTransferSize);

        if (read) {
            std::memcpy(buf, transfer_buf, out_size_transferred);
        }

        buf += out_size_transferred;
        size -= out_size_transferred;
    }

    R_SUCCEED();
}

} // namespace sphaira::usb
