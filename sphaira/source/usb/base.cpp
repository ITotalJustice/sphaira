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
#include <ranges>
#include <cstring>

namespace sphaira::usb {

Base::Base(u64 transfer_timeout) {
    m_transfer_timeout = transfer_timeout;
    ueventCreate(GetCancelEvent(), true);
    // this avoids allocations during transfers.
    m_aligned.reserve(1024 * 1024 * 16);
}

Result Base::TransferPacketImpl(bool read, void *page, u32 size, u32 *out_size_transferred, u64 timeout) {
    u32 xfer_id;

    /* If we're not configured yet, wait to become configured first. */
    R_TRY(IsUsbConnected(timeout));

    /* Select the appropriate endpoint and begin a transfer. */
    const auto ep = read ? UsbSessionEndpoint_Out : UsbSessionEndpoint_In;
    R_TRY(TransferAsync(ep, page, size, std::addressof(xfer_id)));

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

// NOTE: it is now possible to request the transfer buffer using GetTransferBuffer(),
// which will always be aligned and have the size aligned.
// this allows for zero-copy transferrs to take place.
// this is used in usb_upload.cpp.
// do note that this relies of the host sending / receiving buffers of an aligned size.
Result Base::TransferAll(bool read, void *data, u32 size, u64 timeout) {
    auto buf = static_cast<u8*>(data);
    auto transfer_buf = m_aligned.data();
    const auto alias = buf == transfer_buf;

    if (!alias) {
        m_aligned.resize(size);
    }

    while (size) {
        if (!alias && !read) {
            std::memcpy(transfer_buf, buf, size);
        }

        u32 out_size_transferred;
        R_TRY(TransferPacketImpl(read, transfer_buf, size, &out_size_transferred, timeout));

        if (!alias && read) {
            std::memcpy(buf, transfer_buf, out_size_transferred);
        }

        if (alias) {
            transfer_buf += out_size_transferred;
        } else {
            buf += out_size_transferred;
        }

        size -= out_size_transferred;
    }

    R_SUCCEED();
}

} // namespace sphaira::usb
