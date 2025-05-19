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
#include "usb/tinfoil.hpp"
#include "log.hpp"
#include <ranges>

namespace sphaira::yati::source {
namespace {

namespace tinfoil = usb::tinfoil;

} // namespace

Usb::Usb(u64 transfer_timeout) {
    m_usb = std::make_unique<usb::UsbDs>(transfer_timeout);
    m_open_result = m_usb->Init();
}

Usb::~Usb() {
}

Result Usb::WaitForConnection(u64 timeout, std::vector<std::string>& out_names) {
    tinfoil::TUSHeader header;
    R_TRY(m_usb->TransferAll(true, &header, sizeof(header), timeout));
    R_UNLESS(header.magic == tinfoil::Magic_List0, Result_BadMagic);
    R_UNLESS(header.nspListSize > 0, Result_BadCount);
    log_write("USB got header\n");

    std::vector<char> names(header.nspListSize);
    R_TRY(m_usb->TransferAll(true, names.data(), names.size(), timeout));

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

Result Usb::SendCmdHeader(u32 cmdId, size_t dataSize, u64 timeout) {
    tinfoil::USBCmdHeader header{
        .magic = tinfoil::Magic_Command0,
        .type = tinfoil::USBCmdType::REQUEST,
        .cmdId = cmdId,
        .dataSize = dataSize,
    };

    return m_usb->TransferAll(false, &header, sizeof(header), timeout);
}

Result Usb::SendFileRangeCmd(u64 off, u64 size, u64 timeout) {
    tinfoil::FileRangeCmdHeader fRangeHeader;
    fRangeHeader.size = size;
    fRangeHeader.offset = off;
    fRangeHeader.nspNameLen = m_transfer_file_name.size();
    fRangeHeader.padding = 0;

    R_TRY(SendCmdHeader(tinfoil::USBCmdId::FILE_RANGE, sizeof(fRangeHeader) + fRangeHeader.nspNameLen, timeout));
    R_TRY(m_usb->TransferAll(false, &fRangeHeader, sizeof(fRangeHeader), timeout));
    R_TRY(m_usb->TransferAll(false, m_transfer_file_name.data(), m_transfer_file_name.size(), timeout));

    tinfoil::USBCmdHeader responseHeader;
    R_TRY(m_usb->TransferAll(true, &responseHeader, sizeof(responseHeader), timeout));

    R_SUCCEED();
}

Result Usb::Finished(u64 timeout) {
    return SendCmdHeader(tinfoil::USBCmdId::EXIT, 0, timeout);
}

Result Usb::Read(void* buf, s64 off, s64 size, u64* bytes_read) {
    R_TRY(GetOpenResult());
    R_TRY(SendFileRangeCmd(off, size, m_usb->GetTransferTimeout()));
    R_TRY(m_usb->TransferAll(true, buf, size));
    *bytes_read = size;
    R_SUCCEED();
}

} // namespace sphaira::yati::source
