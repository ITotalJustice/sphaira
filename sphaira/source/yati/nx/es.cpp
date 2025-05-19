#include "yati/nx/es.hpp"
#include "yati/nx/crypto.hpp"
#include "yati/nx/nxdumptool_rsa.h"
#include "yati/nx/service_guard.h"
#include "defines.hpp"
#include "log.hpp"
#include <memory>
#include <cstring>

namespace sphaira::es {
namespace {

Service g_esSrv;

NX_GENERATE_SERVICE_GUARD(es);

Result _esInitialize() {
    return smGetService(&g_esSrv, "es");
}

void _esCleanup() {
    serviceClose(&g_esSrv);
}

Result ListTicket(u32 cmd_id, s32 *out_entries_written, FsRightsId* out_ids, s32 count) {
    struct {
        u32 num_rights_ids_written;
    } out;

    const Result rc = serviceDispatchInOut(&g_esSrv, cmd_id, *out_entries_written, out,
        .buffer_attrs = { SfBufferAttr_HipcMapAlias | SfBufferAttr_Out },
        .buffers = { { out_ids, count * sizeof(*out_ids) } },
    );

    if (R_SUCCEEDED(rc) && out_entries_written) *out_entries_written = out.num_rights_ids_written;
    return rc;
}

} // namespace

Result Initialize() {
    return esInitialize();
}

void Exit() {
    esExit();
}

Service* GetServiceSession() {
    return &g_esSrv;
}

Result ImportTicket(const void* tik_buf, u64 tik_size, const void* cert_buf, u64 cert_size) {
    return serviceDispatch(&g_esSrv, 1,
        .buffer_attrs = { SfBufferAttr_HipcMapAlias | SfBufferAttr_In, SfBufferAttr_HipcMapAlias | SfBufferAttr_In },
        .buffers = { { tik_buf, tik_size }, { cert_buf, cert_size } }
    );
}

Result CountCommonTicket(s32* count) {
    return serviceDispatchOut(&g_esSrv, 9, *count);
}

Result CountPersonalizedTicket(s32* count) {
    return serviceDispatchOut(&g_esSrv, 10, *count);
}

Result ListCommonTicket(s32 *out_entries_written, FsRightsId* out_ids, s32 count) {
    return ListTicket(11, out_entries_written, out_ids, count);
}

Result ListPersonalizedTicket(s32 *out_entries_written, FsRightsId* out_ids, s32 count) {
    return ListTicket(12, out_entries_written, out_ids, count);
}

Result ListMissingPersonalizedTicket(s32 *out_entries_written, FsRightsId* out_ids, s32 count) {
    return ListTicket(13, out_entries_written, out_ids, count);
}

Result GetCommonTicketSize(u64 *size_out, const FsRightsId* rightsId) {
    return serviceDispatchInOut(&g_esSrv, 14, *rightsId, *size_out);
}

Result GetCommonTicketData(u64 *size_out, void *tik_data, u64 tik_size, const FsRightsId* rightsId) {
    return serviceDispatchInOut(&g_esSrv, 16, *rightsId, *size_out,
        .buffer_attrs = { SfBufferAttr_HipcMapAlias | SfBufferAttr_Out },
        .buffers = { { tik_data, tik_size } },
    );
}

Result GetCommonTicketAndCertificateSize(u64 *tik_size_out, u64 *cert_size_out, const FsRightsId* rightsId) {
    if (hosversionBefore(4,0,0)) {
        return MAKERESULT(Module_Libnx, LibnxError_IncompatSysVer);
    }

    struct {
        u64 ticket_size;
        u64 cert_size;
    } out;

    const Result rc = serviceDispatchInOut(&g_esSrv, 22, *rightsId, out);
    if (R_SUCCEEDED(rc)) {
        *tik_size_out = out.ticket_size;
        *cert_size_out = out.cert_size;
    }

    return rc;
}

Result GetCommonTicketAndCertificateData(u64 *tik_size_out, u64 *cert_size_out, void* tik_buf, u64 tik_size, void* cert_buf, u64 cert_size, const FsRightsId* rightsId) {
    if (hosversionBefore(4,0,0)) {
        return MAKERESULT(Module_Libnx, LibnxError_IncompatSysVer);
    }

    struct {
        u64 ticket_size;
        u64 cert_size;
    } out;

    const Result rc = serviceDispatchInOut(&g_esSrv, 23, *rightsId, out,
        .buffer_attrs = { SfBufferAttr_HipcMapAlias | SfBufferAttr_Out, SfBufferAttr_HipcMapAlias | SfBufferAttr_Out },
        .buffers = { { tik_buf, tik_size }, { cert_buf, cert_size } }
    );

    if (R_SUCCEEDED(rc)) {
        *tik_size_out = out.ticket_size;
        *cert_size_out = out.cert_size;
    }

    return rc;
}

typedef enum {
    TikPropertyMask_None                 = 0,
    TikPropertyMask_PreInstallation      = BIT(0),  ///< Determines if the title comes pre-installed on the device. Most likely unused -- a remnant from previous ticket formats.
    TikPropertyMask_SharedTitle          = BIT(1),  ///< Determines if the title holds shared contents only. Most likely unused -- a remnant from previous ticket formats.
    TikPropertyMask_AllContents          = BIT(2),  ///< Determines if the content index mask shall be bypassed. Most likely unused -- a remnant from previous ticket formats.
    TikPropertyMask_DeviceLinkIndepedent = BIT(3),  ///< Determines if the console should *not* connect to the Internet to verify if the title's being used by the primary console.
    TikPropertyMask_Volatile             = BIT(4),  ///< Determines if the ticket copy inside ticket.bin is available after reboot. Can be encrypted.
    TikPropertyMask_ELicenseRequired     = BIT(5),  ///< Determines if the console should connect to the Internet to perform license verification.
    TikPropertyMask_Count                = 6        ///< Total values supported by this enum.
} TikPropertyMask;

Result GetTicketDataOffset(std::span<const u8> ticket, u64& out) {
    log_write("inside es\n");
    u32 signature_type;
    std::memcpy(std::addressof(signature_type), ticket.data(), sizeof(signature_type));

    u32 signature_size;
    switch (signature_type) {
        case es::TicketSigantureType_RSA_4096_SHA1: log_write("RSA-4096 PKCS#1 v1.5 with SHA-1\n"); signature_size = 0x200; break;
        case es::TicketSigantureType_RSA_2048_SHA1: log_write("RSA-2048 PKCS#1 v1.5 with SHA-1\n"); signature_size = 0x100; break;
        case es::TicketSigantureType_ECDSA_SHA1: log_write("ECDSA with SHA-1\n"); signature_size = 0x3C; break;
        case es::TicketSigantureType_RSA_4096_SHA256: log_write("RSA-4096 PKCS#1 v1.5 with SHA-256\n"); signature_size = 0x200; break;
        case es::TicketSigantureType_RSA_2048_SHA256: log_write("RSA-2048 PKCS#1 v1.5 with SHA-256\n"); signature_size = 0x100; break;
        case es::TicketSigantureType_ECDSA_SHA256: log_write("ECDSA with SHA-256\n"); signature_size = 0x3C; break;
        case es::TicketSigantureType_HMAC_SHA1_160: log_write("HMAC-SHA1-160\n"); signature_size = 0x14; break;
        default: log_write("unknown ticket\n"); return 0x1;
    }

    // align-up to 0x40.
    out = ((signature_size + sizeof(signature_type)) + 0x3F) & ~0x3F;
    R_SUCCEED();
}

Result GetTicketData(std::span<const u8> ticket, es::TicketData* out) {
    u64 data_off;
    R_TRY(GetTicketDataOffset(ticket, data_off));
    std::memcpy(out, ticket.data() + data_off, sizeof(*out));

    // validate ticket data.
    R_UNLESS(out->ticket_version1 == 0x2, Result_InvalidTicketVersion); // must be version 2.
    R_UNLESS(out->title_key_type == es::TicketTitleKeyType_Common || out->title_key_type == es::TicketTitleKeyType_Personalized, Result_InvalidTicketKeyType);
    R_UNLESS(out->master_key_revision <= 0x20, Result_InvalidTicketKeyRevision);

    R_SUCCEED();
}

Result SetTicketData(std::span<u8> ticket, const es::TicketData* in) {
    u64 data_off;
    R_TRY(GetTicketDataOffset(ticket, data_off));
    std::memcpy(ticket.data() + data_off, in, sizeof(*in));
    R_SUCCEED();
}

Result GetTitleKey(keys::KeyEntry& out, const TicketData& data, const keys::Keys& keys) {
    if (data.title_key_type == es::TicketTitleKeyType_Common) {
        std::memcpy(std::addressof(out), data.title_key_block, sizeof(out));
    } else if (data.title_key_type == es::TicketTitleKeyType_Personalized) {
        auto rsa_key = (const es::EticketRsaDeviceKey*)keys.eticket_device_key.key;
        log_write("personalised ticket\n");
        log_write("master_key_revision: %u\n", data.master_key_revision);
        log_write("license_type: %u\n", data.license_type);
        log_write("properties_bitfield: 0x%X\n", data.properties_bitfield);
        log_write("device_id: 0x%lX vs 0x%lX\n", data.device_id, std::byteswap(rsa_key->device_id));

        R_UNLESS(data.device_id == std::byteswap(rsa_key->device_id), 0x1);
        log_write("device id is same\n");

        u8 out_keydata[RSA2048_BYTES]{};
        size_t out_keydata_size;
        R_UNLESS(rsa2048OaepDecrypt(out_keydata, sizeof(out_keydata), data.title_key_block, rsa_key->modulus, &rsa_key->public_exponent, sizeof(rsa_key->public_exponent), rsa_key->private_exponent, sizeof(rsa_key->private_exponent), NULL, 0, &out_keydata_size), 0x1);
        R_UNLESS(out_keydata_size >= sizeof(out), 0x1);
        std::memcpy(std::addressof(out), out_keydata, sizeof(out));
    } else {
        R_THROW(0x1);
    }

    R_SUCCEED();
}

Result DecryptTitleKey(keys::KeyEntry& out, u8 key_gen, const keys::Keys& keys) {
    keys::KeyEntry title_kek;
    R_TRY(keys.GetTitleKek(std::addressof(title_kek), key_gen));
    crypto::cryptoAes128(std::addressof(out), std::addressof(out), std::addressof(title_kek), false);

    R_SUCCEED();
}

// todo: i thought i already wrote the code for this??
// todo: patch the ticket.
Result PatchTicket(std::span<u8> ticket, const keys::Keys& keys) {
    TicketData data;
    R_TRY(GetTicketData(ticket, &data));

    if (data.title_key_type == es::TicketTitleKeyType_Common) {
        // todo: verify common signature
    } else if (data.title_key_type == es::TicketTitleKeyType_Personalized) {

    }

    R_SUCCEED();
}

} // namespace sphaira::es
