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

Result EncyrptDecryptTitleKey(keys::KeyEntry& out, u8 key_gen, const keys::Keys& keys, bool is_encryptor) {
    keys::KeyEntry title_kek;
    R_TRY(keys.GetTitleKek(std::addressof(title_kek), key_gen));
    crypto::cryptoAes128(std::addressof(out), std::addressof(out), std::addressof(title_kek), is_encryptor);
    R_SUCCEED();
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

Result GetTicketDataOffset(std::span<const u8> ticket, u64& out, bool is_cert) {
    log_write("inside es\n");
    u32 signature_type;
    std::memcpy(std::addressof(signature_type), ticket.data(), sizeof(signature_type));

    if (is_cert) {
        signature_type = std::byteswap(signature_type);
    }

    switch (signature_type) {
        case SigType_Rsa4096Sha1: log_write("RSA-4096 PKCS#1 v1.5 with SHA-1\n"); out = sizeof(SignatureBlockRsa4096); break;
        case SigType_Rsa2048Sha1: log_write("RSA-2048 PKCS#1 v1.5 with SHA-1\n"); out = sizeof(SignatureBlockRsa2048); break;
        case SigType_Ecc480Sha1: log_write("ECDSA with SHA-1\n"); out = sizeof(SignatureBlockEcc480); break;
        case SigType_Rsa4096Sha256: log_write("RSA-4096 PKCS#1 v1.5 with SHA-256\n"); out = sizeof(SignatureBlockRsa4096); break;
        case SigType_Rsa2048Sha256: log_write("RSA-2048 PKCS#1 v1.5 with SHA-256\n"); out = sizeof(SignatureBlockRsa2048); break;
        case SigType_Ecc480Sha256: log_write("ECDSA with SHA-256\n"); out = sizeof(SignatureBlockEcc480); break;
        case SigType_Hmac160Sha1: log_write("HMAC-SHA1-160\n"); out = sizeof(SignatureBlockHmac160); break;
        default: log_write("unknown ticket: %u\n", signature_type); R_THROW(Result_EsBadTitleKeyType);
    }

    R_SUCCEED();
}

Result GetTicketData(std::span<const u8> ticket, es::TicketData* out) {
    u64 data_off;
    R_TRY(GetTicketDataOffset(ticket, data_off));
    if (ticket.size() < data_off + sizeof(*out)) {
        log_write("[ES] invalid ticket size: %zu vs %zu\n", ticket.size(), data_off + sizeof(*out));
        R_THROW(Result_EsBadTicketSize);
    }

    std::memcpy(out, ticket.data() + data_off, sizeof(*out));

    // validate ticket data.
    log_write("[ES] validating ticket data\n");
    R_UNLESS(out->format_version == 0x2, Result_EsInvalidTicketFromatVersion); // must be version 2.
    R_UNLESS(out->title_key_type == es::TitleKeyType_Common || out->title_key_type == es::TitleKeyType_Personalized, Result_EsInvalidTicketKeyType);
    R_UNLESS(out->master_key_revision <= 0x20, Result_EsInvalidTicketKeyRevision);
    log_write("[ES] valid ticket data\n");

    R_SUCCEED();
}

Result GetTitleKey(keys::KeyEntry& out, const TicketData& data, const keys::Keys& keys) {
    if (data.title_key_type == es::TitleKeyType_Common) {
        std::memcpy(std::addressof(out), data.title_key_block, sizeof(out));
    } else if (data.title_key_type == es::TitleKeyType_Personalized) {
        auto rsa_key = (const es::EticketRsaDeviceKey*)keys.eticket_device_key.key;
        log_write("personalised ticket\n");
        log_write("master_key_revision: %u\n", data.master_key_revision);
        log_write("license_type: %u\n", data.license_type);
        log_write("properties_bitfield: 0x%X\n", data.properties_bitfield);
        log_write("device_id: 0x%lX vs 0x%lX\n", data.device_id, std::byteswap(rsa_key->device_id));

        R_UNLESS(data.device_id == std::byteswap(rsa_key->device_id), Result_EsPersonalisedTicketDeviceIdMissmatch);
        log_write("device id is same\n");

        u8 out_keydata[RSA2048_BYTES]{};
        size_t out_keydata_size;
        R_UNLESS(rsa2048OaepDecrypt(out_keydata, sizeof(out_keydata), data.title_key_block, rsa_key->modulus, &rsa_key->public_exponent, sizeof(rsa_key->public_exponent), rsa_key->private_exponent, sizeof(rsa_key->private_exponent), NULL, 0, &out_keydata_size), Result_EsFailedDecryptPersonalisedTicket);
        R_UNLESS(out_keydata_size >= sizeof(out), Result_EsBadDecryptedPersonalisedTicketSize);
        std::memcpy(std::addressof(out), out_keydata, sizeof(out));
    } else {
        R_THROW(Result_EsBadTitleKeyType);
    }

    R_SUCCEED();
}

Result DecryptTitleKey(keys::KeyEntry& out, u8 key_gen, const keys::Keys& keys) {
    return EncyrptDecryptTitleKey(out, key_gen, keys, false);
}

Result EncryptTitleKey(keys::KeyEntry& out, u8 key_gen, const keys::Keys& keys) {
    return EncyrptDecryptTitleKey(out, key_gen, keys, true);
}

// this function is taken from nxdumptool
Result ShouldPatchTicket(const TicketData& data, std::span<const u8> ticket, std::span<const u8> cert_chain, bool patch_personalised, bool& should_patch) {
    should_patch = false;

    if (data.title_key_type == es::TitleKeyType_Common) {
        SigType tik_sig_type;
        std::memcpy(&tik_sig_type, ticket.data(), sizeof(tik_sig_type));

        if (tik_sig_type != SigType_Rsa2048Sha256) {
            R_SUCCEED();
        }

        const auto cert_name = std::strrchr(data.issuer, '-') + 1;
        R_UNLESS(cert_name, Result_EsBadTitleKeyType);
        const auto cert_name_span = std::span{(const u8*)cert_name, std::strlen(cert_name)};

        // find the cert from inside the cert chain.
        const auto it = std::ranges::search(cert_chain, cert_name_span);
        R_UNLESS(!it.empty(), Result_EsBadTitleKeyType);
        const auto cert = cert_chain.subspan(std::distance(cert_chain.begin(), it.begin()) - offsetof(CertHeader, subject));

        const auto cert_header = (const CertHeader*)cert.data();
        const auto pub_key_type = std::byteswap<u32>(cert_header->pub_key_type);
        log_write("[ES] cert_header->issuer: %s\n", cert_header->issuer);
        log_write("[ES] cert_header->pub_key_type: %u\n", pub_key_type);
        log_write("[ES] cert_header->subject: %s\n", cert_header->subject);

        std::span<const u8> public_key{};
        u32 public_exponent{};

        switch (pub_key_type) {
            case PubKeyType_Rsa4096: {
                auto pub_key = (const PublicKeyBlockRsa4096*)(cert.data() + sizeof(CertHeader));
                public_key = pub_key->public_key;
                public_exponent = pub_key->public_exponent;
            }   break;
            case PubKeyType_Rsa2048: {
                auto pub_key = (const PublicKeyBlockRsa2048*)(cert.data() + sizeof(CertHeader));
                public_key = pub_key->public_key;
                public_exponent = pub_key->public_exponent;
            }   break;
            case PubKeyType_Ecc480: {
                R_SUCCEED();
            }   break;
            default:
                R_THROW(Result_EsBadTitleKeyType);
        }

        const auto tik = (const TicketRsa2048*)ticket.data();
        const auto check_data = ticket.subspan(offsetof(TicketRsa2048, data));

        if (rsa2048VerifySha256BasedPkcs1v15Signature(check_data.data(), check_data.size(), tik->signature_block.sign, public_key.data(), &public_exponent, sizeof(public_exponent))) {
            log_write("[ES] common ticket is same\n");
        } else {
            log_write("[ES] common ticket is modified\n");
            should_patch = true;
        }

        R_SUCCEED();
    } else if (data.title_key_type == es::TitleKeyType_Personalized) {
        if (patch_personalised) {
            log_write("[ES] patching personalised ticket\n");
        } else {
            log_write("[ES] keeping personalised ticket\n");
        }

        should_patch = patch_personalised;
        R_SUCCEED();
    } else {
        R_THROW(Result_EsBadTitleKeyType);
    }
}

Result ShouldPatchTicket(std::span<const u8> ticket, std::span<const u8> cert_chain, bool patch_personalised, bool& should_patch) {
    TicketData data;
    R_TRY(GetTicketData(ticket, &data));

    return ShouldPatchTicket(data, ticket, cert_chain, patch_personalised, should_patch);
}

Result PatchTicket(std::vector<u8>& ticket, std::span<const u8> cert_chain, u8 key_gen, const keys::Keys& keys, bool patch_personalised) {
    TicketData data;
    R_TRY(GetTicketData(ticket, &data));

    // check if we should create a fake common ticket.
    bool should_patch;
    R_TRY(ShouldPatchTicket(data, ticket, cert_chain, patch_personalised, should_patch));

    if (!should_patch) {
        R_SUCCEED();
    }

    // store copy of rights id an title key.
    keys::KeyEntry title_key;
    R_TRY(GetTitleKey(title_key, data, keys));
    const auto rights_id = data.rights_id;

    // following StandardNSP format.
    TicketRsa2048 out{};
    out.signature_block.sig_type = SigType_Rsa2048Sha256;
    std::memset(out.signature_block.sign, 0xFF, sizeof(out.signature_block.sign));
    std::strcpy(out.data.issuer, "Root-CA00000003-XS00000020");
    std::memcpy(out.data.title_key_block, title_key.key, sizeof(title_key.key));
    out.data.format_version = 0x2;
    out.data.master_key_revision = key_gen;
    out.data.rights_id = rights_id;
    out.data.sect_hdr_offset = ticket.size();

    // overwrite old ticket with new fake ticket data.
    ticket.resize(sizeof(out));
    std::memcpy(ticket.data(), &out, sizeof(out));

    R_SUCCEED();
}

} // namespace sphaira::es
