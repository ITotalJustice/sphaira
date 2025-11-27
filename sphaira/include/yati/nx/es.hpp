#pragma once

#include <switch.h>
#include <span>
#include <vector>
#include "ncm.hpp"
#include "keys.hpp"

namespace sphaira::es {

enum TitleKeyType : u8 {
    TitleKeyType_Common = 0,
    TitleKeyType_Personalized = 1,
};

enum SigType : u32 {
    SigType_Rsa4096Sha1   = 65536,
    SigType_Rsa2048Sha1   = 65537,
    SigType_Ecc480Sha1    = 65538,
    SigType_Rsa4096Sha256 = 65539,
    SigType_Rsa2048Sha256 = 65540,
    SigType_Ecc480Sha256  = 65541,
    SigType_Hmac160Sha1   = 65542
};

enum PubKeyType : u32 {
    PubKeyType_Rsa4096 = 0,
    PubKeyType_Rsa2048 = 1,
    PubKeyType_Ecc480  = 2
};

struct SignatureBlockRsa4096 {
    SigType sig_type;
    u8 sign[0x200];
    u8 reserved_1[0x3C];
};
static_assert(sizeof(SignatureBlockRsa4096) == 0x240);

struct SignatureBlockRsa2048 {
    SigType sig_type;
    u8 sign[0x100];
    u8 reserved_1[0x3C];
};
static_assert(sizeof(SignatureBlockRsa2048) == 0x140);

struct SignatureBlockEcc480 {
    SigType sig_type;
    u8 sign[0x3C];
    u8 reserved_1[0x40];
};
static_assert(sizeof(SignatureBlockEcc480) == 0x80);

struct SignatureBlockHmac160 {
    SigType sig_type;
    u8 sign[0x14];
    u8 reserved_1[0x28];
};
static_assert(sizeof(SignatureBlockHmac160) == 0x40);

struct CertHeader {
    char issuer[0x40];
    PubKeyType pub_key_type;
    char subject[0x40]; /* ServerId, DeviceId */
    u32 date;
};
static_assert(sizeof(CertHeader) == 0x88);

struct PublicKeyBlockRsa4096 {
    u8 public_key[0x200];
    u32 public_exponent;
    u8 reserved_1[0x34];
};
static_assert(sizeof(PublicKeyBlockRsa4096) == 0x238);

struct PublicKeyBlockRsa2048 {
    u8 public_key[0x100];
    u32 public_exponent;
    u8 reserved_1[0x34];
};
static_assert(sizeof(PublicKeyBlockRsa2048) == 0x138);

struct PublicKeyBlockEcc480 {
    u8 public_key[0x3C];
    u8 reserved_1[0x3C];
};
static_assert(sizeof(PublicKeyBlockEcc480) == 0x78);

template<typename Sig, typename Pub>
struct Cert {
    Sig signature_block;
    CertHeader cert_header;
    Pub public_key_block;
};

using CertRsa4096PubRsa4096 = Cert<SignatureBlockRsa4096, PublicKeyBlockRsa4096>;
using CertRsa4096PubRsa2048 = Cert<SignatureBlockRsa4096, PublicKeyBlockRsa2048>;
using CertRsa4096PubEcc480 = Cert<SignatureBlockRsa4096, PublicKeyBlockEcc480>;

using CertRsa2048PubRsa4096 = Cert<SignatureBlockRsa2048, PublicKeyBlockRsa4096>;
using CertRsa2048PubRsa2048 = Cert<SignatureBlockRsa2048, PublicKeyBlockRsa2048>;
using CertRsa2048PubEcc480 = Cert<SignatureBlockRsa2048, PublicKeyBlockEcc480>;

using CertEcc480PubRsa4096 = Cert<SignatureBlockEcc480, PublicKeyBlockRsa4096>;
using CertEcc480PubRsa2048 = Cert<SignatureBlockEcc480, PublicKeyBlockRsa2048>;
using CertEcc480PubEcc480 = Cert<SignatureBlockEcc480, PublicKeyBlockEcc480>;

using CertHmac160PubRsa4096 = Cert<SignatureBlockHmac160, PublicKeyBlockRsa4096>;
using CertHmac160PubRsa2048 = Cert<SignatureBlockHmac160, PublicKeyBlockRsa2048>;
using CertHmac160PubEcc480 = Cert<SignatureBlockHmac160, PublicKeyBlockEcc480>;

static_assert(sizeof(CertRsa4096PubRsa4096) == 0x500);
static_assert(sizeof(CertRsa4096PubRsa2048) == 0x400);
static_assert(sizeof(CertRsa4096PubEcc480) == 0x340);
static_assert(sizeof(CertRsa2048PubRsa4096) == 0x400);
static_assert(sizeof(CertRsa2048PubRsa2048) == 0x300);
static_assert(sizeof(CertRsa2048PubEcc480) == 0x240);
static_assert(sizeof(CertEcc480PubRsa4096) == 0x340);
static_assert(sizeof(CertEcc480PubRsa2048) == 0x240);
static_assert(sizeof(CertEcc480PubEcc480) == 0x180);
static_assert(sizeof(CertHmac160PubRsa4096) == 0x300);
static_assert(sizeof(CertHmac160PubRsa2048) == 0x200);
static_assert(sizeof(CertHmac160PubEcc480) == 0x140);

struct TicketData {
    char issuer[0x40];
    u8 title_key_block[0x100];
    u8 format_version;
    u8 title_key_type;
    u16 version;
    TitleKeyType license_type;
    u8 master_key_revision;
    u16 properties_bitfield;
    u8 _0x148[0x8];
    u64 ticket_id;
    u64 device_id;
    FsRightsId rights_id;
    u32 account_id;
    u32 sect_total_size;
    u32 sect_hdr_offset;
    u16 sect_hdr_count;
    u16 sect_hdr_entry_size;
};
static_assert(sizeof(TicketData) == 0x180);

template<typename Sig>
struct Ticket {
    Sig signature_block;
    TicketData data;
};

using TicketRsa4096 = Ticket<SignatureBlockRsa4096>;
using TicketRsa2048 = Ticket<SignatureBlockRsa2048>;
using TicketEcc480 = Ticket<SignatureBlockEcc480>;
using TicketHmac160 = Ticket<SignatureBlockHmac160>;

static_assert(sizeof(TicketRsa4096) == 0x3C0);
static_assert(sizeof(TicketRsa2048) == 0x2C0);
static_assert(sizeof(TicketEcc480) == 0x200);
static_assert(sizeof(TicketHmac160) == 0x1C0);

struct EticketRsaDeviceKey {
    u8 ctr[AES_128_KEY_SIZE];
    u8 private_exponent[0x100];
    u8 modulus[0x100];
    u32 public_exponent;                ///< Stored using big endian byte order. Must match ETICKET_RSA_DEVICE_KEY_PUBLIC_EXPONENT.
    u8 padding[0x14];
    u64 device_id;
    u8 ghash[0x10];
};
static_assert(sizeof(EticketRsaDeviceKey) == 0x240);

// es functions.
Result Initialize();
void Exit();
Service* GetServiceSession();

// todo: find the ipc that gets personalised tickets.
// todo: if ipc doesn't exist, manually parse es personalised save.
// todo: add personalised -> common ticket conversion.
// todo: make the above an option for both dump and install.

Result ImportTicket(const void* tik_buf, u64 tik_size, const void* cert_buf, u64 cert_size);
Result CountCommonTicket(s32* count);
Result CountPersonalizedTicket(s32* count);
Result ListCommonTicket(s32 *out_entries_written, FsRightsId* out_ids, s32 count);
Result ListPersonalizedTicket(s32 *out_entries_written, FsRightsId* out_ids, s32 count);
Result ListMissingPersonalizedTicket(s32 *out_entries_written, FsRightsId* out_ids, s32 count); // untested
Result GetCommonTicketSize(u64 *size_out, const FsRightsId* rightsId);
Result GetCommonTicketData(u64 *size_out, void *tik_data, u64 tik_size, const FsRightsId* rightsId);
Result GetCommonTicketAndCertificateSize(u64 *tik_size_out, u64 *cert_size_out, const FsRightsId* rightsId); // [4.0.0+]
Result GetCommonTicketAndCertificateData(u64 *tik_size_out, u64 *cert_size_out, void* tik_buf, u64 tik_size, void* cert_buf, u64 cert_size, const FsRightsId* rightsId); // [4.0.0+]

// ticket functions.
Result GetTicketDataOffset(std::span<const u8> ticket, u64& out, bool is_cert = false);
Result GetTicketData(std::span<const u8> ticket, es::TicketData* out);

// gets the title key and performs RSA-2048-OAEP if needed.
Result GetTitleKey(keys::KeyEntry& out, const TicketData& data, const keys::Keys& keys);
Result DecryptTitleKey(keys::KeyEntry& out, u8 key_gen, const keys::Keys& keys);
Result EncryptTitleKey(keys::KeyEntry& out, u8 key_gen, const keys::Keys& keys);

Result ShouldPatchTicket(const TicketData& data, std::span<const u8> ticket, std::span<const u8> cert_chain, bool patch_personalised, bool& should_patch);
Result ShouldPatchTicket(std::span<const u8> ticket, std::span<const u8> cert_chain, bool patch_personalised, bool& should_patch);
// cert chain may be modified if the ticket is converted to common ticket.
Result PatchTicket(std::vector<u8>& ticket, std::vector<u8>& cert_chain, u8 key_gen, const keys::Keys& keys, bool patch_personalised);

// fills out with the list of common / personalised rights ids.
Result GetCommonTickets(std::vector<FsRightsId>& out);
Result GetPersonalisedTickets(std::vector<FsRightsId>& out);

// checks if the rights id is found in common / personalised.
Result IsRightsIdCommon(const FsRightsId& id, bool* out);
Result IsRightsIdPersonalised(const FsRightsId& id, bool* out);

// helper for the above if the db has already been parsed.
bool IsRightsIdValid(const FsRightsId& id);
bool IsRightsIdFound(const FsRightsId& id, std::span<const FsRightsId> ids);

// wrapper around ipc.
Result GetCommonTicketAndCertificate(const FsRightsId& rights_id, std::vector<u8>& tik_out, std::vector<u8>& cert_out);
// fetches data from system es save.
Result GetPersonalisedTicketData(u64 *size_out, void *tik_data, u64 tik_size, const FsRightsId* rightsId);
Result GetPersonalisedTicketAndCertificate(const FsRightsId& rights_id, std::vector<u8>& tik_out, std::vector<u8>& cert_out);

// fills out with the decrypted title key.
Result GetTitleKeyDecrypted(const FsRightsId& rights_id, u8 key_gen, const keys::Keys& keys, keys::KeyEntry& out);
Result GetTitleKeyDecrypted(std::span<const u8> ticket, const FsRightsId& rights_id, u8 key_gen, const keys::Keys& keys, keys::KeyEntry& out);

} // namespace sphaira::es
