/*
* Notes:
* - nca's that use title key encryption are decrypted using Tegra SE, whereas
*   standard crypto uses software decryption.
*   The latter is almost always (slightly) faster, and removed the need for es patch.
*/

#pragma once

#include "fs.hpp"
#include "source/base.hpp"
#include "container/base.hpp"
#include "nx/ncm.hpp"
#include "ui/progress_box.hpp"
#include <memory>
#include <optional>

namespace sphaira::yati {

enum { YatiModule = 521 };

enum : Result {
    // unkown container for the source provided.
    Result_ContainerNotFound = MAKERESULT(YatiModule, 10),
    Result_Cancelled = MAKERESULT(YatiModule, 11),

    // nca required by the cnmt but not found in collection.
    Result_NcaNotFound = MAKERESULT(YatiModule, 30),
    Result_InvalidNcaReadSize = MAKERESULT(YatiModule, 31),
    Result_InvalidNcaSigKeyGen = MAKERESULT(YatiModule, 32),
    Result_InvalidNcaMagic = MAKERESULT(YatiModule, 33),
    Result_InvalidNcaSignature0 = MAKERESULT(YatiModule, 34),
    Result_InvalidNcaSignature1 = MAKERESULT(YatiModule, 35),
    // invalid sha256 over the entire nca.
    Result_InvalidNcaSha256 = MAKERESULT(YatiModule, 36),

    // section could not be found.
    Result_NczSectionNotFound = MAKERESULT(YatiModule, 50),
    // section count == 0.
    Result_InvalidNczSectionCount = MAKERESULT(YatiModule, 51),
    // block could not be found.
    Result_NczBlockNotFound = MAKERESULT(YatiModule, 52),
    // block version != 2.
    Result_InvalidNczBlockVersion = MAKERESULT(YatiModule, 53),
    // block type != 1.
    Result_InvalidNczBlockType = MAKERESULT(YatiModule, 54),
    // block count == 0.
    Result_InvalidNczBlockTotal = MAKERESULT(YatiModule, 55),
    // block size exponent < 14 || > 32.
    Result_InvalidNczBlockSizeExponent = MAKERESULT(YatiModule, 56),
    // zstd error while decompressing ncz.
    Result_InvalidNczZstdError = MAKERESULT(YatiModule, 57),

    // nca has rights_id but matching ticket wasn't found.
    Result_TicketNotFound = MAKERESULT(YatiModule, 70),
    // found ticket has missmatching rights_id from it's name.
    Result_InvalidTicketBadRightsId = MAKERESULT(YatiModule, 71),
    Result_InvalidTicketVersion = MAKERESULT(YatiModule, 72),
    Result_InvalidTicketKeyType = MAKERESULT(YatiModule, 73),
    Result_InvalidTicketKeyRevision = MAKERESULT(YatiModule, 74),

    // cert not found for the ticket.
    Result_CertNotFound = MAKERESULT(YatiModule, 90),

    // unable to fetch header from ncm database.
    Result_NcmDbCorruptHeader = MAKERESULT(YatiModule, 110),
    // unable to total infos from ncm database.
    Result_NcmDbCorruptInfos = MAKERESULT(YatiModule, 111),
};

struct Config {
    bool sd_card_install{};

    // enables downgrading patch / data patch (dlc) version.
    bool allow_downgrade{};

    // ignores the install if already installed.
    // checks that every nca is available.
    bool skip_if_already_installed{};

    // installs tickets only.
    bool ticket_only{};

    // flags to enable / disable install of specific types.
    bool skip_base{};
    bool skip_patch{};
    bool skip_addon{};
    bool skip_data_patch{};
    bool skip_ticket{};

    // enables the option to skip sha256 verification.
    bool skip_nca_hash_verify{};

    // enables the option to skip rsa nca fixed key verification.
    bool skip_rsa_header_fixed_key_verify{};

    // enables the option to skip rsa npdm fixed key verification.
    bool skip_rsa_npdm_fixed_key_verify{};

    // if set, it will ignore the distribution bit in the nca header.
    bool ignore_distribution_bit{};

    // converts titlekey to standard crypto, also known as "ticketless".
    // this will not work with addon (dlc), so, addon tickets will be installed.
    bool convert_to_standard_crypto{};

    // encrypts the keak with master key 0, this allows the game to be launched on every fw.
    // implicitly performs standard crypto.
    bool lower_master_key{};

    // sets the system_firmware field in the cnmt extended header.
    // if mkey is higher than fw version, the game still won't launch
    // as the fw won't have the key to decrypt keak.
    bool lower_system_version{};
};

// overridable options, set to avoid
struct ConfigOverride {
    std::optional<bool> sd_card_install{};
    std::optional<bool> skip_nca_hash_verify{};
    std::optional<bool> skip_rsa_header_fixed_key_verify{};
    std::optional<bool> skip_rsa_npdm_fixed_key_verify{};
    std::optional<bool> ignore_distribution_bit{};
    std::optional<bool> convert_to_standard_crypto{};
    std::optional<bool> lower_master_key{};
    std::optional<bool> lower_system_version{};
};

Result InstallFromFile(ui::ProgressBox* pbox, FsFileSystem* fs, const fs::FsPath& path, const ConfigOverride& override = {});
Result InstallFromStdioFile(ui::ProgressBox* pbox, const fs::FsPath& path, const ConfigOverride& override = {});
Result InstallFromSource(ui::ProgressBox* pbox, std::shared_ptr<source::Base> source, const fs::FsPath& path, const ConfigOverride& override = {});
Result InstallFromContainer(ui::ProgressBox* pbox, std::shared_ptr<container::Base> container, const ConfigOverride& override = {});
Result InstallFromCollections(ui::ProgressBox* pbox, std::shared_ptr<source::Base> source, const container::Collections& collections, const ConfigOverride& override = {});

Result ParseCnmtNca(const fs::FsPath& path,  ncm::PackagedContentMeta& header, std::vector<u8>& extended_header, std::vector<NcmPackagedContentInfo>& infos);
Result ParseControlNca(const fs::FsPath& path, u64 id, void* nacp_out = nullptr, s64 nacp_size = 0, std::vector<u8>* icon_out = nullptr);

} // namespace sphaira::yati
