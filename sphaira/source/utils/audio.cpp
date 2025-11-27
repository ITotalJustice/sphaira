#include "utils/audio.hpp"
#include "utils/profile.hpp"
#include "utils/thread.hpp"
#include "utils/devoptab_common.hpp"
#include "yati/source/file.hpp"

#include "app.hpp"
#include "log.hpp"

// sizes of all formats (stacked).
// 2.6 MiB (2,708,685) without                (+0k   | +0k)
// 2.6 MiB (2,733,261) with wav               (+25k  | +25k)
// 2.6 MiB (2,761,933) with mp3               (+53k  | +28k)
// 2.6 MiB (2,766,029) with mp3 with id3      (+58k  | +05k)
// 2.7 MiB (2,798,797) with flac              (+90k  | +32k)
// 2.7 MiB (2,831,565) with stb ogg           (+112k | +33k)

// wav files are common in game romfs, mp3 is overal very common for general use.
// flac isn't really used, and imo not worth including.

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Walloca"

#ifdef ENABLE_AUDIO_FLAC
    #define DR_FLAC_IMPLEMENTATION
    #define DR_FLAC_NO_OGG
    #define DR_FLAC_NO_STDIO
    #define DRFLAC_API static
    #define DRFLAC_PRIVATE static
    #include <dr_flac.h>
#endif // ENABLE_AUDIO_FLAC

#ifdef ENABLE_AUDIO_WAV
    #define DR_WAV_IMPLEMENTATION
    #define DR_WAV_NO_STDIO
    #define DRWAV_API static
    #define DRWAV_PRIVATE static
    #include <dr_wav.h>
#endif // ENABLE_AUDIO_WAV

#ifdef ENABLE_AUDIO_MP3
    #define DR_MP3_IMPLEMENTATION
    #define DR_MP3_NO_STDIO
    #define DRMP3_API static
    #define DRMP3_PRIVATE static
    // improves load / seek times.
    // hopefully drmp3 will have binary seek rather than linear.
    // this also improves
    #define DRMP3_DATA_CHUNK_SIZE (1024*64)
    #include <dr_mp3.h>
    #include <id3v2lib.h>
#endif // ENABLE_AUDIO_MP3

#ifdef ENABLE_AUDIO_OGG
    #if 0
    #define DR_VORBIS_IMPLEMENTATION
    #define DR_VORBIS_NO_STDIO
    #define DR_VORBIS_API static
    #include "dr_vorbis.h"
    #endif
    #define STB_VORBIS_NO_PUSHDATA_API
    #define STB_VORBIS_NO_STDIO
    #define STB_VORBIS_NO_OPENMEM
    #include "stb_vorbis.h"
#endif // ENABLE_AUDIO_OGG

#pragma GCC diagnostic pop
#pragma GCC diagnostic pop
#pragma GCC diagnostic pop
#pragma GCC diagnostic pop

#include <pulsar.h>

namespace sphaira::audio {
namespace {

// todo: pr to libnx
static inline float audrvVoiceGetVolume(AudioDriver* d, int id)
{
    return d->in_voices[id].volume;
}

static inline float audrvVoiceGetPitch(AudioDriver* d, int id)
{
    return d->in_voices[id].pitch;
}

struct File {
    Result Open(fs::Fs* fs, const fs::FsPath& path) {
        auto source = std::make_shared<yati::source::File>(fs, path);
        R_TRY(source->GetSize(&m_size));

        m_buffered = std::make_unique<devoptab::common::BufferedData>(source, m_size, 1024*16);
        R_SUCCEED();
    }

    virtual size_t ReadFile(void* buf, size_t read_size) {
        u64 bytes_read = 0;
        this->m_buffered->Read(buf, m_offset, read_size, &bytes_read);
        this->m_offset += bytes_read;
        return bytes_read;
    }

    bool SeekFile(s64 offset, int origin) {
        if (origin == SEEK_CUR) {
            offset += this->m_offset;
        } else if (origin == SEEK_END) {
            offset = this->m_size + offset;
        }

        if (offset >= 0 && offset <= this->m_size) {
            this->m_offset = offset;
            return true;
        } else {
            return false;
        }
    }

    s64 TellFile() const {
        return this->m_offset;
    }

    s64 GetSize() const {
        return this->m_size;
    }

private:
    std::unique_ptr<devoptab::common::BufferedData> m_buffered{};
    s64 m_offset{};
    s64 m_size{};
};

#ifdef ENABLE_AUDIO_MP3
// gta vice "encrypted" mp3's using xor 0x22, very cool.
struct GTAViceCityFile final : File {
    size_t ReadFile(void* _buf, size_t read_size) override {
        auto buf = (u8*)_buf;
        const auto bytes_read = File::ReadFile(buf, read_size);

        for (size_t i = 0; i < bytes_read; i++) {
            buf[i] ^= 0x22;
        }

        return bytes_read;
    }
};

auto convert_utf16(const ID3v2_TextFrameData* data) -> std::string{
    const size_t len = std::max(data->size - 1, 0);

    if (!len || data->encoding != ID3v2_ENCODING_UNICODE) {
        return {data->text, len};
    }

    if ((u64)data->text & 0x1) {
        log_write("WARNING NOT ALIGNED\n");
    }

    char buf[255];
    const auto sz = utf16_to_utf8((u8*)buf, (u16*)data->text + 1, sizeof(buf) - 1);
    if (sz < 0) {
        return "";
    }

    buf[sz] = 0;
    return buf;
}
#endif // ENABLE_AUDIO_MP3

struct Base {
    virtual ~Base() = default;
    virtual Result LoadFile(fs::Fs* fs, const fs::FsPath& path, u32 flags) = 0;
    virtual Result Update(Progress& out, State& state) = 0;
    virtual Result Seek(u64 target) = 0;

    virtual void GetMeta(Meta* out) {
        *out = {};
    }

    void GetInfo(Info* out) const {
        *out = m_info;
    }

    void GetVolume(float* out) const {
        *out = m_volume;
    }

    void GetPitch(float* out) const {
        *out = m_pitch;
    }

    void SetVolume(float in) {
        m_volume = std::clamp<float>(in, 0.0, 2.0);
    }

    void SetPitch(float in) {
        m_pitch = in;
    }

protected:
    Info m_info{};
    float m_volume{1};
    float m_pitch{1};
};

struct PlsrBase : Base {
    ~PlsrBase() {
        log_write("[MUSIC] destructor CALLED\n");
        plsrPlayerFree(m_id);
    }

    Result Update(Progress& out, State& state) override {
        auto player = plsrPlayerGetInstance();
        R_UNLESS(player, 0x1);

        // update state if needed.
        if (state == State::Playing || state == State::Paused) {
            const auto pause = state == State::Paused;
            for (u32 i = 0; i < m_id->channelCount; i++) {
                audrvVoiceSetPaused(&player->driver, m_id->channels[i].voiceId, pause);
            }
        }

        // set volume and pitch.
        for (u32 i = 0; i < m_id->channelCount; i++) {
            audrvVoiceSetVolume(&player->driver, m_id->channels[i].voiceId, m_volume);
            audrvVoiceSetPitch(&player->driver, m_id->channels[i].voiceId, m_pitch);
        }

        // update progress.
        R_TRY(audrvUpdate(&player->driver));
        out.played = audrvVoiceGetPlayedSampleCount(&player->driver, m_id->channels[0].voiceId);
        const auto loop_end = m_info.sample_count - m_info.loop_start;

        // wrap around to looping sample offset.
        if (out.played >= m_info.sample_count) {
            if (m_info.looping) {
                out.played -= m_info.loop_start;
                out.played = (out.played % loop_end) + m_info.loop_start;
            } else {
                state = State::Finished;
            }
        }

        R_SUCCEED();
    }

    virtual Result Seek(u64 target) {
        R_THROW(0x1);
    }

protected:
    PLSR_PlayerSoundId m_id{};
};

struct CustomBase : Base {
    using Decode = std::function<int(int samples, s16* out)>;

    virtual ~CustomBase() {
        if (m_drv) {
            if (m_voice_id >= 0) {
                audrvVoiceDrop(m_drv, m_voice_id);
            }

            audrvUpdate(m_drv);
            audrvMemPoolDetach(m_drv, m_memory_pool_id);

            // force remove.
            m_drv->in_mempools[m_memory_pool_id].state = AudioRendererMemPoolState_Invalid;
            audrvMemPoolRemove(m_drv, m_memory_pool_id);
        }
    }

    virtual u64 Tell() = 0;

    // taken from sys-tune.
    Result Create(int channel_count, int sample_rate, const Decode& decode) {
        auto player = plsrPlayerGetInstance();
        R_UNLESS(player, 0x1);
        m_drv = &player->driver;

        const int MinSampleCount  = 2048; // 21ms of audio at 44100
        const int MaxChannelCount = channel_count;
        const int AudioSampleSize = MinSampleCount * MaxChannelCount * sizeof(s16);
        const int AudioPoolSize   = ((AudioSampleSize * BufferCount) + 0xFFF) &~ 0xFFF;
        constexpr std::align_val_t pool_align{AUDREN_MEMPOOL_ALIGNMENT};

        /* Register memory pool. */
        m_aligned = std::make_unique<s16*>(new(pool_align) s16[AudioPoolSize / sizeof(s16)]);
        log_write("ptr: 0x%lX size: 0x%X\n", (u64)*m_aligned, AudioPoolSize);
        m_memory_pool_id = audrvMemPoolAdd(m_drv, *m_aligned, AudioPoolSize);
        log_write("got pool id: %d\n", m_memory_pool_id);
        R_UNLESS(audrvMemPoolAttach(m_drv, m_memory_pool_id), 0x1);

        m_voice_id = _getFreeVoiceId(player);
        log_write("got voice id: %d\n", m_voice_id);
        R_UNLESS(m_voice_id >= 0, 0x1);

        R_UNLESS(audrvVoiceInit(m_drv, m_voice_id, channel_count, PcmFormat_Int16, sample_rate), 0x1);
        audrvVoiceSetDestinationMix(m_drv, m_voice_id, AUDREN_FINAL_MIX_ID);

        if (channel_count == 1) {
            audrvVoiceSetMixFactor(m_drv, m_voice_id, 1.0f, 0, 0);
            audrvVoiceSetMixFactor(m_drv, m_voice_id, 1.0f, 0, 1);
        } else {
            audrvVoiceSetMixFactor(m_drv, m_voice_id, 1.0f, 0, 0);
            audrvVoiceSetMixFactor(m_drv, m_voice_id, 0.0f, 0, 1);
            audrvVoiceSetMixFactor(m_drv, m_voice_id, 0.0f, 1, 0);
            audrvVoiceSetMixFactor(m_drv, m_voice_id, 1.0f, 1, 1);
        }

        audrvVoiceStart(m_drv, m_voice_id);
        m_sample_count = AudioSampleSize / BufferCount / sizeof(s16);

        log_write("[AUDIO] pool size: %u sample_count: %d\n", AudioPoolSize, m_sample_count);

        for (int i = 0; i < BufferCount; i++) {
            m_buffers[i].data_pcm16          = *m_aligned;
            m_buffers[i].size                = AudioSampleSize;
            m_buffers[i].start_sample_offset = i * m_sample_count;
            m_buffers[i].end_sample_offset   = m_buffers[i].start_sample_offset + m_sample_count;
        }

        m_decode = decode;
        m_channel_count = channel_count;
        R_SUCCEED();
    }

    Result Update(Progress& out, State& state) override {
        if (state == State::Playing) {
            // set volume and pitch.
            audrvVoiceSetVolume(m_drv, m_voice_id, m_volume);
            audrvVoiceSetPitch(m_drv, m_voice_id, m_pitch);

            // update first to get the buffer status and apply volume/pitch.
            R_TRY(audrvUpdate(m_drv));

            // get the next free buffer.
            AudioDriverWaveBuf *refillBuf = nullptr;
            for (auto &buffer : m_buffers) {
                if (buffer.state == AudioDriverWaveBufState_Free || buffer.state == AudioDriverWaveBufState_Done) {
                    refillBuf = &buffer;
                    break;
                }
            }

            if (refillBuf) {
                s16 *data = *m_aligned + refillBuf->start_sample_offset * m_channel_count;

                const int nSamples = m_decode(m_sample_count, data);
                if (nSamples > 0) {
                    armDCacheFlush(data, nSamples * m_channel_count * sizeof(u16));
                    refillBuf->end_sample_offset = refillBuf->start_sample_offset + nSamples;

                    // todo: handle error here, should it fail.
                    if (!audrvVoiceAddWaveBuf(m_drv, m_voice_id, refillBuf)) {
                        R_THROW(0x1);
                    }

                    // update again as we pushed a new buffer.
                    R_TRY(audrvUpdate(m_drv));
                } else {
                    if (Tell() < m_info.sample_count) {
                        state = State::Error;
                    } else if (IsAllBuffersEmpty()) {
                        state = State::Finished;
                    }
                }
            }
        }

        // audrvVoiceGetPlayedSampleCount doesn't handle seek and has no way of adjusting :/
        // out.played = audrvVoiceGetPlayedSampleCount(m_drv, m_voice_id);
        out.played = Tell();

        R_SUCCEED();
    }

private:
    bool IsAllBuffersEmpty() const {
        for (auto &buffer : m_buffers) {
            if (buffer.state != AudioDriverWaveBufState_Free && buffer.state != AudioDriverWaveBufState_Done) {
                log_write("got state: %u\n", buffer.state);
                return false;
            }
        }
        return true;
    }

    static int _getFreeVoiceId(const PLSR_Player* player) {
        for(int id = player->config.startVoiceId; id <= player->config.endVoiceId; id++) {
            if(!player->driver.in_voices[id].is_used) {
                return id;
            }
        }

        return -1;
    }

private:
    static constexpr const int BufferCount = 2;

    AudioDriver* m_drv{};
    std::unique_ptr<s16*> m_aligned{};
    AudioDriverWaveBuf m_buffers[BufferCount]{};
    Decode m_decode{};
    int m_memory_pool_id{};
    int m_voice_id{-1};
    int m_sample_count{};
    int m_channel_count{};
};

struct PlsrBFSTM final : PlsrBase {
    Result LoadFile(fs::Fs* fs, const fs::FsPath& path, u32 flags) override {
        std::vector<u8> buf;
        R_TRY(fs->read_entire_file(path, buf));

        PLSR_BFSTM bfstm;
        R_TRY(plsrBFSTMOpenMem(buf.data(), buf.size(), &bfstm));
        ON_SCOPE_EXIT(plsrBFSTMClose(&bfstm));

        PLSR_BFSTMInfo info;
        R_TRY(plsrBFSTMReadInfo(&bfstm, &info));

        m_info.sample_count = info.sampleCount;
        m_info.sample_rate = info.sampleRate;
        m_info.loop_start = info.loopStartSample;
        m_info.looping = info.looping;

        R_TRY(plsrPlayerLoadStream(&bfstm, &m_id));
        return plsrPlayerPlay(m_id);
    }
};

struct PlsrBFWAV final : PlsrBase {
    Result LoadFile(fs::Fs* fs, const fs::FsPath& path, u32 flags) override {
        std::vector<u8> buf;
        R_TRY(fs->read_entire_file(path, buf));
        // todo: fix this
        // see devoptab_bfsar.cpp read()
        buf.resize(buf.size() + 64);

        PLSR_BFWAV bfwav;
        R_TRY(plsrBFWAVOpenMem(buf.data(), buf.size(), &bfwav));
        // R_TRY(plsrBFWAVOpen(path, &bfwav));
        ON_SCOPE_EXIT(plsrBFWAVClose(&bfwav));

        PLSR_BFWAVInfo info;
        R_TRY(plsrBFWAVReadInfo(&bfwav, &info));

        log_write("read file\n");

        m_info.sample_count = info.sampleCount;
        m_info.sample_rate = info.sampleRate;
        m_info.loop_start = info.loopStartSample;
        m_info.looping = info.looping;

        R_TRY(plsrPlayerLoadWave(&bfwav, &m_id));
        return plsrPlayerPlay(m_id);
    }
};

#ifdef ENABLE_AUDIO_WAV
struct DrWAV final : CustomBase {
    ~DrWAV() {
        drwav_uninit(&m_wav);
    }

    Result LoadFile(fs::Fs* fs, const fs::FsPath& path, u32 flags) override {
        R_TRY(m_file.Open(fs, path));
        const auto rc = drwav_init_with_metadata(&m_wav, OnRead, OnSeek, OnTell, &m_file, 0, nullptr);
        R_UNLESS(rc, 0x5);

        drwav_uint64 length_pcm;
        if (DRWAV_SUCCESS != drwav_get_length_in_pcm_frames(&m_wav, &length_pcm)) {
            R_THROW(0x6);
        }

        m_info.sample_count = length_pcm;
        m_info.sample_rate = m_wav.sampleRate;
        m_info.channels = m_wav.channels;

        return Create(m_wav.channels, m_wav.sampleRate, [this](int sample_count, s16 *data) -> int {
            return drwav_read_pcm_frames_s16(&m_wav, sample_count, data);
        });
    }

    Result Seek(u64 target) override {
        return drwav_seek_to_pcm_frame(&m_wav, target);
    }

    u64 Tell() override {
        drwav_uint64 pos = 0;
        drwav_get_cursor_in_pcm_frames(&m_wav, &pos);
        return pos;
    }

    void GetMeta(Meta* out) override {
        *out = {};

        for (u32 i = 0; i < m_wav.metadataCount; i++) {
            const auto& meta_data = m_wav.pMetadata[i];

            if (meta_data.type & drwav_metadata_type_list_info_title) {
                log_write("title: %.*s\n", meta_data.data.infoText.stringLength, meta_data.data.infoText.pString);
                out->title = std::string(meta_data.data.infoText.pString, meta_data.data.infoText.stringLength);
            }
            if (meta_data.type & drwav_metadata_type_list_info_artist) {
                log_write("artist: %.*s\n", meta_data.data.infoText.stringLength, meta_data.data.infoText.pString);
                out->artist = std::string(meta_data.data.infoText.pString, meta_data.data.infoText.stringLength);
            }

            log_write("[DRWAV] meta_data[%u] type: %d\n", i, meta_data.type);
        }
    }

private:
    static size_t OnRead(void *pUserData, void *pBufferOut, size_t bytesToRead) {
        auto file = static_cast<File*>(pUserData);
        return file->ReadFile(pBufferOut, bytesToRead);
    }

    static drwav_bool32 OnSeek(void *pUserData, int offset, drwav_seek_origin origin) {
        auto file = static_cast<File*>(pUserData);
        return file->SeekFile(offset, origin);
    }

    static drwav_bool32 OnTell(void *pUserData, drwav_int64* pCursor) {
        auto file = static_cast<File*>(pUserData);
        *pCursor = file->TellFile();
        return true;
    }

private:
    drwav m_wav{};
    File m_file{};
};
#endif // ENABLE_AUDIO_WAV

#ifdef ENABLE_AUDIO_MP3
struct DrMP3 final : CustomBase {
    DrMP3(std::unique_ptr<File>&& file = std::make_unique<File>()) : m_file{std::forward<decltype(file)>(file)} {

    }

    ~DrMP3() {
        drmp3_uninit(&m_mp3);
    }

    Result LoadFile(fs::Fs* fs, const fs::FsPath& path, u32 flags) override {
        R_TRY(m_file->Open(fs, path));
        const auto rc = drmp3_init(&m_mp3, OnRead, OnSeek, OnTell, OnMeta, this, nullptr);
        R_UNLESS(rc, 0x1);

        // todo: this is kinda slow.
        // todo: check id3 tags for song length and use that if found.
        const auto pcm_frame_count = drmp3_get_pcm_frame_count(&m_mp3);
        R_UNLESS(pcm_frame_count, 0x1);

        // wayyyy too slow to be useable for anything >= 3min.
        // todo: ask if dr_mp3 could generate the seek table at runtime instead
        // ie, pass in an empty table and have it fill it out as the song plays
        // drmp3_uint32 num_seek_points = pcm_frame_count;
        // m_seek_points.resize(num_seek_points);

        // if (!drmp3_calculate_seek_points(&m_mp3, &num_seek_points, m_seek_points.data())) {
        //     R_THROW(0x1);
        // }

        // m_seek_points.resize(num_seek_points);
        // if (!drmp3_bind_seek_table(&m_mp3, num_seek_points, m_seek_points.data())) {
        //     R_THROW(0x1);
        // }

        m_info.sample_count = pcm_frame_count;
        m_info.sample_rate = m_mp3.sampleRate;
        m_info.channels = m_mp3.channels;

        return Create(m_mp3.channels, m_mp3.sampleRate, [this](int sample_count, s16 *data) -> int {
            return drmp3_read_pcm_frames_s16(&m_mp3, sample_count, data);
        });
    }

    Result Seek(u64 target) override {
        return drmp3_seek_to_pcm_frame(&m_mp3, target);
    }

    u64 Tell() override {
        return m_mp3.currentPCMFrame;
    }

    void GetMeta(Meta* out) override {
        *out = m_meta;
    }

private:
    static size_t OnRead(void *pUserData, void *pBufferOut, size_t bytesToRead) {
        auto data = static_cast<DrMP3*>(pUserData);
        return data->m_file->ReadFile(pBufferOut, bytesToRead);
    }

    static drmp3_bool32 OnSeek(void *pUserData, int offset, drmp3_seek_origin origin) {
        auto data = static_cast<DrMP3*>(pUserData);
        return data->m_file->SeekFile(offset, origin);
    }

    static drmp3_bool32 OnTell(void *pUserData, drmp3_int64* pCursor) {
        auto data = static_cast<DrMP3*>(pUserData);
        *pCursor = data->m_file->TellFile();
        return true;
    }

    static void OnMeta(void* pUserData, const drmp3_metadata* pMetadata) {
        auto data = static_cast<DrMP3*>(pUserData);

        switch (pMetadata->type) {
            case DRMP3_METADATA_TYPE_ID3V1: log_write("[DRMP3] meta: ID3V1 size: %zu\n", pMetadata->rawDataSize); break;
            case DRMP3_METADATA_TYPE_ID3V2: log_write("[DRMP3] meta: ID3V2 size: %zu\n", pMetadata->rawDataSize); break;
            case DRMP3_METADATA_TYPE_APE:   log_write("[DRMP3] meta: APE size: %zu\n",   pMetadata->rawDataSize); break;
            case DRMP3_METADATA_TYPE_XING:  log_write("[DRMP3] meta: XING size: %zu\n",  pMetadata->rawDataSize); break;
            case DRMP3_METADATA_TYPE_VBRI:  log_write("[DRMP3] meta: VBRI size: %zu\n",  pMetadata->rawDataSize); break;
        }

        // todo: this lib fetches utf16 text incorrectly.
        if (pMetadata->type == DRMP3_METADATA_TYPE_ID3V2) {
            auto tag = ID3v2_read_tag_from_buffer((const char*)pMetadata->pRawData, pMetadata->rawDataSize);
            if (!tag) {
                return;
            }
            ON_SCOPE_EXIT(ID3v2_Tag_free(tag));

            log_write("[ID3v2] tag loaded\n");
            if (auto itag = ID3v2_Tag_get_artist_frame(tag)) {
                log_write("artist_frame: %s\n", convert_utf16(itag->data).c_str());
                data->m_meta.artist = convert_utf16(itag->data);
            }
            if (auto itag = ID3v2_Tag_get_album_frame(tag)) {
                log_write("album_frame: %s\n", convert_utf16(itag->data).c_str());
                data->m_meta.album = convert_utf16(itag->data);
            }
            if (auto itag = ID3v2_Tag_get_title_frame(tag)) {
                log_write("title_frame: %s\n", convert_utf16(itag->data).c_str());
                data->m_meta.title = convert_utf16(itag->data);
            }
            if (auto itag = ID3v2_Tag_get_track_frame(tag)) {
                log_write("track_frame: %s\n", convert_utf16(itag->data).c_str());
            }
            if (auto itag = ID3v2_Tag_get_album_artist_frame(tag)) {
                log_write("album_artist_frame: %s\n", convert_utf16(itag->data).c_str());
                if (data->m_meta.artist.empty()) {
                    data->m_meta.artist = convert_utf16(itag->data);
                }
            }
            if (auto itag = ID3v2_Tag_get_album_cover_frame(tag)) {
                log_write("album_cover_frame: mine: %s type: %d size: %d\n", itag->data->mime_type, itag->data->picture_type, itag->data->picture_size);

                data->m_meta.image.resize(itag->data->picture_size);
                std::memcpy(data->m_meta.image.data(), itag->data->data, itag->data->picture_size);
            }
        }
    }

public:
    // has to be public because dr_mp3 uses the same userdata for all callbacks.
    // this means that the OnMeta and Io calls point to the same thing, so we have to
    // pass the this pointer to handle both.
    std::unique_ptr<File> m_file;
    Meta m_meta{};

private:
    drmp3 m_mp3{};
    #if 0
    std::vector<drmp3_seek_point> m_seek_points{};
    #endif
};
#endif // ENABLE_AUDIO_MP3

#ifdef ENABLE_AUDIO_FLAC
struct DrFLAC final : CustomBase {
    ~DrFLAC() {
        drflac_close(m_flac);
    }

    Result LoadFile(fs::Fs* fs, const fs::FsPath& path, u32 flags) override {
        R_TRY(m_file.Open(fs, path));
        m_flac = drflac_open(OnRead, OnSeek, OnTell, &m_file, nullptr);
        R_UNLESS(m_flac, 0x1);

        m_info.sample_count = m_flac->totalPCMFrameCount;
        m_info.sample_rate = m_flac->sampleRate;
        m_info.channels = m_flac->channels;

        return Create(m_flac->channels, m_flac->sampleRate, [this](int sample_count, s16 *data) -> int {
            return drflac_read_pcm_frames_s16(m_flac, sample_count, data);
        });
    }

    Result Seek(u64 target) override {
        return drflac_seek_to_pcm_frame(m_flac, target);
    }

    u64 Tell() override {
        return m_flac->currentPCMFrame;
    }

private:
    static size_t OnRead(void *pUserData, void *pBufferOut, size_t bytesToRead) {
        auto file = static_cast<File*>(pUserData);
        return file->ReadFile(pBufferOut, bytesToRead);
    }

    static drflac_bool32 OnSeek(void *pUserData, int offset, drflac_seek_origin origin) {
        auto file = static_cast<File*>(pUserData);
        return file->SeekFile(offset, origin);
    }

    static drflac_bool32 OnTell(void *pUserData, drflac_int64* pCursor) {
        auto file = static_cast<File*>(pUserData);
        *pCursor = file->TellFile();
        return true;
    }

private:
    drflac* m_flac{};
    File m_file{};
};
#endif // ENABLE_AUDIO_FLAC

#ifdef ENABLE_AUDIO_OGG
// api is not ready, leaving this here for when it is.
#if 0
struct DrOGG final : CustomBase {
    ~DrOGG() {
        dr_vorbis_uninit(&m_vorbis);
    }

    Result LoadFile(fs::Fs* fs, const fs::FsPath& path, u32 flags) override {
        R_TRY(m_file.Open(fs, path));
        const auto result = dr_vorbis_init_ex(&m_file, OnRead, OnSeek, &m_meta, OnMeta, nullptr, &m_vorbis);

        // dr_vorbis_ogg_init(&m_file, OnRead, OnSeek, )
        R_UNLESS(result == DR_VORBIS_SUCCESS, 0x5);

        log_write("done init\n");

        // todo:
        m_info.sample_count = m_vorbis.stream.sampleRate * 100;
        m_info.sample_rate = m_vorbis.stream.sampleRate;

        log_write("[DRVORBIS] count: %u rate: %u\n", m_info.sample_count, m_info.sample_rate);

        return Create(m_vorbis.stream.channels, m_vorbis.stream.sampleRate, [this](int sample_count, s16 *data) -> int {
            log_write("[DRVORBIS] doing decode: %d\n", sample_count);

            std::vector<float> temp(sample_count * 2);
            dr_vorbis_uint64 frames_read = 0;
            if (DR_VORBIS_SUCCESS != dr_vorbis_read_pcm_frames_f32(&m_vorbis, temp.data(), sample_count, &frames_read)) {
                log_write("[DRVORBIS] failed to decode\n");
                return -1;
            }

            // convert f32 to pcm32
            for (u64 i = 0; i < frames_read; i++) {
                data[i] = temp[i] * (32767 / 2);
            }

            log_write("[DRVORBIS] got frames: %zu\n", frames_read);
            return frames_read;
        });
    }

    Result Seek(u64 target) override {
        if (DR_VORBIS_SUCCESS != dr_vorbis_seek_to_pcm_frame(&m_vorbis, target)) {
            R_THROW(0x1);
        }

        R_SUCCEED();
    }

    u64 Tell() override {
        // todo:
        return 0;
    }

    void GetMeta(Meta* out) override {
        *out = m_meta;
    }

private:
    static void OnMeta(void* pUserData, const dr_vorbis_metadata* pMetadata) {
    }

    static dr_vorbis_result OnRead(void* pUserData, void* pOutput, size_t bytesToRead, size_t* pBytesRead) {
        auto file = static_cast<File*>(pUserData);
        *pBytesRead = file->ReadFile(pOutput, bytesToRead);
        // todo: handle errors.
        return DR_VORBIS_SUCCESS;
    }

    static dr_vorbis_result OnSeek(void* pUserData, dr_vorbis_int64 offset, dr_vorbis_seek_origin origin) {
        auto file = static_cast<File*>(pUserData);
        file->SeekFile(offset, origin);
        // todo: handle errors.
        return DR_VORBIS_SUCCESS;
    }

private:
    dr_vorbis m_vorbis{};
    File m_file{};
    Meta m_meta{};
};
#endif

struct stbOGG final : CustomBase {
    ~stbOGG() {
        stb_vorbis_close(m_ogg);
    }

    Result LoadFile(fs::Fs* fs, const fs::FsPath& path, u32 flags) override {
        R_TRY(m_file.Open(fs, path));

        const stb_vorbis_io io = {
            .user = &m_file,
            .ftell = OnTell,
            .fseek = OnSeek,
            .fread = OnRead,
            .fclose = NULL,
        };

        int error = 0;
        m_ogg = stb_vorbis_open_io(&io, &error, nullptr, m_file.GetSize());
        // m_ogg = stb_vorbis_open_filename(path, &error, nullptr);
        log_write("[STB] error: %d\n", error);
        R_UNLESS(m_ogg, 0x5);

        const auto info = stb_vorbis_get_info(m_ogg);
        m_info.sample_count = stb_vorbis_stream_length_in_samples(m_ogg);
        m_info.sample_rate = info.sample_rate;
        m_info.channels = info.channels;

        return Create(m_info.channels, m_info.sample_rate, [this](int sample_count, s16 *data) -> int {
            return stb_vorbis_get_samples_short_interleaved(m_ogg, m_info.channels, data, sample_count);
        });
    }

    Result Seek(u64 target) override {
        return stb_vorbis_seek(m_ogg, target);
    }

    u64 Tell() override {
        return m_ogg->current_loc;
    }

private:
    static size_t OnRead(void *pUserData, void *pBufferOut, size_t bytesToRead) {
        auto file = static_cast<File*>(pUserData);
        return file->ReadFile(pBufferOut, bytesToRead);
    }

    static int OnSeek(void *pUserData, long offset, int origin) {
        auto file = static_cast<File*>(pUserData);
        return !file->SeekFile(offset, origin);
    }

    static long OnTell(void *pUserData) {
        auto file = static_cast<File*>(pUserData);
        return file->TellFile();
    }

private:
    stb_vorbis* m_ogg{};
    File m_file{};
};
#endif // ENABLE_AUDIO_OGG

constexpr u32 MAX_SONGS = 4;

struct SongEntry {
    Base* source;
    Progress progress;
    State state;
    Mutex mutex;

    bool IsGood() const {
        return state != State::Free && state != State::Error;
    }

    bool IsPaused() const {
        return state == State::Paused;
    }

    bool IsPlaying() const {
        return state == State::Playing;
    }
};

Mutex g_mutex{};
SongEntry g_songs[MAX_SONGS]{};
PLSR_PlayerSoundId g_sound_ids[std::to_underlying(SoundEffect::MAX)]{};
Thread g_thread{};
UEvent g_cancel_uevent{};
std::atomic_bool g_is_init{};

void thread_func(void* arg) {
    auto player = plsrPlayerGetInstance();
    if (!player) {
        return;
    }

    const std::array waiters{
        waiterForUEvent(&g_cancel_uevent),
        // i thought this would wait until the submitted buffers were empty, like audout
        // however that is not the case, and it blocks for too long.
        // because of this, i wait on the even with a timeout.
        // waiterForEvent(audrenGetFrameEvent()),
    };

    const u64 timeout = 1e+6;
    auto next_timeout = timeout;

    for (;;) {
        const auto deadline = armGetSystemTick() + armNsToTicks(timeout);

        s32 idx;
        const auto rc = waitObjects(&idx, waiters.data(), waiters.size(), next_timeout);

        if ((R_SUCCEEDED(rc) && idx == 0) || (R_FAILED(rc) && rc != SvcError_TimedOut)) {
            log_write("[AUDIO] failed to wait for event\n");
            break;
        }

        SCOPED_MUTEX(&g_mutex);

        for (auto& song : g_songs) {
            // do not block the audio thread for too long as it may be
            // blocked when loading a new song.
            // todo: find alternative to mutex that allows for a timeout.
            // max waiting for 1ms maybe?
            if (!mutexTryLock(&song.mutex)) {
                continue;
            }
            ON_SCOPE_EXIT(mutexUnlock(&song.mutex));

            // always call update on pause/play as the voice may need to be paused
            // which is handled within update.
            if (song.state == State::Playing || song.state == State::Paused) {
                if (R_FAILED(song.source->Update(song.progress, song.state))) {
                    song.state = State::Error;
                    log_write("[AUDIO] failed to update\n");
                }
            }
        }

        const s64 remaining = deadline - armGetSystemTick();
        next_timeout = remaining > 0 ? armTicksToNs(remaining) : 1e+4;
    }
}

} // namespace

Result Init() {
    if (g_is_init) {
        R_SUCCEED();
    }

    SCOPED_MUTEX(&g_mutex);
    R_TRY(plsrPlayerInit());

    #if 1
    if (R_SUCCEEDED(romfsMountDataStorageFromProgram(0x0100000000001000, "qlaunch"))) {
        ON_SCOPE_EXIT(romfsUnmount("qlaunch"));

        PLSR_BFSAR qlaunch_bfsar;
        if (R_SUCCEEDED(plsrBFSAROpen("qlaunch:/sound/qlaunch.bfsar", &qlaunch_bfsar))) {
            ON_SCOPE_EXIT(plsrBFSARClose(&qlaunch_bfsar));

            const auto load_sound = [&qlaunch_bfsar](const char* name, SoundEffect id) {
                if (R_FAILED(plsrPlayerLoadSoundByName(&qlaunch_bfsar, name, &g_sound_ids[std::to_underlying(id)]))) {
                    log_write("[PLSR] failed to load sound effect: %s\n", name);
                }
            };

            // 23ms
            {
                SCOPED_TIMESTAMP("audio sound effect load");
                load_sound("SeGameIconFocus", SoundEffect::Focus);
                load_sound("SeGameIconScroll", SoundEffect::Scroll);
                load_sound("SeGameIconLimit", SoundEffect::Limit);
                load_sound("StartupMenu_Game", SoundEffect::Startup);
                load_sound("SeGameIconAdd", SoundEffect::Install);
                load_sound("SeInsertError", SoundEffect::Error);
            }

            plsrPlayerSetVolume(g_sound_ids[std::to_underlying(SoundEffect::Limit)], 2.0f);
            plsrPlayerSetVolume(g_sound_ids[std::to_underlying(SoundEffect::Focus)], 0.5f);
        }
    } else {
        log_write("failed to mount romfs 0x0100000000001000\n");
    }
    #endif

    for (auto& e : g_songs) {
        mutexInit(&e.mutex);
        e.state = State::Free;
        e.source = nullptr;
    }

    ueventCreate(&g_cancel_uevent, false);
    R_TRY(utils::CreateThread(&g_thread, thread_func, nullptr, 1024*128, 0x20));
    R_TRY(threadStart(&g_thread));

    g_is_init = true;
    R_SUCCEED();
}

void ExitSignal() {
    if (g_is_init) {
        ueventSignal(&g_cancel_uevent);
    }
}

void Exit() {
    if (!g_is_init) {
        return;
    }

    ExitSignal();
    threadWaitForExit(&g_thread);
    threadClose(&g_thread);

    SCOPED_MUTEX(&g_mutex);

    for (auto& id : g_sound_ids) {
        plsrPlayerFree(id);
    }

    for (auto& e : g_songs) {
        SCOPED_MUTEX(&e.mutex);
        if (e.state != State::Free) {
            delete e.source;
        }
    }

    plsrPlayerExit();

    std::memset(g_songs, 0, sizeof(g_songs));
    std::memset(g_sound_ids, 0, sizeof(g_sound_ids));
    g_is_init = false;
}

Result PlaySoundEffect(SoundEffect effect) {
    R_UNLESS(g_is_init, 0x1);

    SCOPED_MUTEX(&g_mutex);
    const auto id = g_sound_ids[std::to_underlying(effect)];

    if (plsrPlayerIsPlaying(id)) {
        R_TRY(plsrPlayerStop(id));
        plsrPlayerWaitNextFrame(); // is this needed?
    }

    return plsrPlayerPlay(id);
}

Result OpenSong(fs::Fs* fs, const fs::FsPath& path, u32 flags, SongID* id) {
    R_UNLESS(g_is_init, 0x1);

    SCOPED_MUTEX(&g_mutex);
    R_UNLESS(fs && id && !path.empty(), 0x1);

    for (auto& e : g_songs) {
        SCOPED_MUTEX(&e.mutex);

        if (e.state == State::Free) {
            std::unique_ptr<Base> source{};

            if (path.ends_with(".bfstm")) {
                source = std::make_unique<PlsrBFSTM>();
            }
            else if (path.ends_with(".bfwav")) {
                source = std::make_unique<PlsrBFWAV>();
            }
#ifdef ENABLE_AUDIO_WAV
            else if (path.ends_with(".wav")) {
                source = std::make_unique<DrWAV>();
            }
#endif // ENABLE_AUDIO_WAV
#ifdef ENABLE_AUDIO_MP3
            else if (path.ends_with(".mp3") || path.ends_with(".mp2") || path.ends_with(".mp1")) {
                source = std::make_unique<DrMP3>();
            }
            else if (path.ends_with(".adf")) {
                source = std::make_unique<DrMP3>(std::make_unique<GTAViceCityFile>());
            }
#endif // ENABLE_AUDIO_MP3
#ifdef ENABLE_AUDIO_FLAC
            else if (path.ends_with(".flac")) {
                source = std::make_unique<DrFLAC>();
            }
#endif // ENABLE_AUDIO_FLAC
#ifdef ENABLE_AUDIO_OGG
            // else if (path.ends_with(".ogg")) {
            //     source = std::make_unique<DrOGG>();
            // }
            else if (path.ends_with(".ogg")) {
                source = std::make_unique<stbOGG>();
            }
#endif // ENABLE_AUDIO_OGG

            R_UNLESS(source, 0x1);
            R_TRY(source->LoadFile(fs, path, flags));

            e.state = State::Paused;
            e.source = source.release();
            e.progress = {};
            *id = &e;

            R_SUCCEED();
        }
    }

    R_THROW(0x1);
}

Result CloseSong(SongID* id) {
    R_UNLESS(g_is_init, 0x1);

    R_UNLESS(id && *id, 0x1);
    auto e = static_cast<SongEntry*>(*id);

    SCOPED_MUTEX(&e->mutex);
    R_UNLESS(e->state != State::Free, 0x1);

    delete e->source;
    e->state = State::Free;
    *id = nullptr;

    R_SUCCEED();
}

#define LockSongAndDo(cond_func, ...) do { \
    R_UNLESS(g_is_init, 0x1); \
    \
    R_UNLESS(id, 0x1); \
    auto e = static_cast<SongEntry*>(id); \
    \
    SCOPED_MUTEX(&e->mutex); \
    R_UNLESS(e->cond_func(), 0x1); \
    \
    __VA_ARGS__ \
    R_SUCCEED(); \
} while (0)

Result PlaySong(SongID id) {
    LockSongAndDo(IsPaused,
        e->state = State::Playing;
    );
}

Result PauseSong(SongID id) {
    LockSongAndDo(IsPlaying,
        e->state = State::Paused;
    );
}

Result SeekSong(SongID id, u64 target) {
    LockSongAndDo(IsGood,
        e->source->Seek(target);
        if (e->state == State::Finished) {
            e->state = State::Playing;
            log_write("set playing now\n");
        }
    );
}

Result GetVolumeSong(SongID id, float* out) {
    LockSongAndDo(IsGood,
        e->source->GetVolume(out);
    );
}

Result SetVolumeSong(SongID id, float in) {
    LockSongAndDo(IsGood,
        e->source->SetVolume(in);
    );
}

Result GetPitchSong(SongID id, float* out) {
    LockSongAndDo(IsGood,
        e->source->GetPitch(out);
    );
}

Result SetPitchSong(SongID id, float in) {
    LockSongAndDo(IsGood,
        e->source->SetPitch(in);
    );
}

Result GetInfo(SongID id, Info* out) {
    LockSongAndDo(IsGood,
        e->source->GetInfo(out);
    );
}

Result GetMeta(SongID id, Meta* out) {
    LockSongAndDo(IsGood,
        e->source->GetMeta(out);
    );
}

Result GetProgress(SongID id, Progress* out_progress, State* out_state) {
    LockSongAndDo(IsGood,
        if (out_progress) *out_progress = e->progress;
        if (out_state) *out_state = e->state;
    );
}

} // namespace sphaira::audio
