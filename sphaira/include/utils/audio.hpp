#pragma once

#include "fs.hpp"
#include "image.hpp"
#include <string>
#include <memory>

namespace sphaira::audio {

enum class State {
    Free,     // private use.
    Playing,  // song is playing.
    Paused,   // song has paused.
    Finished, // song has finished.
    Error,    // error in playback.
};

struct Progress {
    u64 played;
};

struct Info {
    u64 sample_count;
    u32 sample_rate;
    u32 channels;
    u32 loop_start;
    bool looping;
};

struct Meta {
    std::string title{};
    std::string album{};
    std::string artist{};
    std::vector<u8> image{};
};

enum class SoundEffect {
    Focus,
    Scroll,
    Limit,
    Startup,
    Install,
    Error,
    MAX,
};

enum Flag {
    Flag_None = 0,
    // plays the song for ever.
    Flag_Loop = 1 << 0,
};

using SongID = void*;

Result Init();
void ExitSignal();
void Exit();

Result PlaySoundEffect(SoundEffect effect);

Result OpenSong(fs::Fs* fs, const fs::FsPath& path, u32 flags, SongID* id);
Result CloseSong(SongID* id);

Result PlaySong(SongID id);
Result PauseSong(SongID id);
Result SeekSong(SongID id, u64 target);

// todo:
// 0.0 -> 2.0.
Result GetVolumeSong(SongID id, float* out);
Result SetVolumeSong(SongID id, float in);

// todo:
Result GetPitchSong(SongID id, float* out);
Result SetPitchSong(SongID id, float in);

Result GetInfo(SongID id, Info* out);
Result GetMeta(SongID id, Meta* out);
Result GetProgress(SongID id, Progress* out_progress, State* out_state);

} // namespace sphaira::audio
