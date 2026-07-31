use clap::{Parser, ValueEnum};

/// Stored-recording stream selected from the camera.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum CmdStream {
    /// Main/high-quality recording stream.
    Main,
    /// Sub/fluent recording stream.
    #[default]
    Sub,
}

/// Audio policy used while finalizing the MP4 track topology.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum AudioMode {
    /// Intentionally omit camera audio from the export.
    #[default]
    None,
    /// Require valid AAC during bounded preflight and include it in the export.
    Required,
}

/// Export one RecordingEntry from stdin as fragmented MP4 on stdout.
#[derive(Parser, Debug)]
pub struct Opt {
    /// Camera name from the Neolink configuration.
    pub camera: String,

    /// Override the camera config's logical channel.
    #[arg(long)]
    pub channel: Option<u8>,

    /// Main/high-quality or sub/fluent stored-recording stream.
    #[arg(long, value_enum, default_value_t)]
    pub stream: CmdStream,

    /// Whether AAC should be omitted or required during bounded preflight.
    #[arg(long, value_enum, default_value_t)]
    pub audio: AudioMode,

    /// Maximum replay wall time in seconds.
    #[arg(long, default_value_t = 900, value_parser = clap::value_parser!(u64).range(1..=7200))]
    pub max_duration_seconds: u64,

    /// Maximum compressed media bytes accepted from the camera.
    #[arg(long, default_value_t = 2_147_483_648, value_parser = clap::value_parser!(u64).range(1..=8_589_934_592))]
    pub max_media_bytes: u64,
}
