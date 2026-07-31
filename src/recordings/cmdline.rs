use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum CmdStream {
    Main,
    #[default]
    Sub,
}

/// List stored recording metadata without downloading footage.
#[derive(Parser, Debug)]
pub struct Opt {
    /// Camera name from the Neolink configuration.
    pub camera: String,

    /// Camera-local calendar date in YYYY-MM-DD format.
    #[arg(long)]
    pub date: String,

    /// Camera-local inclusive lower time bound in exact HH:MM:SS format.
    #[arg(long, default_value = "00:00:00")]
    pub from: String,

    /// Camera-local inclusive upper time bound in exact HH:MM:SS format.
    #[arg(long, default_value = "23:59:59")]
    pub until: String,

    /// Override the camera config's logical channel.
    #[arg(long)]
    pub channel: Option<u8>,

    /// Recording stream to search.
    #[arg(long, value_enum, default_value_t)]
    pub stream: CmdStream,

    /// Maximum FileInfoList pages requested.
    #[arg(long, default_value_t = neolink_core::bc_protocol::DEFAULT_RECORDING_MAX_PAGES)]
    pub max_pages: usize,

    /// Maximum unique entries retained.
    #[arg(long, default_value_t = neolink_core::bc_protocol::DEFAULT_RECORDING_MAX_ENTRIES)]
    pub max_entries: usize,

    /// Emit JSON including recording identifiers. Credentials and raw XML are never included.
    #[arg(long)]
    pub json: bool,
}
