//! One-shot, bounded stored-recording export to fragmented MP4 on stdout.

use std::{
    future::Future,
    io::Write,
    net::{IpAddr, ToSocketAddrs},
    path::PathBuf,
    pin::Pin,
    process::ExitCode,
    str::FromStr,
    time::Duration,
};

use neolink_core::{
    bc_protocol::{
        BcCamera, BcCameraOpt, ConnectionProtocol, Credentials, MaxEncryption, RecordingEntry,
        RecordingReplay, RecordingReplayEnd, RecordingReplayOptions, RecordingStreamKind,
        HARD_RECORDING_REPLAY_BUFFER_SIZE, HARD_RECORDING_REPLAY_MAX_BUFFERED_MEDIA_BYTES,
    },
    bcmedia::model::{BcMedia, BcMediaAac, BcMediaIframe, VideoType},
    Error as NeolinkError,
};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{config::CameraConfig, config::Config, utils::timeout};

mod cmdline;
mod gst;

pub(crate) use cmdline::Opt;
use cmdline::{AudioMode, CmdStream};
use gst::{Fmp4Muxer, OutputFailure, OutputTarget, PipelineFailure};

const MAX_ENTRY_JSON_BYTES: usize = 64 * 1024;
const PREFLIGHT_MAX_PACKETS: usize = 256;
const PREFLIGHT_MAX_BYTES: usize = 16 * 1024 * 1024;
const H264_PARAMETER_SET_MAX_BYTES: usize = 64 * 1024;
const PREFLIGHT_MAX_DURATION: Duration = Duration::from_secs(15);
const CAMERA_CLEANUP_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_VIDEO_FPS: u32 = 25;
const AUDIO_UNAVAILABLE_MISSING: ExportFailure = ExportFailure(
    "RECORDING_EXPORT_AUDIO_UNAVAILABLE: AAC was required but was not present in the recording",
);
const AUDIO_UNAVAILABLE_ADPCM: ExportFailure =
    ExportFailure("RECORDING_EXPORT_AUDIO_UNAVAILABLE: recording audio is ADPCM rather than AAC");
const AUDIO_UNAVAILABLE_INVALID: ExportFailure =
    ExportFailure("RECORDING_EXPORT_AUDIO_UNAVAILABLE: recording AAC failed codec preflight");

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreflightTermination {
    StreamEnd,
    TimeBound,
    PacketBound,
    ByteBound,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreflightContent {
    NoH264,
    AnnexbAbsent,
    SpsMissing,
    PpsMissing,
    IdrMissingAfterConfig,
}

fn preflight_failure(
    termination: PreflightTermination,
    content: PreflightContent,
) -> ExportFailure {
    match (termination, content) {
        (PreflightTermination::StreamEnd, PreflightContent::NoH264) => {
            ExportFailure("recording codec preflight failed: stream_end:no_h264")
        }
        (PreflightTermination::StreamEnd, PreflightContent::AnnexbAbsent) => {
            ExportFailure("recording codec preflight failed: stream_end:annexb_absent")
        }
        (PreflightTermination::StreamEnd, PreflightContent::SpsMissing) => {
            ExportFailure("recording codec preflight failed: stream_end:sps_missing")
        }
        (PreflightTermination::StreamEnd, PreflightContent::PpsMissing) => {
            ExportFailure("recording codec preflight failed: stream_end:pps_missing")
        }
        (PreflightTermination::StreamEnd, PreflightContent::IdrMissingAfterConfig) => {
            ExportFailure("recording codec preflight failed: stream_end:idr_missing_after_config")
        }
        (PreflightTermination::TimeBound, PreflightContent::NoH264) => {
            ExportFailure("recording codec preflight failed: time_bound:no_h264")
        }
        (PreflightTermination::TimeBound, PreflightContent::AnnexbAbsent) => {
            ExportFailure("recording codec preflight failed: time_bound:annexb_absent")
        }
        (PreflightTermination::TimeBound, PreflightContent::SpsMissing) => {
            ExportFailure("recording codec preflight failed: time_bound:sps_missing")
        }
        (PreflightTermination::TimeBound, PreflightContent::PpsMissing) => {
            ExportFailure("recording codec preflight failed: time_bound:pps_missing")
        }
        (PreflightTermination::TimeBound, PreflightContent::IdrMissingAfterConfig) => {
            ExportFailure("recording codec preflight failed: time_bound:idr_missing_after_config")
        }
        (PreflightTermination::PacketBound, PreflightContent::NoH264) => {
            ExportFailure("recording codec preflight failed: packet_bound:no_h264")
        }
        (PreflightTermination::PacketBound, PreflightContent::AnnexbAbsent) => {
            ExportFailure("recording codec preflight failed: packet_bound:annexb_absent")
        }
        (PreflightTermination::PacketBound, PreflightContent::SpsMissing) => {
            ExportFailure("recording codec preflight failed: packet_bound:sps_missing")
        }
        (PreflightTermination::PacketBound, PreflightContent::PpsMissing) => {
            ExportFailure("recording codec preflight failed: packet_bound:pps_missing")
        }
        (PreflightTermination::PacketBound, PreflightContent::IdrMissingAfterConfig) => {
            ExportFailure("recording codec preflight failed: packet_bound:idr_missing_after_config")
        }
        (PreflightTermination::ByteBound, PreflightContent::NoH264) => {
            ExportFailure("recording codec preflight failed: byte_bound:no_h264")
        }
        (PreflightTermination::ByteBound, PreflightContent::AnnexbAbsent) => {
            ExportFailure("recording codec preflight failed: byte_bound:annexb_absent")
        }
        (PreflightTermination::ByteBound, PreflightContent::SpsMissing) => {
            ExportFailure("recording codec preflight failed: byte_bound:sps_missing")
        }
        (PreflightTermination::ByteBound, PreflightContent::PpsMissing) => {
            ExportFailure("recording codec preflight failed: byte_bound:pps_missing")
        }
        (PreflightTermination::ByteBound, PreflightContent::IdrMissingAfterConfig) => {
            ExportFailure("recording codec preflight failed: byte_bound:idr_missing_after_config")
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecordingExportFailureCategory {
    ReplayRequest,
    OutputInit,
    MuxInit,
    ReplayInvalidMedia,
    ReplayTimeout,
    ReplayRelayTerminated,
    ReplayCameraTerminated,
    ReplayConnectionDropped,
    ReplayIo,
    ReplaySend,
    ReplayStream,
    ReplayStop,
    ReplayDurationLimit,
    ReplayByteLimit,
    ReplayBufferLimit,
    ReplayConsumerStalled,
    ReplayClientDisconnected,
    ReplayCancelled,
    H265Unsupported,
    AacPacketInvalid,
    H264Parse,
    AacParse,
    Mp4Mux,
    OutputDisconnected,
    OutputStalled,
    OutputWrite,
    FinalizationTimeout,
    PipelineOther,
    CameraCleanupTimeout,
    CameraCleanupFailed,
}

impl RecordingExportFailureCategory {
    fn failure(self) -> ExportFailure {
        match self {
            Self::ReplayRequest => {
                ExportFailure("recording export failed: replay_request_rejected")
            }
            Self::OutputInit => ExportFailure("recording export failed: output_init"),
            Self::MuxInit => ExportFailure("recording export failed: mux_init"),
            Self::ReplayInvalidMedia => {
                ExportFailure("recording export failed: replay_invalid_media")
            }
            Self::ReplayTimeout => ExportFailure("recording export failed: replay_timeout"),
            Self::ReplayRelayTerminated => {
                ExportFailure("recording export failed: replay_relay_terminated")
            }
            Self::ReplayCameraTerminated => {
                ExportFailure("recording export failed: replay_camera_terminated")
            }
            Self::ReplayConnectionDropped => {
                ExportFailure("recording export failed: replay_connection_dropped")
            }
            Self::ReplayIo => ExportFailure("recording export failed: replay_io"),
            Self::ReplaySend => ExportFailure("recording export failed: replay_send"),
            Self::ReplayStream => ExportFailure("recording export failed: replay_stream"),
            Self::ReplayStop => ExportFailure("recording export failed: replay_stop"),
            Self::ReplayDurationLimit => {
                ExportFailure("recording export failed: replay_duration_limit")
            }
            Self::ReplayByteLimit => ExportFailure("recording export failed: replay_byte_limit"),
            Self::ReplayBufferLimit => {
                ExportFailure("recording export failed: replay_buffer_limit")
            }
            Self::ReplayConsumerStalled => {
                ExportFailure("recording export failed: replay_consumer_stalled")
            }
            Self::ReplayClientDisconnected => {
                ExportFailure("recording export failed: replay_client_disconnected")
            }
            Self::ReplayCancelled => ExportFailure("recording export failed: replay_cancelled"),
            Self::H265Unsupported => ExportFailure("recording export failed: h265_unsupported"),
            Self::AacPacketInvalid => ExportFailure("recording export failed: aac_packet_invalid"),
            Self::H264Parse => ExportFailure("recording export failed: h264_parse"),
            Self::AacParse => ExportFailure("recording export failed: aac_parse"),
            Self::Mp4Mux => ExportFailure("recording export failed: mp4_mux"),
            Self::OutputDisconnected => {
                ExportFailure("recording export failed: output_disconnected")
            }
            Self::OutputStalled => ExportFailure("recording export failed: output_stalled"),
            Self::OutputWrite => ExportFailure("recording export failed: output_write"),
            Self::FinalizationTimeout => {
                ExportFailure("recording export failed: finalization_timeout")
            }
            Self::PipelineOther => ExportFailure("recording export failed: pipeline_other"),
            Self::CameraCleanupTimeout => {
                ExportFailure("recording export failed: camera_cleanup_timeout")
            }
            Self::CameraCleanupFailed => {
                ExportFailure("recording export failed: camera_cleanup_failed")
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExportFailure(&'static str);

impl std::fmt::Display for ExportFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ExportFailure {}

type ExportResult<T> = std::result::Result<T, ExportFailure>;

/// Render the one-shot export result using its exact machine-readable stderr contract.
pub(crate) fn report_result<W>(result: ExportResult<()>, mut stderr: W) -> ExitCode
where
    W: Write,
{
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // A single controlled line is deliberate: eligible audio failures
            // must begin with the documented fallback marker, without Rust's
            // usual `Error: ` termination prefix or an anyhow cause chain.
            let _ = writeln!(stderr, "{error}");
            ExitCode::FAILURE
        }
    }
}

/// Run the redacted one-shot export path without installing a logger or reactor.
pub(crate) async fn run_from_config(opt: Opt, config_path: Option<PathBuf>) -> ExportResult<()> {
    run_from_config_inner(opt, config_path).await
}

async fn run_from_config_inner(opt: Opt, config_path: Option<PathBuf>) -> ExportResult<()> {
    let entry = read_recording_entry(tokio::io::stdin()).await?;
    let path = config_path.ok_or(ExportFailure("--config is required"))?;
    let config_text = std::fs::read_to_string(path)
        .map_err(|_| ExportFailure("unable to read Neolink configuration"))?;
    let mut config: Config = toml::from_str(&config_text)
        .map_err(|_| ExportFailure("unable to parse Neolink configuration"))?;
    validator::Validate::validate(&config)
        .map_err(|_| ExportFailure("Neolink configuration is invalid"))?;
    config.resolve_offline_timeouts();
    config.resolve_startup_keyframe_waits();

    let mut camera_config = config
        .cameras
        .into_iter()
        .find(|camera| camera.name == opt.camera)
        .ok_or(ExportFailure("camera is not present in the configuration"))?;
    let channel = opt.channel.unwrap_or(camera_config.channel_id);
    if channel > 31 {
        return Err(ExportFailure("recording channel must be between 0 and 31"));
    }
    camera_config.channel_id = channel;
    // Raw packet debugging can carry RecordingEntry payloads. It is never
    // permitted for the binary export process, regardless of configuration.
    camera_config.debug = false;

    let camera = connect_dedicated(&camera_config).await?;
    let result = export_camera(&camera, entry, &opt, channel).await;
    let cleanup = cleanup_camera(&camera).await;
    result?;
    cleanup
}

async fn read_recording_entry<R>(reader: R) -> ExportResult<RecordingEntry>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    reader
        .take((MAX_ENTRY_JSON_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| ExportFailure("unable to read RecordingEntry JSON from stdin"))?;
    if bytes.len() > MAX_ENTRY_JSON_BYTES {
        return Err(ExportFailure(
            "RecordingEntry JSON exceeds the 64 KiB limit",
        ));
    }
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Err(ExportFailure(
            "stdin must contain one RecordingEntry JSON object",
        ));
    }

    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let entry = RecordingEntry::deserialize(&mut deserializer)
        .map_err(|_| ExportFailure("stdin must contain one valid RecordingEntry JSON object"))?;
    deserializer
        .end()
        .map_err(|_| ExportFailure("stdin must contain exactly one RecordingEntry JSON object"))?;
    Ok(entry)
}

async fn connect_dedicated(camera_config: &CameraConfig) -> ExportResult<BcCamera> {
    let (port, addrs) = resolve_addresses(camera_config)?;
    let options = BcCameraOpt {
        name: camera_config.name.clone(),
        channel_id: camera_config.channel_id,
        addrs,
        port,
        uid: camera_config.camera_uid.clone(),
        protocol: ConnectionProtocol::TcpUdp,
        discovery: camera_config.discovery,
        relay_server_region: camera_config.relay_server_region.clone(),
        credentials: Credentials {
            username: camera_config.username.clone(),
            password: camera_config.password.clone(),
        },
        debug: false,
        max_discovery_retries: camera_config.max_discovery_retries,
        udp_gap_skip_ms: camera_config.udp_gap_skip_ms,
    };
    let camera = BcCamera::new(&options)
        .await
        .map_err(|_| ExportFailure("unable to establish dedicated camera connection"))?;
    let max_encryption = match camera_config.max_encryption.to_ascii_lowercase().as_str() {
        "none" => MaxEncryption::None,
        "bcencrypt" => MaxEncryption::BcEncrypt,
        _ => MaxEncryption::Aes,
    };
    timeout(camera.login_with_maxenc(max_encryption))
        .await
        .map_err(|_| ExportFailure("camera authentication timed out"))?
        .map_err(|_| ExportFailure("camera authentication failed"))?;
    Ok(camera)
}

fn resolve_addresses(camera_config: &CameraConfig) -> ExportResult<(Option<u16>, Vec<IpAddr>)> {
    let Some(value) = camera_config.camera_addr.as_deref() else {
        if camera_config.camera_uid.is_some() {
            return Ok((None, Vec::new()));
        }
        return Err(ExportFailure("camera has neither an address nor a UID"));
    };

    if let Ok(addresses) = value.to_socket_addrs() {
        let addresses = addresses.collect::<Vec<_>>();
        let port = addresses.first().map(std::net::SocketAddr::port);
        return Ok((
            port,
            addresses.into_iter().map(|address| address.ip()).collect(),
        ));
    }
    IpAddr::from_str(value)
        .map(|address| (None, vec![address]))
        .map_err(|_| ExportFailure("camera address is invalid"))
}

async fn export_camera(
    camera: &BcCamera,
    entry: RecordingEntry,
    opt: &Opt,
    channel: u8,
) -> ExportResult<()> {
    let replay_options = replay_options(opt, channel);
    let replay = camera
        .start_recording_replay(&entry, replay_options)
        .await
        .map_err(|_| RecordingExportFailureCategory::ReplayRequest.failure())?;
    let mut feed = CameraReplay { replay };
    let output =
        OutputTarget::stdout().map_err(|_| RecordingExportFailureCategory::OutputInit.failure())?;
    drive_and_shutdown(&mut feed, opt.audio, output).await
}

fn replay_options(opt: &Opt, channel: u8) -> RecordingReplayOptions {
    RecordingReplayOptions {
        channel,
        stream: match opt.stream {
            CmdStream::Main => RecordingStreamKind::Main,
            CmdStream::Sub => RecordingStreamKind::Sub,
        },
        max_duration: Duration::from_secs(opt.max_duration_seconds),
        max_media_bytes: opt.max_media_bytes,
        // Stored footage arrives in camera-sized bursts. This one-shot export
        // uses the already enforced core ceilings so a healthy fast consumer is
        // not mistaken for a stalled one merely because the default eight-slot
        // queue filled between scheduler turns.
        max_buffered_media_bytes: HARD_RECORDING_REPLAY_MAX_BUFFERED_MEDIA_BYTES,
        buffer_size: HARD_RECORDING_REPLAY_BUFFER_SIZE,
        ..Default::default()
    }
}

async fn cleanup_camera(camera: &BcCamera) -> ExportResult<()> {
    let _ = tokio::time::timeout(CAMERA_CLEANUP_TIMEOUT, camera.logout()).await;
    tokio::time::timeout(CAMERA_CLEANUP_TIMEOUT, camera.shutdown())
        .await
        .map_err(|_| RecordingExportFailureCategory::CameraCleanupTimeout.failure())?
        .map_err(|_| RecordingExportFailureCategory::CameraCleanupFailed.failure())
}

trait ReplayFeed {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = ExportResult<Option<BcMedia>>> + Send + '_>>;
    fn shutdown(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = ExportResult<RecordingReplayEnd>> + Send + '_>>;
}

struct CameraReplay {
    replay: RecordingReplay,
}

impl ReplayFeed for CameraReplay {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = ExportResult<Option<BcMedia>>> + Send + '_>> {
        Box::pin(async move {
            match self.replay.get_data().await {
                Ok(Ok(media)) => Ok(Some(media)),
                Ok(Err(error)) => Err(replay_item_error(&error)),
                Err(NeolinkError::StreamFinished) => Ok(None),
                Err(error) => Err(replay_item_error(&error)),
            }
        })
    }

    fn shutdown(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = ExportResult<RecordingReplayEnd>> + Send + '_>> {
        Box::pin(async move {
            self.replay
                .shutdown()
                .await
                .map_err(|error| replay_item_error(&error))
        })
    }
}

fn replay_item_error(error: &NeolinkError) -> ExportFailure {
    match error {
        NeolinkError::NomError(_) | NeolinkError::NomIncomplete(_) => {
            RecordingExportFailureCategory::ReplayInvalidMedia.failure()
        }
        NeolinkError::RecordingReplayAndStopFailed { replay, .. } => {
            replay_item_error(replay.as_ref())
        }
        NeolinkError::RecordingReplayStopFailed { .. } => {
            RecordingExportFailureCategory::ReplayStop.failure()
        }
        NeolinkError::Timeout(_)
        | NeolinkError::TimeoutError(_)
        | NeolinkError::TimeoutDisconnected
        | NeolinkError::BcUdpTimeout
        | NeolinkError::BcUdpReconnectTimeout
        | NeolinkError::DiscoveryTimeout => RecordingExportFailureCategory::ReplayTimeout.failure(),
        NeolinkError::RelayTerminate => {
            RecordingExportFailureCategory::ReplayRelayTerminated.failure()
        }
        NeolinkError::CameraTerminate => {
            RecordingExportFailureCategory::ReplayCameraTerminated.failure()
        }
        NeolinkError::DroppedConnection
        | NeolinkError::DroppedConnectionTry(_)
        | NeolinkError::BroadcastDroppedConnectionTry(_)
        | NeolinkError::ConnectionShutdown
        | NeolinkError::BcUdpDropReciver(_)
        | NeolinkError::BcUdpDropSender
        | NeolinkError::BcUdpPayloadDroppedInner
        | NeolinkError::DroppedSubscriber => {
            RecordingExportFailureCategory::ReplayConnectionDropped.failure()
        }
        NeolinkError::Io(_) => RecordingExportFailureCategory::ReplayIo.failure(),
        NeolinkError::TokioBcSendError => RecordingExportFailureCategory::ReplaySend.failure(),
        _ => RecordingExportFailureCategory::ReplayStream.failure(),
    }
}

async fn drive_export<F: ReplayFeed>(
    feed: &mut F,
    audio_mode: AudioMode,
    mut output: OutputTarget,
) -> ExportResult<()> {
    let (buffered, fps) = match collect_preflight(feed, audio_mode).await {
        Ok(preflight) => preflight,
        Err(error) => {
            // No media bytes exist yet, but honor the output flush contract on
            // every path. Production's raw nonblocking fd has no userspace
            // buffer; this seam also verifies deterministic fixture writers.
            output.flush_without_output();
            return Err(error);
        }
    };
    let mut muxer = Fmp4Muxer::new(audio_mode == AudioMode::Required, fps, output)
        .map_err(|_| RecordingExportFailureCategory::MuxInit.failure())?;
    let mut clocks = MediaClocks::new(fps);
    for media in buffered {
        if let Err(error) = push_media(&muxer, &mut clocks, audio_mode, media) {
            return Err(finish_after_input_error(&mut muxer, error));
        }
    }

    let output_cancelled = muxer.output_cancelled();
    loop {
        let media = tokio::select! {
            _ = output_cancelled.cancelled() => {
                return Err(output_error(&muxer));
            }
            media = feed.next() => match media {
                Ok(media) => media,
                Err(error) => return Err(finish_after_input_error(&mut muxer, error)),
            },
        };
        let Some(media) = media else {
            break;
        };
        if let Err(error) = push_media(&muxer, &mut clocks, audio_mode, media) {
            return Err(finish_after_input_error(&mut muxer, error));
        }
        if muxer.output_failure().is_some() {
            return Err(output_error(&muxer));
        }
    }

    muxer.finish().map_err(|_| output_error(&muxer))?;
    if muxer.output_failure().is_some() {
        return Err(output_error(&muxer));
    }
    Ok(())
}

fn finish_after_input_error(muxer: &mut Fmp4Muxer, original: ExportFailure) -> ExportFailure {
    let _ = muxer.finish();
    if muxer.output_failure().is_some() || muxer.pipeline_failure().is_some() {
        output_error(muxer)
    } else {
        original
    }
}

async fn drive_and_shutdown<F: ReplayFeed>(
    feed: &mut F,
    audio_mode: AudioMode,
    output: OutputTarget,
) -> ExportResult<()> {
    let export = drive_export(feed, audio_mode, output).await;
    let stopped = feed.shutdown().await;
    export?;
    replay_end_result(stopped?)
}

fn replay_end_result(end: RecordingReplayEnd) -> ExportResult<()> {
    match end {
        RecordingReplayEnd::CameraEnd => Ok(()),
        RecordingReplayEnd::DurationLimit => {
            Err(RecordingExportFailureCategory::ReplayDurationLimit.failure())
        }
        RecordingReplayEnd::ByteLimit => {
            Err(RecordingExportFailureCategory::ReplayByteLimit.failure())
        }
        RecordingReplayEnd::BufferLimit => {
            Err(RecordingExportFailureCategory::ReplayBufferLimit.failure())
        }
        RecordingReplayEnd::ConsumerStalled => {
            Err(RecordingExportFailureCategory::ReplayConsumerStalled.failure())
        }
        RecordingReplayEnd::ClientDisconnected => {
            Err(RecordingExportFailureCategory::ReplayClientDisconnected.failure())
        }
        RecordingReplayEnd::Cancelled => {
            Err(RecordingExportFailureCategory::ReplayCancelled.failure())
        }
    }
}

fn output_error(muxer: &Fmp4Muxer) -> ExportFailure {
    classify_output_error(muxer.output_failure(), muxer.pipeline_failure())
}

fn classify_output_error(
    output: Option<OutputFailure>,
    pipeline: Option<PipelineFailure>,
) -> ExportFailure {
    match output {
        Some(OutputFailure::BrokenPipe) => {
            RecordingExportFailureCategory::OutputDisconnected.failure()
        }
        Some(OutputFailure::ConsumerStalled) => {
            RecordingExportFailureCategory::OutputStalled.failure()
        }
        Some(OutputFailure::Write) => RecordingExportFailureCategory::OutputWrite.failure(),
        None => match pipeline {
            Some(PipelineFailure::H264Parse) => RecordingExportFailureCategory::H264Parse.failure(),
            Some(PipelineFailure::AacParse) => RecordingExportFailureCategory::AacParse.failure(),
            Some(PipelineFailure::Mp4Mux) => RecordingExportFailureCategory::Mp4Mux.failure(),
            Some(PipelineFailure::Output) => RecordingExportFailureCategory::OutputWrite.failure(),
            Some(PipelineFailure::Timeout) => {
                RecordingExportFailureCategory::FinalizationTimeout.failure()
            }
            Some(PipelineFailure::Other) | None => {
                RecordingExportFailureCategory::PipelineOther.failure()
            }
        },
    }
}

async fn collect_preflight<F: ReplayFeed>(
    feed: &mut F,
    audio_mode: AudioMode,
) -> ExportResult<(Vec<BcMedia>, u32)> {
    collect_preflight_with_duration(feed, audio_mode, PREFLIGHT_MAX_DURATION).await
}

async fn collect_preflight_with_duration<F: ReplayFeed>(
    feed: &mut F,
    audio_mode: AudioMode,
    max_duration: Duration,
) -> ExportResult<(Vec<BcMedia>, u32)> {
    let deadline = tokio::time::Instant::now() + max_duration;
    let mut state = Preflight::new(audio_mode);
    while !state.ready() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(state.incomplete_failure(state.failure(PreflightTermination::TimeBound)));
        }
        let next = match tokio::time::timeout(remaining, feed.next()).await {
            Ok(next) => next?,
            Err(_) => {
                return Err(state.incomplete_failure(state.failure(PreflightTermination::TimeBound)))
            }
        };
        let Some(media) = next else {
            return Err(state.incomplete_failure(state.failure(PreflightTermination::StreamEnd)));
        };
        state.push(media)?;
    }
    Ok(state.finish())
}

struct Preflight {
    audio_mode: AudioMode,
    buffered: Vec<BcMedia>,
    fps: u32,
    keyframe: bool,
    aac: bool,
    early_aac: Option<BcMedia>,
    observed_packets: usize,
    observed_bytes: usize,
    h264_bootstrap: H264Bootstrap,
    h264_video_packets: usize,
}

impl Preflight {
    fn new(audio_mode: AudioMode) -> Self {
        Self {
            audio_mode,
            buffered: Vec::new(),
            fps: DEFAULT_VIDEO_FPS,
            keyframe: false,
            aac: false,
            early_aac: None,
            observed_packets: 0,
            observed_bytes: 0,
            h264_bootstrap: H264Bootstrap::default(),
            h264_video_packets: 0,
        }
    }

    fn incomplete_failure(&self, fallback: ExportFailure) -> ExportFailure {
        if self.keyframe && self.audio_mode == AudioMode::Required && !self.aac {
            AUDIO_UNAVAILABLE_MISSING
        } else {
            fallback
        }
    }

    fn ready(&self) -> bool {
        self.keyframe && (self.audio_mode == AudioMode::None || self.aac)
    }

    fn content_category(&self) -> PreflightContent {
        if self.h264_video_packets == 0 {
            PreflightContent::NoH264
        } else if !self.h264_bootstrap.saw_annex_b {
            PreflightContent::AnnexbAbsent
        } else if self.h264_bootstrap.sps.is_none() {
            PreflightContent::SpsMissing
        } else if self.h264_bootstrap.pps.is_none() {
            PreflightContent::PpsMissing
        } else {
            PreflightContent::IdrMissingAfterConfig
        }
    }

    fn failure(&self, termination: PreflightTermination) -> ExportFailure {
        preflight_failure(termination, self.content_category())
    }

    fn push(&mut self, media: BcMedia) -> ExportResult<()> {
        let payload_bytes = media_payload_len(&media);
        let Some(observed_bytes) = self.observed_bytes.checked_add(payload_bytes) else {
            return Err(self.incomplete_failure(self.failure(PreflightTermination::ByteBound)));
        };
        if self.observed_packets >= PREFLIGHT_MAX_PACKETS {
            return Err(self.incomplete_failure(self.failure(PreflightTermination::PacketBound)));
        }
        if observed_bytes > PREFLIGHT_MAX_BYTES {
            return Err(self.incomplete_failure(self.failure(PreflightTermination::ByteBound)));
        }
        self.observed_packets += 1;
        self.observed_bytes = observed_bytes;

        match media {
            BcMedia::InfoV1(info) => {
                self.update_fps(info.fps);
                return Ok(());
            }
            BcMedia::InfoV2(info) => {
                self.update_fps(info.fps);
                return Ok(());
            }
            BcMedia::Iframe(frame) if frame.video_type == VideoType::H265 => {
                return Err(RecordingExportFailureCategory::H265Unsupported.failure());
            }
            BcMedia::Pframe(frame) if frame.video_type == VideoType::H265 => {
                return Err(RecordingExportFailureCategory::H265Unsupported.failure());
            }
            BcMedia::Adpcm(_) if self.audio_mode == AudioMode::Required && !self.aac => {
                return Err(AUDIO_UNAVAILABLE_ADPCM);
            }
            // Once valid AAC selected the fixed audio track, incompatible later
            // audio is ignored rather than being fed into the AAC parser or
            // deliberately truncating an otherwise valid stream.
            BcMedia::Adpcm(_) if self.audio_mode == AudioMode::Required => return Ok(()),
            BcMedia::Adpcm(_) | BcMedia::Aac(_) if self.audio_mode == AudioMode::None => {
                return Ok(());
            }
            BcMedia::Aac(aac) if !self.keyframe => {
                if !valid_aac(&aac) {
                    return Err(AUDIO_UNAVAILABLE_INVALID);
                }
                self.early_aac = Some(BcMedia::Aac(aac));
                return Ok(());
            }
            BcMedia::Iframe(mut frame) if !self.keyframe => {
                self.h264_video_packets += 1;
                let Some(data) = self.h264_bootstrap.observe(&frame.data)? else {
                    return Ok(());
                };
                frame.data = data;
                self.accept_keyframe(frame)?;
                return Ok(());
            }
            BcMedia::Pframe(frame) if !self.keyframe => {
                self.h264_video_packets += 1;
                let Some(data) = self.h264_bootstrap.observe(&frame.data)? else {
                    return Ok(());
                };
                self.accept_keyframe(BcMediaIframe {
                    video_type: frame.video_type,
                    microseconds: frame.microseconds,
                    time: None,
                    data,
                })?;
                return Ok(());
            }
            BcMedia::Aac(ref aac) => {
                if !valid_aac(aac) {
                    return Err(AUDIO_UNAVAILABLE_INVALID);
                }
                self.aac = true;
            }
            _ => {}
        }
        self.retain(media)
    }

    fn update_fps(&mut self, fps: u8) {
        if (1..=120).contains(&fps) {
            self.fps = u32::from(fps);
        }
    }

    fn accept_keyframe(&mut self, frame: BcMediaIframe) -> ExportResult<()> {
        self.keyframe = true;
        self.retain(BcMedia::Iframe(frame))?;
        if let Some(aac) = self.early_aac.take() {
            self.aac = true;
            self.retain(aac)?;
        }
        Ok(())
    }

    fn retain(&mut self, media: BcMedia) -> ExportResult<()> {
        self.buffered.push(media);
        Ok(())
    }

    fn finish(self) -> (Vec<BcMedia>, u32) {
        (self.buffered, self.fps)
    }
}

fn valid_aac(aac: &BcMediaAac) -> bool {
    aac.duration_info().is_some_and(|info| {
        info.parsed_len == info.payload_len && info.parsed_len as usize == aac.data.len()
    })
}

#[derive(Default)]
struct H264Bootstrap {
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
    saw_annex_b: bool,
}

impl H264Bootstrap {
    fn observe(&mut self, data: &[u8]) -> ExportResult<Option<Vec<u8>>> {
        let mut offset = 0usize;
        let mut first_idr = None;
        let mut packet_sps_before_idr = false;
        let mut packet_pps_before_idr = false;
        while let Some((start, prefix_len)) = next_annex_b_start(data, offset) {
            self.saw_annex_b = true;
            let nal_start = start + prefix_len;
            if nal_start >= data.len() {
                return Err(ExportFailure("recording H.264 Annex-B preflight failed"));
            }
            let next = next_annex_b_start(data, nal_start + 1)
                .map(|(next, _)| next)
                .unwrap_or(data.len());
            if next <= nal_start + 1 {
                return Err(ExportFailure("recording H.264 Annex-B preflight failed"));
            }
            let nal = &data[nal_start..next];
            match nal[0] & 0x1f {
                7 => {
                    self.sps = Some(normalize_parameter_set(nal)?);
                    if first_idr.is_none() {
                        packet_sps_before_idr = true;
                    }
                }
                8 => {
                    self.pps = Some(normalize_parameter_set(nal)?);
                    if first_idr.is_none() {
                        packet_pps_before_idr = true;
                    }
                }
                5 if first_idr.is_none() => {
                    first_idr = Some(start);
                }
                _ => {}
            }
            offset = next;
        }
        let Some(_first_idr) = first_idr else {
            return Ok(None);
        };
        let (Some(sps), Some(pps)) = (self.sps.as_deref(), self.pps.as_deref()) else {
            return Ok(None);
        };
        let prepend_sps = (!packet_sps_before_idr).then_some(sps);
        let prepend_pps = (!packet_pps_before_idr).then_some(pps);
        let capacity =
            prepend_sps.map_or(0, <[u8]>::len) + prepend_pps.map_or(0, <[u8]>::len) + data.len();
        if capacity > PREFLIGHT_MAX_BYTES {
            return Err(preflight_failure(
                PreflightTermination::ByteBound,
                PreflightContent::IdrMissingAfterConfig,
            ));
        }
        let mut synthesized = Vec::with_capacity(capacity);
        if let Some(sps) = prepend_sps {
            synthesized.extend_from_slice(sps);
        }
        if let Some(pps) = prepend_pps {
            synthesized.extend_from_slice(pps);
        }
        synthesized.extend_from_slice(data);
        Ok(Some(synthesized))
    }
}

fn normalize_parameter_set(nal: &[u8]) -> ExportResult<Vec<u8>> {
    let size = nal.len().checked_add(4).ok_or(ExportFailure(
        "recording H.264 parameter set exceeds preflight limit",
    ))?;
    if size > H264_PARAMETER_SET_MAX_BYTES {
        return Err(ExportFailure(
            "recording H.264 parameter set exceeds preflight limit",
        ));
    }
    let mut normalized = Vec::with_capacity(size);
    normalized.extend_from_slice(&[0, 0, 0, 1]);
    normalized.extend_from_slice(nal);
    Ok(normalized)
}

#[cfg(test)]
fn usable_h264_keyframe(data: &[u8]) -> bool {
    H264Bootstrap::default()
        .observe(data)
        .is_ok_and(|frame| frame.is_some())
}

fn next_annex_b_start(data: &[u8], from: usize) -> Option<(usize, usize)> {
    let mut index = from;
    while index + 3 <= data.len() {
        if index + 4 <= data.len() && data[index..index + 4] == [0, 0, 0, 1] {
            return Some((index, 4));
        }
        if data[index..index + 3] == [0, 0, 1] {
            return Some((index, 3));
        }
        index += 1;
    }
    None
}

fn media_payload_len(media: &BcMedia) -> usize {
    match media {
        BcMedia::InfoV1(_) | BcMedia::InfoV2(_) => 0,
        BcMedia::Iframe(frame) => frame.data.len(),
        BcMedia::Pframe(frame) => frame.data.len(),
        BcMedia::Aac(frame) => frame.data.len(),
        BcMedia::Adpcm(frame) => frame.data.len(),
    }
}

struct MediaClocks {
    video: TimestampNormalizer,
    video_duration_us: u64,
    audio_us: u64,
}

impl MediaClocks {
    fn new(fps: u32) -> Self {
        let video_duration_us = 1_000_000 / u64::from(fps.clamp(1, 120));
        Self {
            video: TimestampNormalizer::new(video_duration_us),
            video_duration_us,
            audio_us: 0,
        }
    }
}

fn push_media(
    muxer: &Fmp4Muxer,
    clocks: &mut MediaClocks,
    audio_mode: AudioMode,
    media: BcMedia,
) -> ExportResult<()> {
    match media {
        BcMedia::Iframe(frame) => {
            if frame.video_type != VideoType::H264 {
                return Err(RecordingExportFailureCategory::H265Unsupported.failure());
            }
            let pts = clocks.video.normalize(frame.microseconds);
            muxer
                .push_video(frame.data, pts, clocks.video_duration_us, true)
                .map_err(|_| output_error(muxer))?;
        }
        BcMedia::Pframe(frame) => {
            if frame.video_type != VideoType::H264 {
                return Err(RecordingExportFailureCategory::H265Unsupported.failure());
            }
            let pts = clocks.video.normalize(frame.microseconds);
            muxer
                .push_video(frame.data, pts, clocks.video_duration_us, false)
                .map_err(|_| output_error(muxer))?;
        }
        BcMedia::Aac(aac) if audio_mode == AudioMode::Required => {
            if !valid_aac(&aac) {
                return Err(RecordingExportFailureCategory::AacPacketInvalid.failure());
            }
            let duration = u64::from(
                aac.duration_info()
                    .expect("AAC was validated above")
                    .duration_us,
            );
            muxer
                .push_aac(aac.data, clocks.audio_us, duration)
                .map_err(|_| output_error(muxer))?;
            clocks.audio_us = clocks.audio_us.saturating_add(duration);
        }
        // AAC was selected during preflight. A later incompatible camera-audio
        // packet is omitted; it is never passed to the AAC track.
        BcMedia::Adpcm(_) if audio_mode == AudioMode::Required => {}
        BcMedia::InfoV1(_) | BcMedia::InfoV2(_) | BcMedia::Aac(_) | BcMedia::Adpcm(_) => {}
    }
    Ok(())
}

struct TimestampNormalizer {
    expected_frame_duration_us: u64,
    last_raw: Option<u32>,
    output: u64,
}

impl TimestampNormalizer {
    fn new(expected_frame_duration_us: u64) -> Self {
        Self {
            expected_frame_duration_us,
            last_raw: None,
            output: 0,
        }
    }

    fn normalize(&mut self, raw: u32) -> u64 {
        const MIN_SUPPORTED_FRAME_DURATION_US: u64 = 1_000_000 / 120;
        const MAX_PLAUSIBLE_FRAME_GAP_FRAMES: u64 = 240;
        const MAX_PLAUSIBLE_FRAME_GAP_US: u64 = 10_000_000;
        let Some(previous) = self.last_raw.replace(raw) else {
            self.output = 0;
            return 0;
        };
        let wrapped_delta = u64::from(raw.wrapping_sub(previous));
        // Accept ordinary jitter plus bounded gaps spanning at most 240
        // nominal frames, but never infer a rate above the supported 120 fps
        // ceiling or preserve a discontinuity longer than ten seconds.
        let minimum = (self.expected_frame_duration_us / 2).max(MIN_SUPPORTED_FRAME_DURATION_US);
        let maximum = self
            .expected_frame_duration_us
            .saturating_mul(MAX_PLAUSIBLE_FRAME_GAP_FRAMES)
            .min(MAX_PLAUSIBLE_FRAME_GAP_US);
        let advance = if (minimum..=maximum).contains(&wrapped_delta) {
            wrapped_delta
        } else {
            // Duplicate/tiny timestamps and discontinuous camera-clock resets
            // advance by the validated nominal frame duration. The following
            // plausible raw delta resumes immediately from the new raw base.
            self.expected_frame_duration_us
        };
        self.output = self.output.saturating_add(advance);
        self.output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use clap::Parser;
    use gstreamer::prelude::{ElementExt, ElementExtManual, GstBinExtManual, GstObjectExt, PadExt};
    use neolink_core::bcmedia::model::{BcMediaAdpcm, BcMediaIframe, BcMediaPframe};
    use std::{
        collections::VecDeque,
        io,
        process::{Command, Stdio},
        sync::atomic::{AtomicUsize, Ordering},
        sync::{Arc, Mutex},
    };
    #[cfg(unix)]
    use std::{os::fd::OwnedFd, os::unix::net::UnixStream};

    const AAC_ADTS_SILENCE: &str = "//FucAOf/N4CAExhdmM1OS4zNy4xMDAAAjBADv/xbnABf/wBGCAH";

    fn entry_json() -> String {
        serde_json::json!({
            "id": "/private/RecM01_20260730_010203_fixture.mp4",
            "name": "private-fixture",
            "sizeBytes": 1234
        })
        .to_string()
    }

    fn iframe(timestamp: u32, video_type: VideoType) -> BcMedia {
        let raw = include_bytes!("../../crates/core/src/bcmedia/samples/iframe_0.raw");
        BcMedia::Iframe(BcMediaIframe {
            video_type,
            microseconds: timestamp,
            time: None,
            data: raw[32..].to_vec(),
        })
    }

    fn iframe_with_data(timestamp: u32, data: Vec<u8>) -> BcMedia {
        BcMedia::Iframe(BcMediaIframe {
            video_type: VideoType::H264,
            microseconds: timestamp,
            time: None,
            data,
        })
    }

    fn pframe(timestamp: u32, video_type: VideoType) -> BcMedia {
        let raw = include_bytes!("../../crates/core/src/bcmedia/samples/pframe_0.raw");
        BcMedia::Pframe(BcMediaPframe {
            video_type,
            microseconds: timestamp,
            data: raw[24..].to_vec(),
        })
    }

    fn aac() -> BcMedia {
        BcMedia::Aac(BcMediaAac {
            data: base64::engine::general_purpose::STANDARD
                .decode(AAC_ADTS_SILENCE)
                .unwrap(),
        })
    }

    fn ffprobe_document(bytes: &[u8], entries: &str) -> Option<serde_json::Value> {
        let mut child = match Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-show_entries",
                entries,
                "-of",
                "json",
                "-i",
                "pipe:0",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
            Err(error) => panic!("spawn ffprobe: {error}"),
        };
        child
            .stdin
            .take()
            .expect("ffprobe stdin")
            .write_all(bytes)
            .expect("write fMP4 to ffprobe stdin");
        let output = child.wait_with_output().expect("wait for ffprobe");
        assert!(
            output.status.success(),
            "ffprobe rejected deterministic fMP4: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Some(serde_json::from_slice(&output.stdout).expect("parse ffprobe JSON"))
    }

    fn assert_ffprobe_streams(bytes: &[u8], expect_audio: bool) {
        let Some(document) = ffprobe_document(bytes, "stream=codec_type,codec_name") else {
            return;
        };
        let streams = document["streams"].as_array().expect("ffprobe streams");
        assert!(streams
            .iter()
            .any(|stream| { stream["codec_type"] == "video" && stream["codec_name"] == "h264" }));
        assert_eq!(
            streams
                .iter()
                .any(|stream| { stream["codec_type"] == "audio" && stream["codec_name"] == "aac" }),
            expect_audio,
            "ffprobe audio-track presence"
        );
    }

    struct FakeReplay {
        packets: VecDeque<BcMedia>,
        end: RecordingReplayEnd,
        shutdowns: Arc<AtomicUsize>,
        shutdown_failure: bool,
        wait_after_packets: bool,
        next_failure: Option<ExportFailure>,
    }

    impl FakeReplay {
        fn new(packets: Vec<BcMedia>, end: RecordingReplayEnd) -> (Self, Arc<AtomicUsize>) {
            let shutdowns = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    packets: packets.into(),
                    end,
                    shutdowns: shutdowns.clone(),
                    shutdown_failure: false,
                    wait_after_packets: false,
                    next_failure: None,
                },
                shutdowns,
            )
        }

        fn fail_shutdown(mut self) -> Self {
            self.shutdown_failure = true;
            self
        }

        fn wait_after_packets(mut self) -> Self {
            self.wait_after_packets = true;
            self
        }

        fn fail_next_after_packets(mut self, failure: ExportFailure) -> Self {
            self.next_failure = Some(failure);
            self
        }
    }

    impl ReplayFeed for FakeReplay {
        fn next(
            &mut self,
        ) -> Pin<Box<dyn Future<Output = ExportResult<Option<BcMedia>>> + Send + '_>> {
            Box::pin(async move {
                if let Some(media) = self.packets.pop_front() {
                    Ok(Some(media))
                } else if let Some(failure) = self.next_failure.take() {
                    Err(failure)
                } else if self.wait_after_packets {
                    std::future::pending().await
                } else {
                    Ok(None)
                }
            })
        }

        fn shutdown(
            &mut self,
        ) -> Pin<Box<dyn Future<Output = ExportResult<RecordingReplayEnd>> + Send + '_>> {
            Box::pin(async move {
                self.shutdowns.fetch_add(1, Ordering::SeqCst);
                if self.shutdown_failure {
                    Err(RecordingExportFailureCategory::ReplayStop.failure())
                } else {
                    Ok(self.end)
                }
            })
        }
    }

    #[derive(Clone, Default)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct BrokenWriter;

    impl Write for BrokenWriter {
        fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "fixture closed"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "fixture closed"))
        }
    }

    #[derive(Clone, Default)]
    struct FlushCountingWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
        flushes: Arc<AtomicUsize>,
    }

    impl Write for FlushCountingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn writer_box<W: Write + Send + 'static>(writer: W) -> OutputTarget {
        OutputTarget::test_writer(Box::new(writer))
    }

    fn video_packets() -> Vec<BcMedia> {
        let mut packets = vec![
            pframe(10_000, VideoType::H264),
            iframe(20_000, VideoType::H264),
        ];
        for index in 1..=30 {
            packets.push(pframe(20_000 + index * 40_000, VideoType::H264));
        }
        packets.push(iframe(1_260_000, VideoType::H264));
        for index in 1..=5 {
            packets.push(pframe(1_260_000 + index * 40_000, VideoType::H264));
        }
        packets
    }

    fn assert_fragmented_mp4(bytes: &[u8]) {
        let offset = |name: &[u8; 4]| {
            bytes
                .windows(4)
                .position(|window| window == name)
                .unwrap_or_else(|| panic!("missing MP4 box/type {:?}", name))
        };
        let ftyp = offset(b"ftyp");
        let moov = offset(b"moov");
        let moof = offset(b"moof");
        let mdat = offset(b"mdat");
        assert!(ftyp < moov && moov < moof && moof < mdat);
    }

    #[test]
    fn clap_exposes_distinct_bounded_export_options() {
        let parsed = crate::cmdline::Opt::try_parse_from([
            "neolink",
            "--config",
            "fixture.toml",
            "recording-export",
            "camera-a",
            "--channel",
            "1",
            "--stream",
            "main",
            "--audio",
            "required",
            "--max-duration-seconds",
            "30",
            "--max-media-bytes",
            "4096",
        ])
        .unwrap();
        let Some(crate::cmdline::Command::RecordingExport(export)) = parsed.cmd else {
            panic!("recording-export command was not selected");
        };
        assert_eq!(export.camera, "camera-a");
        assert_eq!(export.channel, Some(1));
        assert_eq!(export.stream, CmdStream::Main);
        assert_eq!(export.audio, AudioMode::Required);
        assert_eq!(export.max_duration_seconds, 30);
        assert_eq!(export.max_media_bytes, 4096);
        let replay = replay_options(&export, 1);
        assert_eq!(replay.buffer_size, HARD_RECORDING_REPLAY_BUFFER_SIZE);
        assert_eq!(replay.buffer_size, 32);
        assert_eq!(
            replay.max_buffered_media_bytes,
            HARD_RECORDING_REPLAY_MAX_BUFFERED_MEDIA_BYTES
        );
        assert_eq!(replay.max_buffered_media_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn clap_rejects_limits_above_core_ceilings() {
        assert!(crate::cmdline::Opt::try_parse_from([
            "neolink",
            "recording-export",
            "camera-a",
            "--max-duration-seconds",
            "7201",
        ])
        .is_err());
        assert!(crate::cmdline::Opt::try_parse_from([
            "neolink",
            "recording-export",
            "camera-a",
            "--max-media-bytes",
            "8589934593",
        ])
        .is_err());
    }

    #[tokio::test]
    async fn stdin_accepts_one_object_and_rejects_empty_multiple_or_oversized_input() {
        let parsed = read_recording_entry(entry_json().as_bytes()).await.unwrap();
        assert_eq!(parsed.name.as_deref(), Some("private-fixture"));
        assert!(read_recording_entry(b"  \n\t".as_slice()).await.is_err());
        let multiple = format!("{} {}", entry_json(), entry_json());
        assert!(read_recording_entry(multiple.as_bytes()).await.is_err());
        let oversized = vec![b' '; MAX_ENTRY_JSON_BYTES + 1];
        assert!(read_recording_entry(oversized.as_slice()).await.is_err());
        assert!(read_recording_entry(b"[]".as_slice()).await.is_err());
    }

    #[test]
    fn public_failures_never_echo_private_input() {
        let sentinel = "/private/RecM01_20260730_010203_DO_NOT_LOG.mp4";
        for failure in [
            ExportFailure("stdin must contain one valid RecordingEntry JSON object"),
            RecordingExportFailureCategory::ReplayRequest.failure(),
            RecordingExportFailureCategory::ReplayStream.failure(),
            RecordingExportFailureCategory::PipelineOther.failure(),
            RecordingExportFailureCategory::H264Parse.failure(),
            RecordingExportFailureCategory::AacParse.failure(),
            RecordingExportFailureCategory::Mp4Mux.failure(),
            RecordingExportFailureCategory::FinalizationTimeout.failure(),
            ExportFailure("recording H.264 Annex-B preflight failed"),
            ExportFailure("recording H.264 parameter set exceeds preflight limit"),
        ] {
            let output = failure.to_string();
            assert!(!output.contains(sentinel));
            assert!(!output.contains("uid"));
            assert!(!output.contains("password"));
        }
    }

    #[test]
    fn replay_item_error_classifier_is_static_redacted_and_preserves_nested_replay_cause() {
        let private = "PRIVATE_RECORDING_IDENTIFIER_DO_NOT_LOG";
        for error in [
            NeolinkError::NomError(private.to_owned()),
            NeolinkError::NomIncomplete(1),
        ] {
            let failure = replay_item_error(&error);
            assert_eq!(
                failure,
                RecordingExportFailureCategory::ReplayInvalidMedia.failure()
            );
            assert!(!failure.to_string().contains(private));
        }

        for (error, expected) in [
            (
                NeolinkError::TimeoutDisconnected,
                RecordingExportFailureCategory::ReplayTimeout,
            ),
            (
                NeolinkError::RelayTerminate,
                RecordingExportFailureCategory::ReplayRelayTerminated,
            ),
            (
                NeolinkError::CameraTerminate,
                RecordingExportFailureCategory::ReplayCameraTerminated,
            ),
            (
                NeolinkError::DroppedSubscriber,
                RecordingExportFailureCategory::ReplayConnectionDropped,
            ),
            (
                NeolinkError::Io(Arc::new(io::Error::other(private))),
                RecordingExportFailureCategory::ReplayIo,
            ),
            (
                NeolinkError::TokioBcSendError,
                RecordingExportFailureCategory::ReplaySend,
            ),
            (
                NeolinkError::OtherString(private.to_owned()),
                RecordingExportFailureCategory::ReplayStream,
            ),
        ] {
            let failure = replay_item_error(&error);
            assert_eq!(failure, expected.failure());
            assert!(!failure.to_string().contains(private));

            let nested = NeolinkError::RecordingReplayAndStopFailed {
                replay: Arc::new(error),
                stop: Arc::new(NeolinkError::Io(Arc::new(io::Error::other(private)))),
            };
            let nested_failure = replay_item_error(&nested);
            assert_eq!(nested_failure, expected.failure());
            assert!(!nested_failure.to_string().contains(private));
        }

        let pure_stop = NeolinkError::RecordingReplayStopFailed {
            stop: Arc::new(NeolinkError::Io(Arc::new(io::Error::other(private)))),
        };
        assert_eq!(
            replay_item_error(&pure_stop),
            RecordingExportFailureCategory::ReplayStop.failure()
        );
        assert!(!replay_item_error(&pure_stop).to_string().contains(private));
    }

    #[test]
    fn timestamp_normalizer_preserves_plausible_jitter_gaps_and_wrap() {
        let mut jitter = TimestampNormalizer::new(40_000);
        assert_eq!(jitter.normalize(1_000_000), 0);
        assert_eq!(jitter.normalize(1_038_000), 38_000);
        assert_eq!(jitter.normalize(1_080_000), 80_000);
        assert_eq!(jitter.normalize(1_200_000), 200_000);

        let mut wrap = TimestampNormalizer::new(40_000);
        assert_eq!(wrap.normalize(u32::MAX - 20_000), 0);
        assert_eq!(wrap.normalize(19_999), 40_000);
        assert_eq!(wrap.normalize(59_999), 80_000);
    }

    #[test]
    fn timestamp_normalizer_replaces_duplicate_tiny_and_reset_deltas_with_nominal_duration() {
        let mut timestamps = TimestampNormalizer::new(40_000);
        assert_eq!(timestamps.normalize(1_000_000_000), 0);
        assert_eq!(timestamps.normalize(1_000_000_000), 40_000);
        assert_eq!(timestamps.normalize(1_000_001_000), 80_000);
        assert_eq!(timestamps.normalize(100), 120_000);
        assert_eq!(timestamps.normalize(40_100), 160_000);

        let mut large_gap = TimestampNormalizer::new(40_000);
        assert_eq!(large_gap.normalize(0), 0);
        assert_eq!(large_gap.normalize(9_600_001), 40_000);
    }

    #[test]
    fn timestamp_normalizer_uses_validated_twenty_and_twenty_five_fps_durations() {
        let mut twenty_fps = MediaClocks::new(20);
        assert_eq!(twenty_fps.video_duration_us, 50_000);
        assert_eq!(twenty_fps.video.normalize(500), 0);
        assert_eq!(twenty_fps.video.normalize(500), 50_000);
        assert_eq!(twenty_fps.video.normalize(50_500), 100_000);

        let mut twenty_five_fps = MediaClocks::new(25);
        assert_eq!(twenty_five_fps.video_duration_us, 40_000);
        assert_eq!(twenty_five_fps.video.normalize(500), 0);
        assert_eq!(twenty_five_fps.video.normalize(500), 40_000);
        assert_eq!(twenty_five_fps.video.normalize(40_500), 80_000);
    }

    #[test]
    fn preflight_drops_pre_keyframe_video_and_starts_with_h264_idr() {
        let mut state = Preflight::new(AudioMode::None);
        state.push(pframe(1, VideoType::H264)).unwrap();
        state.push(pframe(2, VideoType::H264)).unwrap();
        assert!(!state.ready());
        state.push(iframe(3, VideoType::H264)).unwrap();
        assert!(state.ready());
        let (packets, _) = state.finish();
        assert_eq!(packets.len(), 1);
        assert!(matches!(packets[0], BcMedia::Iframe(_)));
    }

    #[test]
    fn h264_keyframe_validation_accepts_three_and_four_byte_annex_b_start_codes() {
        let four_byte = [
            0, 0, 0, 1, 0x67, 0x01, 0, 0, 0, 1, 0x68, 0x02, 0, 0, 0, 1, 0x65, 0x03,
        ];
        let three_byte = [
            0, 0, 1, 0x67, 0x01, 0, 0, 1, 0x68, 0x02, 0, 0, 1, 0x65, 0x03,
        ];
        assert!(usable_h264_keyframe(&four_byte));
        assert!(usable_h264_keyframe(&three_byte));
    }

    #[test]
    fn h264_keyframe_validation_rejects_missing_or_truncated_nals() {
        for malformed in [
            vec![],
            vec![0, 0, 0, 1],
            vec![0, 0, 1, 0x67, 1, 0, 0, 1, 0x68, 2],
            vec![0, 0, 1, 0x67, 1, 0, 0, 1, 0x65, 3],
            vec![0, 0, 1, 0x68, 2, 0, 0, 1, 0x65, 3],
            vec![0, 0, 1, 0x67, 1, 0, 0, 1, 0x68, 2, 0, 0, 1],
        ] {
            assert!(!usable_h264_keyframe(&malformed));
        }
    }

    fn count_annex_b_nal_type(data: &[u8], expected: u8) -> usize {
        let mut offset = 0;
        let mut count = 0;
        while let Some((start, prefix_len)) = next_annex_b_start(data, offset) {
            let nal_start = start + prefix_len;
            if nal_start >= data.len() {
                break;
            }
            if data[nal_start] & 0x1f == expected {
                count += 1;
            }
            offset = next_annex_b_start(data, nal_start + 1)
                .map(|(next, _)| next)
                .unwrap_or(data.len());
        }
        count
    }

    #[test]
    fn same_packet_parameter_sets_are_not_prepended_twice() {
        let packet = vec![
            0, 0, 1, 0x67, 0x01, 0, 0, 0, 1, 0x68, 0x02, 0, 0, 1, 0x65, 0x03,
        ];
        let synthesized = H264Bootstrap::default().observe(&packet).unwrap().unwrap();
        assert_eq!(count_annex_b_nal_type(&synthesized, 7), 1);
        assert_eq!(count_annex_b_nal_type(&synthesized, 8), 1);
        assert_eq!(count_annex_b_nal_type(&synthesized, 5), 1);
    }

    #[test]
    fn idr_before_same_packet_parameter_sets_is_normalized_into_decodable_order() {
        let packet = vec![
            0, 0, 1, 0x65, 0x03, 0, 0, 1, 0x67, 0x01, 0, 0, 0, 1, 0x68, 0x02,
        ];
        let synthesized = H264Bootstrap::default().observe(&packet).unwrap().unwrap();
        assert!(synthesized.starts_with(&[0, 0, 0, 1, 0x67, 0x01]));
        assert!(usable_h264_keyframe(&synthesized));
    }

    #[test]
    fn trailing_malformed_nal_is_rejected_even_after_complete_keyframe() {
        let packet = vec![
            0, 0, 1, 0x67, 0x01, 0, 0, 1, 0x68, 0x02, 0, 0, 1, 0x65, 0x03, 0, 0, 1,
        ];
        assert_eq!(
            H264Bootstrap::default().observe(&packet).unwrap_err(),
            ExportFailure("recording H.264 Annex-B preflight failed")
        );
    }

    #[test]
    fn preflight_assembles_split_sps_pps_and_idr_without_retaining_leading_video() {
        let mut state = Preflight::new(AudioMode::None);
        state
            .push(iframe_with_data(1, vec![0, 0, 1, 0x67, 0x11]))
            .unwrap();
        state
            .push(BcMedia::Pframe(BcMediaPframe {
                video_type: VideoType::H264,
                microseconds: 2,
                data: vec![0, 0, 0, 1, 0x68, 0x22],
            }))
            .unwrap();
        assert!(!state.ready());
        state
            .push(BcMedia::Pframe(BcMediaPframe {
                video_type: VideoType::H264,
                microseconds: 3,
                data: vec![0, 0, 1, 0x65, 0x33],
            }))
            .unwrap();
        assert!(state.ready());
        let (packets, _) = state.finish();
        assert_eq!(packets.len(), 1);
        let BcMedia::Iframe(frame) = &packets[0] else {
            panic!("IDR from a wire P-frame was not promoted to a keyframe");
        };
        assert_eq!(frame.microseconds, 3);
        assert!(usable_h264_keyframe(&frame.data));
        assert!(frame.data.starts_with(&[0, 0, 0, 1, 0x67, 0x11]));
    }

    #[test]
    fn preflight_requires_parameter_sets_before_idr_and_uses_latest_duplicates() {
        let mut state = Preflight::new(AudioMode::None);
        state
            .push(iframe_with_data(1, vec![0, 0, 1, 0x65, 0x01]))
            .unwrap();
        state
            .push(iframe_with_data(2, vec![0, 0, 1, 0x67, 0x10]))
            .unwrap();
        state
            .push(iframe_with_data(3, vec![0, 0, 1, 0x67, 0x20]))
            .unwrap();
        state
            .push(iframe_with_data(4, vec![0, 0, 1, 0x68, 0x30]))
            .unwrap();
        assert!(!state.ready());
        state
            .push(iframe_with_data(5, vec![0, 0, 1, 0x65, 0x40]))
            .unwrap();
        let (packets, _) = state.finish();
        let BcMedia::Iframe(frame) = &packets[0] else {
            panic!("expected synthesized keyframe");
        };
        assert!(frame
            .data
            .windows(6)
            .any(|window| window == [0, 0, 0, 1, 0x67, 0x20]));
        assert!(!frame
            .data
            .windows(6)
            .any(|window| window == [0, 0, 0, 1, 0x67, 0x10]));
    }

    #[test]
    fn preflight_rejects_truncated_and_oversized_parameter_sets() {
        let mut truncated = Preflight::new(AudioMode::None);
        assert_eq!(
            truncated
                .push(iframe_with_data(1, vec![0, 0, 0, 1]))
                .unwrap_err(),
            ExportFailure("recording H.264 Annex-B preflight failed")
        );

        let mut oversized_data = vec![0, 0, 0, 1, 0x67];
        oversized_data.resize(H264_PARAMETER_SET_MAX_BYTES + 1, 0x55);
        let mut oversized = Preflight::new(AudioMode::None);
        assert_eq!(
            oversized
                .push(iframe_with_data(1, oversized_data))
                .unwrap_err(),
            ExportFailure("recording H.264 parameter set exceeds preflight limit")
        );
    }

    #[test]
    fn missing_idr_still_obeys_the_existing_packet_bound() {
        let mut state = Preflight::new(AudioMode::None);
        state
            .push(iframe_with_data(1, vec![0, 0, 1, 0x67, 0x11]))
            .unwrap();
        state
            .push(iframe_with_data(2, vec![0, 0, 1, 0x68, 0x22]))
            .unwrap();
        for index in 2..PREFLIGHT_MAX_PACKETS {
            state.push(pframe(index as u32, VideoType::H264)).unwrap();
        }
        assert_eq!(
            state.push(pframe(999, VideoType::H264)).unwrap_err(),
            preflight_failure(
                PreflightTermination::PacketBound,
                PreflightContent::IdrMissingAfterConfig,
            )
        );
        assert!(!state.ready());
    }

    #[test]
    fn preflight_skips_malformed_h264_iframes_until_first_usable_keyframe() {
        let mut state = Preflight::new(AudioMode::None);
        state
            .push(iframe_with_data(1, vec![0, 0, 1, 0x65, 0x01]))
            .unwrap();
        state.push(pframe(2, VideoType::H264)).unwrap();
        assert!(!state.ready());
        state.push(iframe(3, VideoType::H264)).unwrap();
        assert!(state.ready());
        let (packets, _) = state.finish();
        assert_eq!(packets.len(), 1);
        let BcMedia::Iframe(frame) = &packets[0] else {
            panic!("preflight did not retain the usable iframe");
        };
        assert_eq!(frame.microseconds, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_output_is_written_when_split_parameter_sets_never_reach_idr() {
        let packets = vec![
            iframe_with_data(1, vec![0, 0, 1, 0x67, 0x11]),
            iframe_with_data(2, vec![0, 0, 1, 0x68, 0x22]),
        ];
        let (mut replay, shutdowns) = FakeReplay::new(packets, RecordingReplayEnd::CameraEnd);
        let output = SharedWriter::default();
        let error = drive_and_shutdown(&mut replay, AudioMode::None, writer_box(output.clone()))
            .await
            .unwrap_err();
        assert_eq!(
            error,
            preflight_failure(
                PreflightTermination::StreamEnd,
                PreflightContent::IdrMissingAfterConfig,
            )
        );
        assert!(output
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deadline_after_observed_packet_uses_time_bound_and_structural_content() {
        let (replay, _) = FakeReplay::new(
            vec![iframe_with_data(1, vec![0x65, 0x01])],
            RecordingReplayEnd::CameraEnd,
        );
        let mut replay = replay.wait_after_packets();
        let error = collect_preflight_with_duration(
            &mut replay,
            AudioMode::None,
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            preflight_failure(
                PreflightTermination::TimeBound,
                PreflightContent::AnnexbAbsent,
            )
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn malformed_h264_drive_reports_static_parse_category_without_private_text() {
        let malformed = iframe_with_data(
            1,
            vec![
                0, 0, 0, 1, 0x67, 0x01, 0, 0, 0, 1, 0x68, 0x02, 0, 0, 0, 1, 0x65, 0x03,
            ],
        );
        let (mut replay, shutdowns) =
            FakeReplay::new(vec![malformed], RecordingReplayEnd::CameraEnd);
        let output = SharedWriter::default();
        let error = drive_and_shutdown(&mut replay, AudioMode::None, writer_box(output.clone()))
            .await
            .unwrap_err();
        assert_eq!(error, RecordingExportFailureCategory::H264Parse.failure());
        assert!(!error.to_string().contains("private"));
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn preflight_rejects_h265_adpcm_and_invalid_aac_before_output() {
        let mut h265 = Preflight::new(AudioMode::None);
        assert_eq!(
            h265.push(iframe(0, VideoType::H265)).unwrap_err(),
            RecordingExportFailureCategory::H265Unsupported.failure()
        );
        let mut adpcm = Preflight::new(AudioMode::Required);
        assert_eq!(
            adpcm
                .push(BcMedia::Adpcm(BcMediaAdpcm { data: vec![0; 8] }))
                .unwrap_err(),
            AUDIO_UNAVAILABLE_ADPCM
        );
        let mut invalid_aac = Preflight::new(AudioMode::Required);
        assert_eq!(
            invalid_aac
                .push(BcMedia::Aac(BcMediaAac { data: vec![0; 8] }))
                .unwrap_err(),
            AUDIO_UNAVAILABLE_INVALID
        );
        let mut missing_aac = Preflight::new(AudioMode::Required);
        missing_aac.push(iframe(0, VideoType::H264)).unwrap();
        assert_eq!(
            missing_aac.incomplete_failure(missing_aac.failure(PreflightTermination::TimeBound)),
            AUDIO_UNAVAILABLE_MISSING
        );

        let mut no_keyframe = Preflight::new(AudioMode::Required);
        let oversized = BcMedia::Pframe(BcMediaPframe {
            video_type: VideoType::H264,
            microseconds: 0,
            data: vec![0; PREFLIGHT_MAX_BYTES + 1],
        });
        assert_eq!(
            no_keyframe.push(oversized.clone()).unwrap_err(),
            preflight_failure(PreflightTermination::ByteBound, PreflightContent::NoH264,)
        );
        let mut usable_keyframe = Preflight::new(AudioMode::Required);
        usable_keyframe.push(iframe(0, VideoType::H264)).unwrap();
        assert_eq!(
            usable_keyframe.push(oversized).unwrap_err(),
            AUDIO_UNAVAILABLE_MISSING
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn audio_fallback_marker_is_exclusive_to_eligible_zero_output_failures() {
        let mut video_without_audio = vec![iframe(0, VideoType::H264)];
        for timestamp in 1..=PREFLIGHT_MAX_PACKETS {
            video_without_audio.push(pframe(timestamp as u32, VideoType::H264));
        }
        let eligible = [
            (vec![iframe(0, VideoType::H264)], AUDIO_UNAVAILABLE_MISSING),
            (video_without_audio, AUDIO_UNAVAILABLE_MISSING),
            (
                vec![
                    iframe(0, VideoType::H264),
                    BcMedia::Adpcm(BcMediaAdpcm { data: vec![0; 8] }),
                ],
                AUDIO_UNAVAILABLE_ADPCM,
            ),
            (
                vec![
                    iframe(0, VideoType::H264),
                    BcMedia::Aac(BcMediaAac { data: vec![0; 8] }),
                ],
                AUDIO_UNAVAILABLE_INVALID,
            ),
        ];
        for (packets, expected) in eligible {
            let (mut replay, shutdowns) = FakeReplay::new(packets, RecordingReplayEnd::Cancelled);
            let output = SharedWriter::default();
            let error =
                drive_and_shutdown(&mut replay, AudioMode::Required, writer_box(output.clone()))
                    .await
                    .unwrap_err();
            assert_eq!(error, expected);
            assert!(error
                .to_string()
                .starts_with("RECORDING_EXPORT_AUDIO_UNAVAILABLE:"));
            assert!(output
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty());
            assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
        }

        let (mut h265, _) = FakeReplay::new(
            vec![iframe(0, VideoType::H265)],
            RecordingReplayEnd::Cancelled,
        );
        let output = SharedWriter::default();
        let error = drive_and_shutdown(&mut h265, AudioMode::Required, writer_box(output.clone()))
            .await
            .unwrap_err();
        assert!(!error
            .to_string()
            .contains("RECORDING_EXPORT_AUDIO_UNAVAILABLE"));
        assert!(output
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
    }

    #[test]
    fn preflight_bound_failures_and_packet_over_byte_precedence_are_exact() {
        let mut state = Preflight::new(AudioMode::None);
        for index in 0..PREFLIGHT_MAX_PACKETS {
            state
                .push(iframe_with_data(index as u32, vec![0x41, 0x01]))
                .unwrap();
        }
        assert_eq!(
            state
                .push(iframe_with_data(999, vec![0x41, 0x01]))
                .unwrap_err(),
            preflight_failure(
                PreflightTermination::PacketBound,
                PreflightContent::AnnexbAbsent,
            )
        );
        let oversized_at_packet_cap = BcMedia::Pframe(BcMediaPframe {
            video_type: VideoType::H264,
            microseconds: 1_000,
            data: vec![0; PREFLIGHT_MAX_BYTES + 1],
        });
        assert_eq!(
            state.push(oversized_at_packet_cap).unwrap_err(),
            preflight_failure(
                PreflightTermination::PacketBound,
                PreflightContent::AnnexbAbsent,
            )
        );

        let mut bytes = Preflight::new(AudioMode::None);
        let oversized = BcMedia::Pframe(BcMediaPframe {
            video_type: VideoType::H264,
            microseconds: 0,
            data: vec![0; PREFLIGHT_MAX_BYTES + 1],
        });
        assert_eq!(
            bytes.push(oversized).unwrap_err(),
            preflight_failure(PreflightTermination::ByteBound, PreflightContent::NoH264,)
        );
    }

    #[test]
    fn structural_preflight_content_has_exact_static_categories() {
        let no_video = Preflight::new(AudioMode::None);
        assert_eq!(no_video.content_category(), PreflightContent::NoH264);

        let mut no_annex_b = Preflight::new(AudioMode::None);
        no_annex_b
            .push(iframe_with_data(1, vec![0x65, 0x01]))
            .unwrap();
        assert_eq!(
            no_annex_b.content_category(),
            PreflightContent::AnnexbAbsent
        );

        let mut no_sps = Preflight::new(AudioMode::None);
        no_sps
            .push(iframe_with_data(
                1,
                vec![0, 0, 1, 0x68, 0x01, 0, 0, 1, 0x65, 0x02],
            ))
            .unwrap();
        assert_eq!(no_sps.content_category(), PreflightContent::SpsMissing);

        let mut no_pps = Preflight::new(AudioMode::None);
        no_pps
            .push(iframe_with_data(
                1,
                vec![0, 0, 1, 0x67, 0x01, 0, 0, 1, 0x65, 0x02],
            ))
            .unwrap();
        assert_eq!(no_pps.content_category(), PreflightContent::PpsMissing);

        let mut no_idr = Preflight::new(AudioMode::None);
        no_idr
            .push(iframe_with_data(
                1,
                vec![0, 0, 1, 0x67, 0x01, 0, 0, 1, 0x68, 0x02],
            ))
            .unwrap();
        assert_eq!(
            no_idr.content_category(),
            PreflightContent::IdrMissingAfterConfig
        );
    }

    #[test]
    fn every_preflight_cross_product_is_exact_private_data_free_and_stable() {
        let private = "/private/RecM01_20260730_010203_DO_NOT_LOG.mp4";
        let expected = [
            (
                PreflightTermination::StreamEnd,
                PreflightContent::NoH264,
                "recording codec preflight failed: stream_end:no_h264",
            ),
            (
                PreflightTermination::StreamEnd,
                PreflightContent::AnnexbAbsent,
                "recording codec preflight failed: stream_end:annexb_absent",
            ),
            (
                PreflightTermination::StreamEnd,
                PreflightContent::SpsMissing,
                "recording codec preflight failed: stream_end:sps_missing",
            ),
            (
                PreflightTermination::StreamEnd,
                PreflightContent::PpsMissing,
                "recording codec preflight failed: stream_end:pps_missing",
            ),
            (
                PreflightTermination::StreamEnd,
                PreflightContent::IdrMissingAfterConfig,
                "recording codec preflight failed: stream_end:idr_missing_after_config",
            ),
            (
                PreflightTermination::TimeBound,
                PreflightContent::NoH264,
                "recording codec preflight failed: time_bound:no_h264",
            ),
            (
                PreflightTermination::TimeBound,
                PreflightContent::AnnexbAbsent,
                "recording codec preflight failed: time_bound:annexb_absent",
            ),
            (
                PreflightTermination::TimeBound,
                PreflightContent::SpsMissing,
                "recording codec preflight failed: time_bound:sps_missing",
            ),
            (
                PreflightTermination::TimeBound,
                PreflightContent::PpsMissing,
                "recording codec preflight failed: time_bound:pps_missing",
            ),
            (
                PreflightTermination::TimeBound,
                PreflightContent::IdrMissingAfterConfig,
                "recording codec preflight failed: time_bound:idr_missing_after_config",
            ),
            (
                PreflightTermination::PacketBound,
                PreflightContent::NoH264,
                "recording codec preflight failed: packet_bound:no_h264",
            ),
            (
                PreflightTermination::PacketBound,
                PreflightContent::AnnexbAbsent,
                "recording codec preflight failed: packet_bound:annexb_absent",
            ),
            (
                PreflightTermination::PacketBound,
                PreflightContent::SpsMissing,
                "recording codec preflight failed: packet_bound:sps_missing",
            ),
            (
                PreflightTermination::PacketBound,
                PreflightContent::PpsMissing,
                "recording codec preflight failed: packet_bound:pps_missing",
            ),
            (
                PreflightTermination::PacketBound,
                PreflightContent::IdrMissingAfterConfig,
                "recording codec preflight failed: packet_bound:idr_missing_after_config",
            ),
            (
                PreflightTermination::ByteBound,
                PreflightContent::NoH264,
                "recording codec preflight failed: byte_bound:no_h264",
            ),
            (
                PreflightTermination::ByteBound,
                PreflightContent::AnnexbAbsent,
                "recording codec preflight failed: byte_bound:annexb_absent",
            ),
            (
                PreflightTermination::ByteBound,
                PreflightContent::SpsMissing,
                "recording codec preflight failed: byte_bound:sps_missing",
            ),
            (
                PreflightTermination::ByteBound,
                PreflightContent::PpsMissing,
                "recording codec preflight failed: byte_bound:pps_missing",
            ),
            (
                PreflightTermination::ByteBound,
                PreflightContent::IdrMissingAfterConfig,
                "recording codec preflight failed: byte_bound:idr_missing_after_config",
            ),
        ];
        assert_eq!(expected.len(), 4 * 5);
        for (termination, content, expected_line) in expected {
            let line = preflight_failure(termination, content).to_string();
            assert_eq!(line, expected_line);
            assert!(!line.contains(private));
            assert!(!line.contains("uid"));
            assert!(!line.contains("password"));
        }
    }

    #[test]
    fn every_post_preflight_category_is_exact_private_data_free_and_stable() {
        let private = "/private/RecM01_20260730_010203_DO_NOT_LOG.mp4";
        let expected = [
            (
                RecordingExportFailureCategory::ReplayRequest,
                "recording export failed: replay_request_rejected",
            ),
            (
                RecordingExportFailureCategory::OutputInit,
                "recording export failed: output_init",
            ),
            (
                RecordingExportFailureCategory::MuxInit,
                "recording export failed: mux_init",
            ),
            (
                RecordingExportFailureCategory::ReplayInvalidMedia,
                "recording export failed: replay_invalid_media",
            ),
            (
                RecordingExportFailureCategory::ReplayTimeout,
                "recording export failed: replay_timeout",
            ),
            (
                RecordingExportFailureCategory::ReplayRelayTerminated,
                "recording export failed: replay_relay_terminated",
            ),
            (
                RecordingExportFailureCategory::ReplayCameraTerminated,
                "recording export failed: replay_camera_terminated",
            ),
            (
                RecordingExportFailureCategory::ReplayConnectionDropped,
                "recording export failed: replay_connection_dropped",
            ),
            (
                RecordingExportFailureCategory::ReplayIo,
                "recording export failed: replay_io",
            ),
            (
                RecordingExportFailureCategory::ReplaySend,
                "recording export failed: replay_send",
            ),
            (
                RecordingExportFailureCategory::ReplayStream,
                "recording export failed: replay_stream",
            ),
            (
                RecordingExportFailureCategory::ReplayStop,
                "recording export failed: replay_stop",
            ),
            (
                RecordingExportFailureCategory::ReplayDurationLimit,
                "recording export failed: replay_duration_limit",
            ),
            (
                RecordingExportFailureCategory::ReplayByteLimit,
                "recording export failed: replay_byte_limit",
            ),
            (
                RecordingExportFailureCategory::ReplayBufferLimit,
                "recording export failed: replay_buffer_limit",
            ),
            (
                RecordingExportFailureCategory::ReplayConsumerStalled,
                "recording export failed: replay_consumer_stalled",
            ),
            (
                RecordingExportFailureCategory::ReplayClientDisconnected,
                "recording export failed: replay_client_disconnected",
            ),
            (
                RecordingExportFailureCategory::ReplayCancelled,
                "recording export failed: replay_cancelled",
            ),
            (
                RecordingExportFailureCategory::H265Unsupported,
                "recording export failed: h265_unsupported",
            ),
            (
                RecordingExportFailureCategory::AacPacketInvalid,
                "recording export failed: aac_packet_invalid",
            ),
            (
                RecordingExportFailureCategory::H264Parse,
                "recording export failed: h264_parse",
            ),
            (
                RecordingExportFailureCategory::AacParse,
                "recording export failed: aac_parse",
            ),
            (
                RecordingExportFailureCategory::Mp4Mux,
                "recording export failed: mp4_mux",
            ),
            (
                RecordingExportFailureCategory::OutputDisconnected,
                "recording export failed: output_disconnected",
            ),
            (
                RecordingExportFailureCategory::OutputStalled,
                "recording export failed: output_stalled",
            ),
            (
                RecordingExportFailureCategory::OutputWrite,
                "recording export failed: output_write",
            ),
            (
                RecordingExportFailureCategory::FinalizationTimeout,
                "recording export failed: finalization_timeout",
            ),
            (
                RecordingExportFailureCategory::PipelineOther,
                "recording export failed: pipeline_other",
            ),
            (
                RecordingExportFailureCategory::CameraCleanupTimeout,
                "recording export failed: camera_cleanup_timeout",
            ),
            (
                RecordingExportFailureCategory::CameraCleanupFailed,
                "recording export failed: camera_cleanup_failed",
            ),
        ];
        assert_eq!(expected.len(), 30);
        for (category, expected_line) in expected {
            let line = category.failure().to_string();
            assert_eq!(line, expected_line);
            assert!(line.starts_with("recording export failed: "));
            assert!(!line.contains(private));
            assert!(!line.contains("uid"));
            assert!(!line.contains("password"));
        }
    }

    #[test]
    fn output_failure_precedes_every_pipeline_failure_and_pipeline_mapping_is_exact() {
        let pipelines = [
            None,
            Some(PipelineFailure::H264Parse),
            Some(PipelineFailure::AacParse),
            Some(PipelineFailure::Mp4Mux),
            Some(PipelineFailure::Output),
            Some(PipelineFailure::Timeout),
            Some(PipelineFailure::Other),
        ];
        for pipeline in pipelines {
            assert_eq!(
                classify_output_error(Some(OutputFailure::BrokenPipe), pipeline),
                RecordingExportFailureCategory::OutputDisconnected.failure()
            );
            assert_eq!(
                classify_output_error(Some(OutputFailure::ConsumerStalled), pipeline),
                RecordingExportFailureCategory::OutputStalled.failure()
            );
            assert_eq!(
                classify_output_error(Some(OutputFailure::Write), pipeline),
                RecordingExportFailureCategory::OutputWrite.failure()
            );
        }

        for (pipeline, expected) in [
            (
                Some(PipelineFailure::H264Parse),
                RecordingExportFailureCategory::H264Parse.failure(),
            ),
            (
                Some(PipelineFailure::AacParse),
                RecordingExportFailureCategory::AacParse.failure(),
            ),
            (
                Some(PipelineFailure::Mp4Mux),
                RecordingExportFailureCategory::Mp4Mux.failure(),
            ),
            (
                Some(PipelineFailure::Output),
                RecordingExportFailureCategory::OutputWrite.failure(),
            ),
            (
                Some(PipelineFailure::Timeout),
                RecordingExportFailureCategory::FinalizationTimeout.failure(),
            ),
            (
                Some(PipelineFailure::Other),
                RecordingExportFailureCategory::PipelineOther.failure(),
            ),
            (
                None,
                RecordingExportFailureCategory::PipelineOther.failure(),
            ),
        ] {
            assert_eq!(classify_output_error(None, pipeline), expected);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn video_only_mux_is_fragmented_and_starts_with_no_private_text() {
        let (mut replay, shutdowns) =
            FakeReplay::new(video_packets(), RecordingReplayEnd::CameraEnd);
        let output = SharedWriter::default();
        drive_and_shutdown(&mut replay, AudioMode::None, writer_box(output.clone()))
            .await
            .unwrap();
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
        let bytes = output
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        assert_fragmented_mp4(&bytes);
        assert_ffprobe_streams(&bytes, false);
        assert!(!bytes.windows(7).any(|window| window == b"private"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn duplicate_camera_timestamps_produce_nominal_twenty_five_fps_mp4_timing() {
        let mut packets = vec![iframe(1_000_000, VideoType::H264)];
        for _ in 1..25 {
            packets.push(pframe(1_000_000, VideoType::H264));
        }
        let (mut replay, _) = FakeReplay::new(packets, RecordingReplayEnd::CameraEnd);
        let output = SharedWriter::default();
        drive_and_shutdown(&mut replay, AudioMode::None, writer_box(output.clone()))
            .await
            .unwrap();
        let bytes = output
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        assert_fragmented_mp4(&bytes);
        let Some(document) = ffprobe_document(
            &bytes,
            "stream=codec_type,r_frame_rate,avg_frame_rate,time_base,duration",
        ) else {
            return;
        };
        let video = document["streams"]
            .as_array()
            .and_then(|streams| {
                streams
                    .iter()
                    .find(|stream| stream["codec_type"] == "video")
            })
            .expect("ffprobe video stream");
        assert_eq!(video["r_frame_rate"], "25/1");
        let average_rate = video["avg_frame_rate"]
            .as_str()
            .expect("ffprobe average video frame rate");
        assert_ne!(average_rate, "10000/1");
        let (numerator, denominator) = average_rate
            .split_once('/')
            .expect("rational ffprobe average video frame rate");
        let average_rate = numerator
            .parse::<f64>()
            .expect("numeric ffprobe average-rate numerator")
            / denominator
                .parse::<f64>()
                .expect("numeric ffprobe average-rate denominator");
        assert!(
            (24.0..=27.0).contains(&average_rate),
            "average frame rate was {average_rate}"
        );
        let duration = video["duration"]
            .as_str()
            .expect("ffprobe video duration")
            .parse::<f64>()
            .expect("numeric ffprobe video duration");
        let (time_base_numerator, time_base_denominator) = video["time_base"]
            .as_str()
            .expect("ffprobe video time base")
            .split_once('/')
            .expect("rational ffprobe video time base");
        let track_tick = time_base_numerator
            .parse::<f64>()
            .expect("numeric ffprobe time-base numerator")
            / time_base_denominator
                .parse::<f64>()
                .expect("numeric ffprobe time-base denominator");
        assert!(track_tick.is_finite() && track_tick > 0.0 && track_tick <= 0.040);
        // Twenty-five nominal 40 ms access units include the final sample's
        // duration, so the complete fragmented stream is exactly one second.
        assert!(
            (duration - 1.000).abs() <= track_tick,
            "duration was {duration} with track tick {track_tick}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn video_fragment_is_emitted_before_eos() {
        let output = SharedWriter::default();
        let mut muxer = Fmp4Muxer::new(false, 25, writer_box(output.clone())).unwrap();
        let mut clocks = MediaClocks::new(25);
        for media in video_packets() {
            push_media(&muxer, &mut clocks, AudioMode::None, media).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let has_fragment = output
                    .0
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .windows(4)
                    .any(|window| window == b"moof");
                if has_fragment {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fMP4 must emit a media fragment before EOS");
        muxer.finish().unwrap();
        assert_eq!(muxer.test_output_reserved(), (0, 0));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn aggregate_h264_access_units_keep_three_monotonic_nominally_timed_packets() {
        let mut aggregate = match iframe(0, VideoType::H264) {
            BcMedia::Iframe(frame) => frame,
            _ => unreachable!(),
        };
        let embedded = match pframe(0, VideoType::H264) {
            BcMedia::Pframe(frame) => frame,
            _ => unreachable!(),
        };
        aggregate.data.extend_from_slice(&embedded.data);

        let output = SharedWriter::default();
        let mut muxer = Fmp4Muxer::new(false, 25, writer_box(output.clone())).unwrap();
        let mut clocks = MediaClocks::new(25);
        push_media(
            &muxer,
            &mut clocks,
            AudioMode::None,
            BcMedia::Iframe(aggregate),
        )
        .unwrap();
        push_media(
            &muxer,
            &mut clocks,
            AudioMode::None,
            pframe(7_975_000, VideoType::H264),
        )
        .unwrap();

        let parser_caps = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let parser = muxer
                    .test_pipeline()
                    .iterate_elements()
                    .into_iter()
                    .flatten()
                    .find(|element| element.name().starts_with("h264parse"))
                    .expect("fixture pipeline has an H.264 parser");
                if let Some(caps) = parser.static_pad("src").and_then(|pad| pad.current_caps()) {
                    break caps;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("H.264 parser did not negotiate source caps");
        assert_eq!(
            parser_caps
                .structure(0)
                .expect("H.264 parser source caps structure")
                .get::<gstreamer::Fraction>("framerate")
                .unwrap(),
            gstreamer::Fraction::new(25, 1)
        );

        muxer.finish().unwrap();
        let bytes = output
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let Some(document) = ffprobe_document(
            &bytes,
            "packet=pts_time,dts_time,duration_time,flags:stream=codec_type,codec_name,r_frame_rate,avg_frame_rate,time_base,duration",
        )
        else {
            return;
        };
        let parse_rational = |value: &str| {
            let (numerator, denominator) = value.split_once('/').expect("ffprobe rational");
            let numerator = numerator.parse::<f64>().expect("rational numerator");
            let denominator = denominator.parse::<f64>().expect("rational denominator");
            assert!(denominator > 0.0);
            numerator / denominator
        };
        let streams = document["streams"].as_array().expect("ffprobe streams");
        let video = streams
            .iter()
            .find(|stream| stream["codec_type"] == "video")
            .expect("ffprobe H.264 video stream");
        assert_eq!(video["codec_name"], "h264");
        let time_base = parse_rational(
            video["time_base"]
                .as_str()
                .expect("ffprobe video time base"),
        );
        assert!(
            time_base > 0.0 && time_base <= 0.040,
            "video time base was {time_base}"
        );
        let packets = document["packets"].as_array().expect("ffprobe packets");
        assert_eq!(packets.len(), 3, "aggregate input must produce three AUs");
        assert!(packets[0]["flags"]
            .as_str()
            .expect("first packet flags")
            .contains('K'));
        assert!(packets[1..].iter().all(|packet| !packet["flags"]
            .as_str()
            .expect("delta packet flags")
            .contains('K')));
        let parse_time = |packet: &serde_json::Value, field: &str| {
            packet[field]
                .as_str()
                .unwrap_or_else(|| panic!("ffprobe packet missing {field}"))
                .parse::<f64>()
                .unwrap_or_else(|_| panic!("ffprobe packet has invalid {field}"))
        };
        let pts = packets
            .iter()
            .map(|packet| parse_time(packet, "pts_time"))
            .collect::<Vec<_>>();
        let dts = packets
            .iter()
            .map(|packet| parse_time(packet, "dts_time"))
            .collect::<Vec<_>>();
        let durations = packets.iter().filter_map(|packet| {
            packet["duration_time"]
                .as_str()
                .map(|duration| duration.parse::<f64>().expect("packet duration"))
        });
        assert!(pts.windows(2).all(|pair| pair[1] > pair[0]));
        assert!(dts.windows(2).all(|pair| pair[1] > pair[0]));
        let first_delta = pts[1] - pts[0];
        assert!(
            (first_delta - 0.040).abs() <= time_base,
            "first split-AU delta was {first_delta}"
        );
        assert!(
            (pts[2] - 7.975).abs() <= time_base,
            "sparse third packet PTS was {}",
            pts[2]
        );
        assert!(
            pts.windows(2)
                .map(|pair| pair[1] - pair[0])
                .all(|delta| delta >= (1.0 / 120.0)),
            "positive packet delta fell below the supported 120 fps floor"
        );
        assert!(durations.into_iter().all(|duration| duration > 0.0));

        let real_rate = video["r_frame_rate"]
            .as_str()
            .expect("ffprobe real frame rate");
        assert_ne!(real_rate, "10000/1");
        let real_rate = parse_rational(real_rate);
        assert!(
            real_rate.is_finite() && real_rate > 0.0 && real_rate <= 120.0,
            "real frame rate was {real_rate}"
        );
        assert_eq!(
            video["avg_frame_rate"]
                .as_str()
                .expect("ffprobe average frame rate"),
            "25/1"
        );
        let stream_duration = video["duration"]
            .as_str()
            .expect("ffprobe video duration")
            .parse::<f64>()
            .expect("numeric ffprobe video duration");
        assert!(
            (stream_duration - (pts[2] + 0.040)).abs() <= time_base,
            "sparse video duration was {stream_duration}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn h264_aac_mux_has_video_and_audio_tracks() {
        let mut packets = video_packets();
        packets.insert(2, aac());
        for _ in 0..8 {
            packets.push(aac());
        }
        let (mut replay, shutdowns) = FakeReplay::new(packets, RecordingReplayEnd::CameraEnd);
        let output = SharedWriter::default();
        drive_and_shutdown(&mut replay, AudioMode::Required, writer_box(output.clone()))
            .await
            .unwrap();
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
        let bytes = output
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        assert_fragmented_mp4(&bytes);
        assert!(bytes.windows(4).any(|window| window == b"vide"));
        assert!(bytes.windows(4).any(|window| window == b"soun"));
        assert!(bytes.windows(4).any(|window| window == b"mp4a"));
        assert_ffprobe_streams(&bytes, true);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn late_codec_failures_use_static_post_preflight_categories() {
        let output = SharedWriter::default();
        let muxer = Fmp4Muxer::new(false, 25, writer_box(output)).unwrap();
        let mut clocks = MediaClocks::new(25);
        push_media(
            &muxer,
            &mut clocks,
            AudioMode::None,
            iframe(0, VideoType::H264),
        )
        .unwrap();
        assert_eq!(
            push_media(
                &muxer,
                &mut clocks,
                AudioMode::None,
                pframe(40_000, VideoType::H265),
            )
            .unwrap_err(),
            RecordingExportFailureCategory::H265Unsupported.failure()
        );

        let output = SharedWriter::default();
        let muxer = Fmp4Muxer::new(true, 25, writer_box(output)).unwrap();
        let mut clocks = MediaClocks::new(25);
        assert_eq!(
            push_media(
                &muxer,
                &mut clocks,
                AudioMode::Required,
                BcMedia::Aac(BcMediaAac { data: vec![0; 8] }),
            )
            .unwrap_err(),
            RecordingExportFailureCategory::AacPacketInvalid.failure()
        );
    }

    #[test]
    fn executable_reporter_preserves_exact_audio_fallback_contract() {
        let mut stderr = Vec::new();
        let code = report_result(Err(AUDIO_UNAVAILABLE_MISSING), &mut stderr);
        assert_ne!(code, ExitCode::SUCCESS);
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            "RECORDING_EXPORT_AUDIO_UNAVAILABLE: AAC was required but was not present in the recording\n"
        );

        let mut success_stderr = Vec::new();
        assert_eq!(
            report_result(Ok(()), &mut success_stderr),
            ExitCode::SUCCESS
        );
        assert!(success_stderr.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn broken_stdout_is_bounded_and_still_requests_stop_once() {
        let (mut replay, shutdowns) =
            FakeReplay::new(video_packets(), RecordingReplayEnd::Cancelled);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            drive_and_shutdown(&mut replay, AudioMode::None, writer_box(BrokenWriter)),
        )
        .await
        .expect("broken stdout handling must be bounded")
        .unwrap_err();
        assert_eq!(
            result,
            RecordingExportFailureCategory::OutputDisconnected.failure()
        );
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn connected_nonreading_stdout_stalls_boundedly_and_stops_once() {
        let (reader, writer) = UnixStream::pair().unwrap();
        let writer = OutputTarget::from_fd(OwnedFd::from(writer)).unwrap();
        let mut packets = video_packets();
        for index in 0..500 {
            packets.push(pframe(2_000_000 + index * 40_000, VideoType::H264));
        }
        let (mut replay, shutdowns) = FakeReplay::new(packets, RecordingReplayEnd::CameraEnd);
        let error = tokio::time::timeout(
            Duration::from_secs(8),
            drive_and_shutdown(&mut replay, AudioMode::None, writer),
        )
        .await
        .expect("a connected nonreading output must not block cleanup")
        .unwrap_err();
        assert_eq!(
            error,
            RecordingExportFailureCategory::OutputStalled.failure()
        );
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
        drop(reader);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stop_failure_is_propagated_once_without_private_data() {
        let (replay, shutdowns) = FakeReplay::new(video_packets(), RecordingReplayEnd::CameraEnd);
        let mut replay = replay.fail_shutdown();
        let output = SharedWriter::default();
        let error = drive_and_shutdown(&mut replay, AudioMode::None, writer_box(output))
            .await
            .unwrap_err();
        assert_eq!(error, RecordingExportFailureCategory::ReplayStop.failure());
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
        assert!(!error.to_string().contains("private"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_stream_failure_after_preflight_precedes_stop_failure() {
        let (replay, shutdowns) = FakeReplay::new(video_packets(), RecordingReplayEnd::CameraEnd);
        let mut replay = replay
            .fail_next_after_packets(RecordingExportFailureCategory::ReplayStream.failure())
            .fail_shutdown();
        let output = SharedWriter::default();
        let error = drive_and_shutdown(&mut replay, AudioMode::None, writer_box(output.clone()))
            .await
            .unwrap_err();
        assert_eq!(
            error,
            RecordingExportFailureCategory::ReplayStream.failure()
        );
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
        let bytes = output
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_fragmented_mp4(&bytes);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writer_is_flushed_after_success_and_after_export_error() {
        let success_writer = FlushCountingWriter::default();
        let (mut success, _) = FakeReplay::new(video_packets(), RecordingReplayEnd::CameraEnd);
        drive_and_shutdown(
            &mut success,
            AudioMode::None,
            writer_box(success_writer.clone()),
        )
        .await
        .unwrap();
        assert_eq!(success_writer.flushes.load(Ordering::SeqCst), 1);
        let success_bytes = success_writer
            .bytes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        assert_fragmented_mp4(&success_bytes);

        let error_writer = FlushCountingWriter::default();
        let packets = vec![iframe(0, VideoType::H265)];
        let (mut failed, _) = FakeReplay::new(packets, RecordingReplayEnd::Cancelled);
        assert!(drive_and_shutdown(
            &mut failed,
            AudioMode::None,
            writer_box(error_writer.clone()),
        )
        .await
        .is_err());
        assert_eq!(error_writer.flushes.load(Ordering::SeqCst), 1);
        assert!(error_writer
            .bytes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_muxer_forces_pipeline_to_null() {
        let output = SharedWriter::default();
        let muxer = Fmp4Muxer::new(false, 25, writer_box(output)).unwrap();
        let pipeline = muxer.test_pipeline();
        drop(muxer);
        assert_eq!(pipeline.current_state(), gstreamer::State::Null);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_limit_endings_are_explicit_failures_after_clean_mux_eos() {
        for (end, expected) in [
            (
                RecordingReplayEnd::DurationLimit,
                RecordingExportFailureCategory::ReplayDurationLimit.failure(),
            ),
            (
                RecordingReplayEnd::ByteLimit,
                RecordingExportFailureCategory::ReplayByteLimit.failure(),
            ),
            (
                RecordingReplayEnd::BufferLimit,
                RecordingExportFailureCategory::ReplayBufferLimit.failure(),
            ),
            (
                RecordingReplayEnd::ConsumerStalled,
                RecordingExportFailureCategory::ReplayConsumerStalled.failure(),
            ),
            (
                RecordingReplayEnd::ClientDisconnected,
                RecordingExportFailureCategory::ReplayClientDisconnected.failure(),
            ),
            (
                RecordingReplayEnd::Cancelled,
                RecordingExportFailureCategory::ReplayCancelled.failure(),
            ),
        ] {
            let (mut replay, shutdowns) = FakeReplay::new(video_packets(), end);
            let output = SharedWriter::default();
            let error =
                drive_and_shutdown(&mut replay, AudioMode::None, writer_box(output.clone()))
                    .await
                    .unwrap_err();
            assert_eq!(error, expected);
            assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
            let bytes = output
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            assert_fragmented_mp4(&bytes);
        }
    }
}
