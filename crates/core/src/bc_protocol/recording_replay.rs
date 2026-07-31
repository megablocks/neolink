use super::{
    recordings::{resolve_recording_uid, RECORDING_UID_DISCOVERY_TIMEOUT},
    BcCamera, BcConnection, Error, RecordingEntry, RecordingStreamKind, Result,
};
use crate::{
    bc::{model::*, xml::*},
    bcmedia::de::MAX_MEDIA_PAYLOAD,
    bcmedia::model::BcMedia,
};
use futures::StreamExt;
use lazy_static::lazy_static;
use regex::Regex;
use std::{
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, OwnedMutexGuard},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

const FILE_INFO_LIST_VERSION: &str = "1.1";
const REPLAY_MESSAGE_CLASS: u16 = 0x6414;
const REPLAY_MESSAGE_NUMBER: u16 = 0;
const DEFAULT_REPLAY_START_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_REPLAY_SEND_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_REPLAY_STOP_TIMEOUT: Duration = Duration::from_secs(2);
const REPLAY_SOCKET_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const HARD_REPLAY_PHASE_TIMEOUT: Duration = Duration::from_secs(30);
// Replay is a loss-intolerant byte stream. This is 25.6% of the generic
// 500-message queue and additionally carries a strict compressed-byte budget.
// Either limit is observable: overflow terminates the producer and drives
// bounded STOP/connection teardown instead of silently dropping media bytes.
const REPLAY_RAW_SUBSCRIPTION_CAPACITY: usize = 128;
const REPLAY_RAW_MAX_BUFFERED_BYTES: usize = 64 * 1024 * 1024;
const HARD_REPLAY_BUFFER_SIZE: usize = 32;

/// Default maximum wall time for one stored-recording replay session.
pub const DEFAULT_RECORDING_REPLAY_MAX_DURATION: Duration = Duration::from_secs(15 * 60);
/// Hard maximum wall time accepted for one stored-recording replay session.
pub const HARD_RECORDING_REPLAY_MAX_DURATION: Duration = Duration::from_secs(2 * 60 * 60);
/// Default maximum compressed media bytes for one stored-recording replay session.
pub const DEFAULT_RECORDING_REPLAY_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Hard maximum compressed media bytes accepted for one replay session.
pub const HARD_RECORDING_REPLAY_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Default number of decoded BcMedia packets buffered for a replay consumer.
pub const DEFAULT_RECORDING_REPLAY_BUFFER_SIZE: usize = 8;
/// Parser ceiling for one decoded compressed media payload.
pub const RECORDING_REPLAY_MAX_DECODED_PACKET_BYTES: u64 = MAX_MEDIA_PAYLOAD as u64;
/// Default maximum compressed payload bytes retained in the decoded replay queue.
pub const DEFAULT_RECORDING_REPLAY_MAX_BUFFERED_MEDIA_BYTES: u64 = 32 * 1024 * 1024;
/// Hard maximum compressed payload bytes retained in the decoded replay queue.
pub const HARD_RECORDING_REPLAY_MAX_BUFFERED_MEDIA_BYTES: u64 = 64 * 1024 * 1024;

lazy_static! {
    static ref EXISTING_STOP_TOKEN: Regex =
        Regex::new(r"(?:^|[^0-9])(01[0-9]{14})(?:[^0-9]|$)").expect("valid stop-token regex");
    static ref RECORDING_START: Regex =
        Regex::new(r"Rec[[:alnum:]_]{3}(?:_|_DST)([0-9]{8})_([0-9]{6})_")
            .expect("valid recording-name regex");
}

/// Safety limits and stream selection for one stored-recording replay.
#[derive(Clone, Debug)]
pub struct RecordingReplayOptions {
    /// Logical camera channel.
    pub channel: u8,
    /// Main/high-quality or sub/fluent recording stream.
    pub stream: RecordingStreamKind,
    /// Maximum wall time before Neolink sends STOP.
    pub max_duration: Duration,
    /// Maximum compressed BcMedia payload bytes delivered to the caller.
    pub max_media_bytes: u64,
    /// Maximum compressed payload bytes retained in the decoded consumer queue.
    ///
    /// Reservations are released before [`RecordingReplay::get_data`] returns a
    /// packet. At steady state the queue retains at most this many payload bytes;
    /// the single producer may additionally own one decoded packet of at most
    /// [`RECORDING_REPLAY_MAX_DECODED_PACKET_BYTES`] while deciding whether it
    /// fits. The raw `Bc` subscription is independently capped at 128 envelopes
    /// and 64 MiB of conservatively accounted envelope bytes, and treats either
    /// overflow as terminal rather than silently dropping replay data. The BC
    /// parser preserves the validated wire body length for XML, extension,
    /// mixed, binary, and header-only envelopes. Only envelopes manually
    /// constructed without that parser sidecar use the full 16 MiB conservative
    /// fallback.
    pub max_buffered_media_bytes: u64,
    /// Maximum decoded BcMedia packets queued for a slow consumer.
    pub buffer_size: usize,
    /// Reject malformed BcMedia instead of attempting resynchronization.
    pub strict: bool,
    /// Maximum time to wait for the command-5 start response.
    pub start_timeout: Duration,
    /// Maximum time to enqueue a START or STOP command.
    pub send_timeout: Duration,
    /// Maximum time to wait for the command-7 STOP response.
    pub stop_timeout: Duration,
}

impl Default for RecordingReplayOptions {
    fn default() -> Self {
        Self {
            channel: 0,
            stream: RecordingStreamKind::Sub,
            max_duration: DEFAULT_RECORDING_REPLAY_MAX_DURATION,
            max_media_bytes: DEFAULT_RECORDING_REPLAY_MAX_BYTES,
            max_buffered_media_bytes: DEFAULT_RECORDING_REPLAY_MAX_BUFFERED_MEDIA_BYTES,
            buffer_size: DEFAULT_RECORDING_REPLAY_BUFFER_SIZE,
            strict: false,
            start_timeout: DEFAULT_REPLAY_START_TIMEOUT,
            send_timeout: DEFAULT_REPLAY_SEND_TIMEOUT,
            stop_timeout: DEFAULT_REPLAY_STOP_TIMEOUT,
        }
    }
}

impl RecordingReplayOptions {
    fn validate(&self) -> Result<()> {
        if self.channel > 31 {
            return Err(Error::Other(
                "Recording replay channel must be between 0 and 31",
            ));
        }
        if self.max_duration.is_zero() || self.max_duration > HARD_RECORDING_REPLAY_MAX_DURATION {
            return Err(Error::Other(
                "Recording replay duration is outside its safety ceiling",
            ));
        }
        if !(1..=HARD_RECORDING_REPLAY_MAX_BYTES).contains(&self.max_media_bytes) {
            return Err(Error::Other(
                "Recording replay byte limit is outside its safety ceiling",
            ));
        }
        if !(1..=HARD_RECORDING_REPLAY_MAX_BUFFERED_MEDIA_BYTES)
            .contains(&self.max_buffered_media_bytes)
        {
            return Err(Error::Other(
                "Recording replay buffered-byte limit is outside its safety ceiling",
            ));
        }
        if !(1..=HARD_REPLAY_BUFFER_SIZE).contains(&self.buffer_size) {
            return Err(Error::Other(
                "Recording replay buffer is outside its safety ceiling",
            ));
        }
        for timeout in [self.start_timeout, self.send_timeout, self.stop_timeout] {
            if timeout.is_zero() || timeout > HARD_REPLAY_PHASE_TIMEOUT {
                return Err(Error::Other(
                    "Recording replay timeout is outside its safety ceiling",
                ));
            }
        }
        Ok(())
    }
}

/// Why a recording replay stopped without a protocol or media error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordingReplayEnd {
    /// The camera ended its pushed BcMedia stream.
    CameraEnd,
    /// The configured wall-time limit was reached.
    DurationLimit,
    /// Delivering another media packet would exceed the configured byte limit.
    ByteLimit,
    /// One decoded packet could not fit in the configured queued-byte budget.
    BufferLimit,
    /// The consumer did not drain the bounded packet queue before another packet arrived.
    ConsumerStalled,
    /// The caller explicitly shut down or dropped the replay handle.
    Cancelled,
    /// The replay consumer disconnected while a packet was being delivered.
    ClientDisconnected,
}

#[derive(Debug)]
struct MediaByteBudget {
    limit: u64,
    reserved: AtomicU64,
    #[cfg(test)]
    high_water: AtomicU64,
}

impl MediaByteBudget {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            reserved: AtomicU64::new(0),
            #[cfg(test)]
            high_water: AtomicU64::new(0),
        }
    }

    fn try_reserve(self: &Arc<Self>, bytes: u64) -> Option<MediaByteReservation> {
        let mut current = self.reserved.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes)?;
            if next > self.limit {
                return None;
            }
            match self.reserved.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    #[cfg(test)]
                    self.high_water.fetch_max(next, Ordering::AcqRel);
                    return Some(MediaByteReservation {
                        budget: self.clone(),
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn reserved(&self) -> u64 {
        self.reserved.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn high_water(&self) -> u64 {
        self.high_water.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct MediaByteReservation {
    budget: Arc<MediaByteBudget>,
    bytes: u64,
}

impl Drop for MediaByteReservation {
    fn drop(&mut self) {
        let previous = self.budget.reserved.fetch_sub(self.bytes, Ordering::AcqRel);
        debug_assert!(previous >= self.bytes);
    }
}

#[derive(Debug)]
struct QueuedMedia {
    packet: Option<Result<BcMedia>>,
    _reservation: Option<MediaByteReservation>,
}

impl QueuedMedia {
    fn media(packet: BcMedia, reservation: MediaByteReservation) -> Self {
        Self {
            packet: Some(Ok(packet)),
            _reservation: Some(reservation),
        }
    }

    fn error(error: Error) -> Self {
        Self {
            packet: Some(Err(error)),
            _reservation: None,
        }
    }

    fn into_packet(mut self) -> Result<BcMedia> {
        self.packet
            .take()
            .expect("queued media packet is consumed exactly once")
    }
}

/// Handle for a bounded stored-recording BcMedia replay.
///
/// Call [`Self::get_data`] to consume decoded BcMedia packets. Calling
/// [`Self::shutdown`], or dropping the handle, signals the background task to
/// send command 7 exactly once. A STOP failure forces the underlying camera
/// connection closed so an orphaned replay cannot continue indefinitely.
///
/// The exact internal decoded-payload residency bound is
/// `max_buffered_media_bytes + RECORDING_REPLAY_MAX_DECODED_PACKET_BYTES`:
/// queued packets retain byte reservations, and the single producer can own at
/// most one not-yet-queued decoded packet. Dequeued caller-owned packets are no
/// longer charged to the internal queue.
///
/// Before decoding, a separate raw queue is capped at both 128 envelopes and 64
/// MiB of conservatively accounted envelope bytes. The connection ingress queue
/// and the poller command currently being routed share an independent 64 MiB
/// byte budget, so its nominal 1000-envelope count cannot retain unbounded raw
/// bodies.
///
/// In addition to the three queue budgets, conservatively allow: 64 MiB for the
/// shared BC transport `Framed` read buffer, including its current body and all
/// retained read-ahead; 16 MiB for the transient decrypt/`to_vec` buffer (the
/// current decrypt implementation allocates even for unencrypted input); 16 MiB
/// for the parsed envelope owned by the source while it waits for ingress
/// permits; 16 MiB for one raw envelope already dequeued into the async-reader
/// adapter after its raw-queue reservation is released during that ownership
/// transfer; 32 MiB for one BcMedia frame being buffered (independently capped
/// 16 MiB additional header plus 16 MiB payload); and 16 MiB for one decoded
/// producer packet before it can obtain a decoded-queue reservation. Retained
/// ingress/raw reservation ownership transitions are not counted again.
///
/// For the locked `tokio-util` 0.7.18 state machine, `reserve(1)` exposes the
/// entire remaining `BytesMut` chunk to one `poll_read_buf` before decoding.
/// Consuming frames advances the view without shrinking its retained backing
/// allocation. With Rust 1.88's amortized doubling and `bytes` 1.11.1's
/// in-place-reclaim rule (`offset >= len`), a 32 MiB backing buffer can grow to
/// 64 MiB: after earlier frames consume just under 16 MiB, an incomplete maximum
/// body plus fixed header can leave `len` just over 16 MiB, so `offset < len`
/// prevents reclaim and `reserve(1)` doubles the allocation.
///
/// That 64 MiB step is final. Whenever a 64 MiB view needs more input, any
/// incomplete valid BC suffix is smaller than a 16 MiB body plus its fixed
/// header, and therefore smaller than half the backing buffer. The view either
/// already has tail capacity or its consumed offset is greater than its length,
/// allowing in-place reclaim instead of growth to 128 MiB. The decoder's error
/// path likewise advances to the next magic or drops all but a three-byte
/// boundary tail before requesting more input.
///
/// The resulting conservative protocol-owned logical-payload ceiling is:
/// 64 MiB ingress + 64 MiB raw queue + 32 MiB default decoded queue + 64 MiB BC
/// transport + 64 MiB in the four 16 MiB transient/owned objects above + 32 MiB
/// BcMedia parser = 320 MiB by default. Replacing the default decoded queue with
/// its 64 MiB hard ceiling gives 352 MiB. These figures exclude fixed metadata,
/// allocator spare capacity, and packets intentionally retained by the caller
/// after dequeue.
/// The parser-validated wire body length is carried through ingress and raw
/// routing for XML, extension, mixed, binary, and header-only envelopes; only
/// manually constructed envelopes without that sidecar use the full body
/// ceiling. Any ingress, raw, or decoded bound exhaustion applies backpressure
/// or terminates observably rather than silently losing replay data.
pub struct RecordingReplay {
    channel: u8,
    handle: Option<JoinHandle<Result<RecordingReplayEnd>>>,
    receiver: mpsc::Receiver<QueuedMedia>,
    byte_budget: Arc<MediaByteBudget>,
    cancel: CancellationToken,
    completion: Option<Result<RecordingReplayEnd>>,
}

impl fmt::Debug for RecordingReplay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordingReplay")
            .field("channel", &self.channel)
            .field("buffered_media_bytes", &self.byte_budget.reserved())
            .field("finished", &self.completion.is_some())
            .finish_non_exhaustive()
    }
}

impl RecordingReplay {
    /// Logical camera channel selected for this replay.
    pub fn channel(&self) -> u8 {
        self.channel
    }

    /// Receive the next decoded BcMedia packet.
    ///
    /// The outer error means the replay has no more packets. An inner error is
    /// a protocol/media failure that was also retained for [`Self::shutdown`].
    pub async fn get_data(&mut self) -> Result<Result<BcMedia>> {
        match self.receiver.recv().await {
            // `into_packet` drops the byte reservation before ownership of the
            // BcMedia is returned to the caller. The configured bound therefore
            // covers only Neolink's internal queue, not data deliberately held
            // by application code after dequeue.
            Some(packet) => Ok(packet.into_packet()),
            None => Err(Error::StreamFinished),
        }
    }

    /// Compressed payload bytes currently retained in Neolink's replay queue.
    pub fn buffered_media_bytes(&self) -> u64 {
        self.byte_budget.reserved()
    }

    /// Idempotently stop replay and await bounded protocol teardown.
    pub async fn shutdown(&mut self) -> Result<RecordingReplayEnd> {
        if let Some(completion) = &self.completion {
            return completion.clone();
        }

        self.cancel.cancel();
        let completion = match self.handle.take() {
            Some(handle) => match handle.await {
                Ok(result) => result,
                Err(error) => Err(error.into()),
            },
            None => Err(Error::StreamFinished),
        };
        self.completion = Some(completion.clone());
        completion
    }
}

impl Drop for RecordingReplay {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = handle.await;
                });
            }
        }
    }
}

struct StartCancellationGuard {
    cancel: CancellationToken,
    armed: bool,
}

impl Drop for StartCancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cancel.cancel();
        }
    }
}

/// Derive the camera's mandatory replay STOP token without retaining a path.
///
/// Reolink recording names normally contain `Rec..._YYYYMMDD_HHMMSS_`; the
/// corresponding STOP token is `01YYYYMMDDHHMMSS`. Existing stop tokens are
/// accepted unchanged.
pub fn recording_replay_stop_token(identifier: &str) -> Option<String> {
    let identifier = identifier.trim();
    if let Some(captures) = EXISTING_STOP_TOKEN.captures(identifier) {
        return captures.get(1).map(|value| value.as_str().to_owned());
    }
    let captures = RECORDING_START.captures(identifier)?;
    Some(format!("01{}{}", &captures[1], &captures[2]))
}

fn recording_identifier(entry: &RecordingEntry) -> Option<&str> {
    entry
        .id
        .as_deref()
        .or(entry.file_name.as_deref())
        .or(entry.name.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn stream_parameters(stream: RecordingStreamKind) -> (&'static str, u8) {
    match stream {
        RecordingStreamKind::Main => ("mainStream", 0),
        RecordingStreamKind::Sub => ("subStream", 1),
    }
}

fn media_payload_len(media: &BcMedia) -> u64 {
    match media {
        BcMedia::InfoV1(_) | BcMedia::InfoV2(_) => 0,
        BcMedia::Iframe(frame) => frame.data.len() as u64,
        BcMedia::Pframe(frame) => frame.data.len() as u64,
        BcMedia::Aac(frame) => frame.data.len() as u64,
        BcMedia::Adpcm(frame) => frame.data.len() as u64,
    }
}

fn start_request(
    identifier: String,
    uid: String,
    stream_name: &str,
    support_sub: u8,
    session_counter: u8,
) -> Bc {
    Bc::new_from_xml(
        BcMeta {
            msg_id: MSG_ID_FILE_INFO_LIST_REPLAY,
            channel_id: session_counter,
            msg_num: REPLAY_MESSAGE_NUMBER,
            response_code: 0,
            stream_type: 0,
            class: REPLAY_MESSAGE_CLASS,
        },
        BcXml {
            file_info_list: Some(FileInfoList {
                version: Some(FILE_INFO_LIST_VERSION.to_owned()),
                file_info: vec![FileInfo {
                    // Standalone cameras use XML channel zero; the opaque ID
                    // selects the physical recording/lens.
                    channel_id: Some(0),
                    id: Some(identifier),
                    uid: Some(uid),
                    support_sub: Some(support_sub),
                    play_speed: Some(1),
                    stream_type: Some(stream_name.to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        },
    )
}

fn stop_request(channel: u8, stop_name: String, stream_name: &str, msg_num: u16) -> Bc {
    Bc::new_from_xml(
        BcMeta {
            msg_id: MSG_ID_FILE_INFO_LIST_STOP,
            channel_id: channel + 1,
            msg_num,
            response_code: 0,
            stream_type: 0,
            class: REPLAY_MESSAGE_CLASS,
        },
        BcXml {
            file_info_list: Some(FileInfoList {
                version: Some(FILE_INFO_LIST_VERSION.to_owned()),
                file_info: vec![FileInfo {
                    channel_id: Some(channel),
                    name: Some(stop_name),
                    stream_type: Some(stream_name.to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        },
    )
}

async fn stop_replay(
    connection: &Arc<BcConnection>,
    channel: u8,
    stop_name: String,
    stream_name: &str,
    msg_num: u16,
    options: &RecordingReplayOptions,
) -> Result<()> {
    let operation = async {
        let mut subscription = tokio::time::timeout(
            options.send_timeout,
            connection.subscribe(MSG_ID_FILE_INFO_LIST_STOP, msg_num),
        )
        .await
        .map_err(|_| Error::TimeoutDisconnected)??;
        tokio::time::timeout(
            options.send_timeout,
            subscription.send(stop_request(channel, stop_name, stream_name, msg_num)),
        )
        .await
        .map_err(|_| Error::TimeoutDisconnected)??;
        let reply = tokio::time::timeout(options.stop_timeout, subscription.recv())
            .await
            .map_err(|_| Error::TimeoutDisconnected)??;
        if reply.meta.response_code != 200 {
            return Err(Error::CameraServiceUnavailable {
                id: MSG_ID_FILE_INFO_LIST_STOP,
                code: reply.meta.response_code,
            });
        }
        Ok(())
    };

    match operation.await {
        Ok(()) => Ok(()),
        Err(stop) => {
            let _ =
                tokio::time::timeout(REPLAY_SOCKET_SHUTDOWN_TIMEOUT, connection.shutdown()).await;
            Err(stop)
        }
    }
}

fn combine_replay_and_stop(
    replay: Result<RecordingReplayEnd>,
    stop: Result<()>,
) -> Result<RecordingReplayEnd> {
    match (replay, stop) {
        (Ok(end), Ok(())) => Ok(end),
        (Ok(_), Err(stop)) => Err(Error::RecordingReplayStopFailed {
            stop: Arc::new(stop),
        }),
        (Err(replay), Ok(())) => Err(replay),
        (Err(replay), Err(stop)) => Err(Error::RecordingReplayAndStopFailed {
            replay: Arc::new(replay),
            stop: Arc::new(stop),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_replay_session(
    connection: Arc<BcConnection>,
    _session_guard: OwnedMutexGuard<()>,
    request: Bc,
    channel: u8,
    stop_name: String,
    stream_name: &'static str,
    stop_msg_num: u16,
    options: RecordingReplayOptions,
    cancel: CancellationToken,
    mut started: Option<oneshot::Sender<Result<()>>>,
    media_tx: mpsc::Sender<QueuedMedia>,
    byte_budget: Arc<MediaByteBudget>,
) -> Result<RecordingReplayEnd> {
    let mut start_may_have_been_sent = false;
    let replay_result = async {
        let (mut subscription, raw_overflow) = tokio::select! {
            _ = cancel.cancelled() => return Ok(RecordingReplayEnd::Cancelled),
            result = tokio::time::timeout(
                options.send_timeout,
                connection.subscribe_bounded(
                    MSG_ID_FILE_INFO_LIST_REPLAY,
                    REPLAY_MESSAGE_NUMBER,
                    REPLAY_RAW_SUBSCRIPTION_CAPACITY,
                    REPLAY_RAW_MAX_BUFFERED_BYTES,
                ),
            ) => result.map_err(|_| Error::TimeoutDisconnected)??,
        };

        start_may_have_been_sent = true;
        tokio::select! {
            _ = cancel.cancelled() => return Ok(RecordingReplayEnd::Cancelled),
            result = tokio::time::timeout(options.send_timeout, subscription.send(request)) => {
                result.map_err(|_| Error::TimeoutDisconnected)??;
            }
        }

        let reply = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(RecordingReplayEnd::Cancelled),
            result = tokio::time::timeout(options.start_timeout, subscription.recv()) => {
                result.map_err(|_| Error::TimeoutDisconnected)??
            }
            _ = raw_overflow.cancelled() => return Ok(RecordingReplayEnd::ConsumerStalled),
        };
        if reply.meta.response_code != 200 {
            return Err(Error::CameraServiceUnavailable {
                id: MSG_ID_FILE_INFO_LIST_REPLAY,
                code: reply.meta.response_code,
            });
        }

        if started
            .take()
            .is_some_and(|started| started.send(Ok(())).is_err())
        {
            return Ok(RecordingReplayEnd::ClientDisconnected);
        }

        let mut delivered_bytes = 0u64;
        let deadline = tokio::time::sleep(options.max_duration);
        tokio::pin!(deadline);
        let mut media = subscription.bcmedia_stream(options.strict);

        loop {
            let packet = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(RecordingReplayEnd::Cancelled),
                _ = &mut deadline => return Ok(RecordingReplayEnd::DurationLimit),
                _ = raw_overflow.cancelled() => return Ok(RecordingReplayEnd::ConsumerStalled),
                packet = media.next() => packet,
            };
            let packet = match packet {
                Some(packet) => packet?,
                None => return Ok(RecordingReplayEnd::CameraEnd),
            };
            let packet_bytes = media_payload_len(&packet);
            let Some(next_bytes) = delivered_bytes.checked_add(packet_bytes) else {
                return Ok(RecordingReplayEnd::ByteLimit);
            };
            if next_bytes > options.max_media_bytes {
                return Ok(RecordingReplayEnd::ByteLimit);
            }

            let Some(reservation) = byte_budget.try_reserve(packet_bytes) else {
                return Ok(RecordingReplayEnd::BufferLimit);
            };
            match media_tx.try_send(QueuedMedia::media(packet, reservation)) {
                Ok(()) => {
                    delivered_bytes = next_bytes;
                    // A single raw BC envelope can contain many decoded media
                    // packets. Yield after each successful enqueue so a ready
                    // consumer can drain between packets instead of being
                    // declared stalled solely because this producer kept the
                    // executor for an entire burst.
                    tokio::task::yield_now().await;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    return Ok(RecordingReplayEnd::ConsumerStalled);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Ok(RecordingReplayEnd::ClientDisconnected);
                }
            }
        }
    }
    .await;

    let stop_result = if start_may_have_been_sent {
        stop_replay(
            &connection,
            channel,
            stop_name,
            stream_name,
            stop_msg_num,
            &options,
        )
        .await
    } else {
        Ok(())
    };
    let final_result = combine_replay_and_stop(replay_result, stop_result);

    if let Some(started) = started {
        let notification = match &final_result {
            Ok(_) => Err(Error::Other(
                "Recording replay ended before start completed",
            )),
            Err(error) => Err(error.clone()),
        };
        let _ = started.send(notification);
    } else if let Err(error) = &final_result {
        // Teardown must not wait on a stalled/full consumer queue. The error
        // remains available from `shutdown()` even when this notification is
        // deliberately dropped.
        let _ = media_tx.try_send(QueuedMedia::error(error.clone()));
    }

    final_result
}

impl BcCamera {
    /// Start a bounded stored-recording replay as pushed BcMedia packets.
    ///
    /// The recording entry comes from [`Self::search_recordings`]. Replay is
    /// serialized per camera connection because command 5 uses message number
    /// zero. The returned handle owns cancellation/STOP teardown.
    ///
    /// The `BcCamera` connection **must be dedicated to this replay**. Raw
    /// subscription overflow and an unacknowledged STOP deliberately fail closed
    /// and can shut down the connection; sharing it with live view or control
    /// traffic would therefore couple unrelated operations to replay teardown.
    /// A non-empty UID configured on the camera is reused directly. Otherwise
    /// the UID is queried for [`RecordingReplayOptions::channel`] with a fixed
    /// timeout before the replay lock is acquired.
    pub async fn start_recording_replay(
        &self,
        entry: &RecordingEntry,
        options: RecordingReplayOptions,
    ) -> Result<RecordingReplay> {
        self.start_recording_replay_with_uid_timeout(
            entry,
            options,
            RECORDING_UID_DISCOVERY_TIMEOUT,
        )
        .await
    }

    async fn start_recording_replay_with_uid_timeout(
        &self,
        entry: &RecordingEntry,
        options: RecordingReplayOptions,
        uid_discovery_timeout: Duration,
    ) -> Result<RecordingReplay> {
        options.validate()?;
        let identifier = recording_identifier(entry)
            .ok_or(Error::Other("Recording replay entry has no identifier"))?;
        let stop_name = recording_replay_stop_token(identifier).ok_or(Error::Other(
            "Recording replay identifier cannot produce a safe STOP token",
        ))?;
        let uid = resolve_recording_uid(
            self.configured_uid.as_deref(),
            options.channel,
            uid_discovery_timeout,
            |channel| self.uid_for_channel(channel),
        )
        .await?;
        let session_guard = tokio::time::timeout(
            options.start_timeout,
            self.recording_replay_lock.clone().lock_owned(),
        )
        .await
        .map_err(|_| Error::TimeoutDisconnected)?;
        let (stream_name, support_sub) = stream_parameters(options.stream);
        let channel = options.channel;
        let session_counter = self.new_message_num().to_le_bytes()[0].max(1);
        let stop_msg_num = self.new_message_num();
        let request = start_request(
            identifier.to_owned(),
            uid,
            stream_name,
            support_sub,
            session_counter,
        );
        let connection = self.get_connection();
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (media_tx, receiver) = mpsc::channel(options.buffer_size);
        let byte_budget = Arc::new(MediaByteBudget::new(options.max_buffered_media_bytes));
        let task_byte_budget = byte_budget.clone();
        let task_options = options.clone();
        let task_connection = connection.clone();
        let mut handle = tokio::spawn(async move {
            run_replay_session(
                task_connection,
                session_guard,
                request,
                channel,
                stop_name,
                stream_name,
                stop_msg_num,
                task_options,
                task_cancel,
                Some(started_tx),
                media_tx,
                task_byte_budget,
            )
            .await
        });
        let mut cancellation_guard = StartCancellationGuard {
            cancel: cancel.clone(),
            armed: true,
        };

        let start_budget = options
            .start_timeout
            .saturating_add(options.send_timeout.saturating_mul(2))
            .saturating_add(options.stop_timeout)
            .saturating_add(REPLAY_SOCKET_SHUTDOWN_TIMEOUT);
        let started = tokio::time::timeout(start_budget, started_rx).await;
        match started {
            Ok(Ok(Ok(()))) => {
                cancellation_guard.armed = false;
                Ok(RecordingReplay {
                    channel: options.channel,
                    handle: Some(handle),
                    receiver,
                    byte_budget,
                    cancel,
                    completion: None,
                })
            }
            Ok(Ok(Err(error))) => {
                cancel.cancel();
                let _ = handle.await;
                Err(error)
            }
            Ok(Err(_)) => {
                cancel.cancel();
                match handle.await {
                    Ok(Err(error)) => Err(error),
                    Ok(Ok(_)) => Err(Error::Other(
                        "Recording replay ended before start notification",
                    )),
                    Err(error) => Err(error.into()),
                }
            }
            Err(_) => {
                cancel.cancel();
                if tokio::time::timeout(
                    options
                        .stop_timeout
                        .saturating_add(REPLAY_SOCKET_SHUTDOWN_TIMEOUT),
                    &mut handle,
                )
                .await
                .is_err()
                {
                    handle.abort();
                    let _ =
                        tokio::time::timeout(REPLAY_SOCKET_SHUTDOWN_TIMEOUT, connection.shutdown())
                            .await;
                }
                Err(Error::TimeoutDisconnected)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bc::codex::DecodedBc,
        bc_protocol::{BcConnSink, BcConnSource, Credentials},
        bcmedia::model::{BcMediaIframe, VideoType},
    };
    use futures::{
        channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        SinkExt,
    };
    use std::{
        collections::{HashMap, HashSet},
        sync::atomic::{AtomicBool, AtomicU16},
    };
    use tokio::{
        sync::{Mutex, RwLock},
        time::timeout,
    };

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    #[derive(Clone)]
    struct TestInbound(UnboundedSender<Result<DecodedBc>>);

    impl TestInbound {
        fn unbounded_send(&self, value: Result<Bc>) -> std::result::Result<(), ()> {
            self.0
                .unbounded_send(value.map(|message| {
                    let wire_body_len = match &message.body {
                        BcBody::ModernMsg(ModernMsg {
                            extension: None,
                            payload: Some(BcPayloads::Binary(data)),
                        }) => data.len() as u32,
                        _ => 0,
                    };
                    DecodedBc {
                        message,
                        wire_body_len,
                    }
                }))
                .map_err(|_| ())
        }
    }

    #[derive(Clone, Copy)]
    enum StartBehavior {
        Acknowledge,
        Reject(u16),
        Ignore,
    }

    #[derive(Clone, Copy)]
    enum StopBehavior {
        Acknowledge,
        Reject(u16),
        Ignore,
    }

    async fn test_camera_with_uid(
        channel: u8,
        configured_uid: Option<String>,
    ) -> (Arc<BcCamera>, UnboundedReceiver<Bc>, TestInbound) {
        let (outbound_tx, outbound_rx) = unbounded::<Bc>();
        let sink: BcConnSink = Box::new(outbound_tx.sink_map_err(|_| Error::DroppedConnection));
        let (inbound_tx, inbound_rx) = unbounded::<Result<DecodedBc>>();
        let source: BcConnSource = Box::new(inbound_rx);
        let connection = BcConnection::new(sink, source).await.unwrap();
        let camera = BcCamera {
            channel_id: channel,
            configured_uid,
            connection: Arc::new(connection),
            logged_in: AtomicBool::new(true),
            message_num: AtomicU16::new(0),
            credentials: Credentials {
                username: "PRIVATE_FIXTURE_USERNAME".to_owned(),
                password: Some("PRIVATE_FIXTURE_PASSWORD".to_owned()),
            },
            abilities: RwLock::new(HashMap::new()),
            unsupported: RwLock::new(HashSet::new()),
            recording_search_lock: Mutex::new(()),
            recording_replay_lock: Default::default(),
        };
        (Arc::new(camera), outbound_rx, TestInbound(inbound_tx))
    }

    async fn test_camera(channel: u8) -> (Arc<BcCamera>, UnboundedReceiver<Bc>, TestInbound) {
        test_camera_with_uid(channel, Some("  PRIVATE_FIXTURE_UID  ".to_owned())).await
    }

    fn options(channel: u8) -> RecordingReplayOptions {
        RecordingReplayOptions {
            channel,
            max_duration: Duration::from_millis(25),
            max_media_bytes: 1024,
            buffer_size: 4,
            start_timeout: Duration::from_millis(40),
            send_timeout: Duration::from_millis(40),
            stop_timeout: Duration::from_millis(40),
            strict: true,
            ..Default::default()
        }
    }

    fn entry() -> RecordingEntry {
        RecordingEntry {
            id: Some("/fixture/RecM01_20260730_010203_PRIVATE.mp4".to_owned()),
            name: None,
            file_name: None,
            record_type: None,
            size_bytes: None,
            start: None,
            end: None,
        }
    }

    fn modern_reply(request: &Bc, response_code: u16, payload: Option<BcPayloads>) -> Bc {
        Bc {
            meta: BcMeta {
                response_code,
                ..request.meta
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: None,
                payload,
            }),
        }
    }

    fn binary_message(start: &Bc, bytes: Vec<u8>) -> Bc {
        modern_reply(start, 200, Some(BcPayloads::Binary(bytes)))
    }

    fn uid_reply(request: &Bc, response_code: u16, uid: Option<&str>) -> Bc {
        modern_reply(
            request,
            response_code,
            uid.map(|uid| {
                BcPayloads::BcXml(BcXml {
                    uid: Some(Uid {
                        version: FILE_INFO_LIST_VERSION.to_owned(),
                        uid: uid.to_owned(),
                    }),
                    ..Default::default()
                })
            }),
        )
    }

    fn iframe(bytes: usize) -> Vec<u8> {
        BcMedia::Iframe(BcMediaIframe {
            video_type: VideoType::H264,
            microseconds: 0,
            time: Some(1),
            data: vec![0x55; bytes],
        })
        .serialize(Vec::new())
        .unwrap()
    }

    fn file_info(message: &Bc) -> &FileInfo {
        match &message.body {
            BcBody::ModernMsg(ModernMsg {
                payload:
                    Some(BcPayloads::BcXml(BcXml {
                        file_info_list: Some(list),
                        ..
                    })),
                ..
            }) => &list.file_info[0],
            _ => panic!("expected FileInfoList XML"),
        }
    }

    async fn drive_device(
        mut outbound: UnboundedReceiver<Bc>,
        inbound: TestInbound,
        channel: u8,
        stream: RecordingStreamKind,
        start_behavior: StartBehavior,
        media: Vec<Vec<u8>>,
        stop_behavior: StopBehavior,
    ) -> usize {
        let start = tokio::time::timeout(TEST_TIMEOUT, outbound.next())
            .await
            .expect("START command timeout")
            .expect("START command");
        assert_eq!(start.meta.msg_id, MSG_ID_FILE_INFO_LIST_REPLAY);
        assert_eq!(start.meta.msg_num, REPLAY_MESSAGE_NUMBER);
        assert_eq!(start.meta.class, REPLAY_MESSAGE_CLASS);
        assert_ne!(start.meta.channel_id, 0);
        let start_info = file_info(&start);
        assert_eq!(start_info.channel_id, Some(0));
        assert_eq!(start_info.uid.as_deref(), Some("PRIVATE_FIXTURE_UID"));
        assert_eq!(start_info.play_speed, Some(1));
        let (stream_name, support_sub) = stream_parameters(stream);
        assert_eq!(start_info.stream_type.as_deref(), Some(stream_name));
        assert_eq!(start_info.support_sub, Some(support_sub));

        match start_behavior {
            StartBehavior::Acknowledge => {
                inbound
                    .unbounded_send(Ok(modern_reply(&start, 200, None)))
                    .unwrap();
                for bytes in media {
                    inbound
                        .unbounded_send(Ok(binary_message(&start, bytes)))
                        .unwrap();
                }
            }
            StartBehavior::Reject(code) => inbound
                .unbounded_send(Ok(modern_reply(&start, code, None)))
                .unwrap(),
            StartBehavior::Ignore => {}
        }

        let stop = tokio::time::timeout(TEST_TIMEOUT, outbound.next())
            .await
            .expect("STOP command timeout")
            .expect("STOP command");
        assert_eq!(stop.meta.msg_id, MSG_ID_FILE_INFO_LIST_STOP);
        assert_eq!(stop.meta.channel_id, channel + 1);
        assert_eq!(stop.meta.class, REPLAY_MESSAGE_CLASS);
        let stop_info = file_info(&stop);
        assert_eq!(stop_info.channel_id, Some(channel));
        assert_eq!(stop_info.name.as_deref(), Some("0120260730010203"));
        assert_eq!(stop_info.stream_type.as_deref(), Some(stream_name));

        match stop_behavior {
            StopBehavior::Acknowledge => inbound
                .unbounded_send(Ok(modern_reply(&stop, 200, None)))
                .unwrap(),
            StopBehavior::Reject(code) => inbound
                .unbounded_send(Ok(modern_reply(&stop, code, None)))
                .unwrap(),
            StopBehavior::Ignore => tokio::time::sleep(Duration::from_millis(100)).await,
        }
        1
    }

    #[test]
    fn stop_token_accepts_supported_names_without_exposing_them_in_errors() {
        assert_eq!(
            recording_replay_stop_token("/fixture/RecM01_20260730_010203_PRIVATE.mp4").as_deref(),
            Some("0120260730010203")
        );
        assert_eq!(
            recording_replay_stop_token("prefix-0120260730010203-suffix").as_deref(),
            Some("0120260730010203")
        );
        assert!(recording_replay_stop_token("PRIVATE_UNSUPPORTED_NAME").is_none());
    }

    #[test]
    fn options_enforce_every_hard_safety_ceiling() {
        assert!(RecordingReplayOptions::default().validate().is_ok());
        let value = RecordingReplayOptions {
            channel: 32,
            ..Default::default()
        };
        assert!(value.validate().is_err());
        let value = RecordingReplayOptions {
            max_duration: Duration::ZERO,
            ..Default::default()
        };
        assert!(value.validate().is_err());
        let value = RecordingReplayOptions {
            max_duration: HARD_RECORDING_REPLAY_MAX_DURATION + Duration::from_nanos(1),
            ..Default::default()
        };
        assert!(value.validate().is_err());
        let value = RecordingReplayOptions {
            max_media_bytes: HARD_RECORDING_REPLAY_MAX_BYTES + 1,
            ..Default::default()
        };
        assert!(value.validate().is_err());
        let value = RecordingReplayOptions {
            max_buffered_media_bytes: HARD_RECORDING_REPLAY_MAX_BUFFERED_MEDIA_BYTES + 1,
            ..Default::default()
        };
        assert!(value.validate().is_err());
        let value = RecordingReplayOptions {
            buffer_size: HARD_REPLAY_BUFFER_SIZE + 1,
            ..Default::default()
        };
        assert!(value.validate().is_err());
        let value = RecordingReplayOptions {
            stop_timeout: HARD_REPLAY_PHASE_TIMEOUT + Duration::from_nanos(1),
            ..Default::default()
        };
        assert!(value.validate().is_err());
    }

    #[tokio::test]
    async fn configured_uid_is_trimmed_and_skips_discovery() {
        let (camera, outbound, inbound) = test_camera(0).await;
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            0,
            RecordingStreamKind::Sub,
            StartBehavior::Acknowledge,
            Vec::new(),
            StopBehavior::Acknowledge,
        ));

        // `drive_device` requires the first outbound command to be START (not
        // UID discovery) and verifies that its XML contains the trimmed UID.
        let mut replay = camera
            .start_recording_replay(&entry(), options(0))
            .await
            .unwrap();
        assert_eq!(
            replay.shutdown().await.unwrap(),
            RecordingReplayEnd::Cancelled
        );
        assert_eq!(device.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn absent_uid_resolves_requested_channel_and_reaches_start() {
        let channel = 1;
        let (camera, mut outbound, inbound) = test_camera_with_uid(0, None).await;
        let device = tokio::spawn(async move {
            let uid_request = timeout(TEST_TIMEOUT, outbound.next())
                .await
                .expect("UID request timeout")
                .expect("UID request");
            assert_eq!(uid_request.meta.msg_id, MSG_ID_UID);
            assert_eq!(uid_request.meta.channel_id, channel);
            inbound
                .unbounded_send(Ok(uid_reply(
                    &uid_request,
                    200,
                    Some("  PRIVATE_FIXTURE_UID  "),
                )))
                .unwrap();
            drive_device(
                outbound,
                inbound,
                channel,
                RecordingStreamKind::Sub,
                StartBehavior::Acknowledge,
                Vec::new(),
                StopBehavior::Acknowledge,
            )
            .await
        });

        let mut replay = camera
            .start_recording_replay(&entry(), options(channel))
            .await
            .unwrap();
        assert_eq!(replay.channel(), channel);
        assert_eq!(
            replay.shutdown().await.unwrap(),
            RecordingReplayEnd::Cancelled
        );
        assert_eq!(device.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn hanging_uid_discovery_times_out_before_replay_lock() {
        let channel = 1;
        let (camera, mut outbound, _inbound) = test_camera_with_uid(0, None).await;
        let replay_guard = camera.recording_replay_lock.clone().lock_owned().await;
        let mut replay_options = options(channel);
        replay_options.start_timeout = Duration::from_secs(1);

        let result = timeout(
            Duration::from_millis(100),
            camera.start_recording_replay_with_uid_timeout(
                &entry(),
                replay_options,
                Duration::from_millis(10),
            ),
        )
        .await
        .expect("UID timeout must happen without waiting for the held replay lock");
        assert!(matches!(result, Err(Error::TimeoutDisconnected)));

        let uid_request = outbound.next().await.expect("UID request");
        assert_eq!(uid_request.meta.msg_id, MSG_ID_UID);
        assert_eq!(uid_request.meta.channel_id, channel);
        assert!(
            timeout(Duration::from_millis(20), outbound.next())
                .await
                .is_err(),
            "timed-out discovery must not send START"
        );
        drop(replay_guard);
    }

    #[tokio::test]
    async fn uid_discovery_preserves_errors_and_rejects_empty_uid_before_start() {
        let (camera, mut outbound, inbound) = test_camera_with_uid(0, None).await;
        let start_camera = camera.clone();
        let start = tokio::spawn(async move {
            start_camera
                .start_recording_replay(&entry(), options(1))
                .await
        });
        let request = outbound.next().await.expect("UID request");
        assert_eq!(request.meta.msg_id, MSG_ID_UID);
        inbound
            .unbounded_send(Ok(uid_reply(&request, 500, None)))
            .unwrap();
        assert!(matches!(
            start.await.unwrap(),
            Err(Error::CameraServiceUnavailable {
                id: MSG_ID_UID,
                code: 500
            })
        ));
        assert!(timeout(Duration::from_millis(20), outbound.next())
            .await
            .is_err());

        let (camera, mut outbound, inbound) = test_camera_with_uid(0, None).await;
        let start_camera = camera.clone();
        let start = tokio::spawn(async move {
            start_camera
                .start_recording_replay(&entry(), options(1))
                .await
        });
        let request = outbound.next().await.expect("UID request");
        assert_eq!(request.meta.msg_id, MSG_ID_UID);
        inbound
            .unbounded_send(Ok(uid_reply(&request, 200, Some("   "))))
            .unwrap();
        assert!(matches!(
            start.await.unwrap(),
            Err(Error::Other("Camera returned an empty UID"))
        ));
        assert!(
            timeout(Duration::from_millis(20), outbound.next())
                .await
                .is_err(),
            "empty discovered UID must not send START"
        );
    }

    #[tokio::test]
    async fn channels_zero_and_one_build_distinct_stop_requests() {
        for channel in [0, 1] {
            let (camera, outbound, inbound) = test_camera(channel).await;
            let device = tokio::spawn(drive_device(
                outbound,
                inbound,
                channel,
                RecordingStreamKind::Sub,
                StartBehavior::Acknowledge,
                Vec::new(),
                StopBehavior::Acknowledge,
            ));
            let mut replay = camera
                .start_recording_replay(&entry(), options(channel))
                .await
                .unwrap();
            assert_eq!(replay.channel(), channel);
            assert!(matches!(
                replay.get_data().await,
                Err(Error::StreamFinished)
            ));
            assert_eq!(
                replay.shutdown().await.unwrap(),
                RecordingReplayEnd::DurationLimit
            );
            assert_eq!(device.await.unwrap(), 1);
        }
    }

    #[tokio::test]
    async fn byte_cap_drops_the_over_limit_packet_and_stops_once() {
        let (camera, outbound, inbound) = test_camera(0).await;
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            0,
            RecordingStreamKind::Sub,
            StartBehavior::Acknowledge,
            vec![iframe(3), iframe(4)],
            StopBehavior::Acknowledge,
        ));
        let mut replay_options = options(0);
        replay_options.max_media_bytes = 5;
        let mut replay = camera
            .start_recording_replay(&entry(), replay_options)
            .await
            .unwrap();
        let packet = replay.get_data().await.unwrap().unwrap();
        assert!(matches!(packet, BcMedia::Iframe(_)));
        assert_eq!(
            replay.buffered_media_bytes(),
            0,
            "dequeue must release the internal byte reservation even while the caller owns media"
        );
        assert!(matches!(
            replay.get_data().await,
            Err(Error::StreamFinished)
        ));
        assert_eq!(
            replay.shutdown().await.unwrap(),
            RecordingReplayEnd::ByteLimit
        );
        assert_eq!(device.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn near_max_packets_stay_within_byte_budget_and_stop_without_consumption() {
        let (camera, mut outbound, inbound) = test_camera(0).await;
        let packet_bytes = MAX_MEDIA_PAYLOAD as usize - 64;
        let byte_budget = packet_bytes as u64 + 1024;
        let mut replay_options = options(0);
        replay_options.max_duration = Duration::from_secs(1);
        replay_options.max_media_bytes = (packet_bytes as u64) * 3;
        replay_options.max_buffered_media_bytes = byte_budget;
        replay_options.buffer_size = 4;

        let start_camera = camera.clone();
        let start = tokio::spawn(async move {
            start_camera
                .start_recording_replay(&entry(), replay_options)
                .await
        });
        let request = timeout(TEST_TIMEOUT, outbound.next())
            .await
            .expect("START command timeout")
            .expect("START command");
        inbound
            .unbounded_send(Ok(modern_reply(&request, 200, None)))
            .unwrap();
        let mut replay = start.await.unwrap().unwrap();
        let observed_budget = replay.byte_budget.clone();

        inbound
            .unbounded_send(Ok(binary_message(&request, iframe(packet_bytes))))
            .unwrap();
        timeout(TEST_TIMEOUT, async {
            while replay.buffered_media_bytes() != packet_bytes as u64 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first near-maximum packet was not queued");

        // No consumer drain: decoding one more near-16 MiB packet must fail the
        // byte reservation immediately, STOP, and never grow the queued payload
        // beyond the configured limit. The producer owns at most that one
        // rejected decoded packet, whose parser ceiling is exported above.
        inbound
            .unbounded_send(Ok(binary_message(&request, iframe(packet_bytes))))
            .unwrap();
        let stop = timeout(TEST_TIMEOUT, outbound.next())
            .await
            .expect("byte-budget exhaustion must issue STOP")
            .expect("STOP command");
        assert_eq!(stop.meta.msg_id, MSG_ID_FILE_INFO_LIST_STOP);
        inbound
            .unbounded_send(Ok(modern_reply(&stop, 200, None)))
            .unwrap();

        assert_eq!(
            timeout(TEST_TIMEOUT, replay.shutdown())
                .await
                .expect("byte-budget teardown must be bounded")
                .unwrap(),
            RecordingReplayEnd::BufferLimit
        );
        assert_eq!(observed_budget.reserved(), packet_bytes as u64);
        assert!(observed_budget.high_water() <= byte_budget);
        assert!(
            observed_budget.high_water() + packet_bytes as u64
                <= byte_budget + RECORDING_REPLAY_MAX_DECODED_PACKET_BYTES,
            "queued payload plus the one in-flight decoded packet exceeded the documented bound"
        );

        drop(replay);
        assert_eq!(
            observed_budget.reserved(),
            0,
            "dropping an undrained queue must release every reservation"
        );
    }

    #[tokio::test]
    async fn malformed_media_reports_error_after_acknowledged_stop() {
        let (camera, outbound, inbound) = test_camera(0).await;
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            0,
            RecordingStreamKind::Sub,
            StartBehavior::Acknowledge,
            vec![vec![0xff; 32]],
            StopBehavior::Acknowledge,
        ));
        let mut replay = camera
            .start_recording_replay(&entry(), options(0))
            .await
            .unwrap();
        assert!(matches!(
            replay.get_data().await,
            Ok(Err(Error::NomError(_)))
        ));
        assert!(matches!(replay.shutdown().await, Err(Error::NomError(_))));
        assert_eq!(device.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn full_consumer_buffer_fails_fast_and_stops() {
        let (camera, outbound, inbound) = test_camera(0).await;
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            0,
            RecordingStreamKind::Sub,
            StartBehavior::Acknowledge,
            vec![iframe(3), iframe(4)],
            StopBehavior::Acknowledge,
        ));
        let mut replay_options = options(0);
        replay_options.buffer_size = 1;
        let mut replay = camera
            .start_recording_replay(&entry(), replay_options)
            .await
            .unwrap();
        assert_eq!(device.await.unwrap(), 1);
        assert_eq!(
            timeout(TEST_TIMEOUT, replay.shutdown())
                .await
                .expect("consumer-stall teardown must be bounded")
                .unwrap(),
            RecordingReplayEnd::ConsumerStalled
        );
    }

    #[tokio::test]
    async fn active_consumer_drains_one_envelope_burst_beyond_queue_capacity() {
        let (camera, outbound, inbound) = test_camera(0).await;
        let mut burst = Vec::new();
        for _ in 0..20 {
            burst.extend_from_slice(&iframe(3));
        }
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            0,
            RecordingStreamKind::Sub,
            StartBehavior::Acknowledge,
            vec![burst],
            StopBehavior::Acknowledge,
        ));
        let mut replay_options = options(0);
        replay_options.buffer_size = 1;
        replay_options.max_media_bytes = 1024 * 1024;
        let mut replay = camera
            .start_recording_replay(&entry(), replay_options)
            .await
            .unwrap();

        let mut packets = 0;
        loop {
            match replay.get_data().await {
                Ok(Ok(_)) => packets += 1,
                Err(Error::StreamFinished) => break,
                other => panic!("unexpected replay result: {other:?}"),
            }
        }
        assert_eq!(packets, 20);
        assert_eq!(
            replay.shutdown().await.unwrap(),
            RecordingReplayEnd::DurationLimit
        );
        assert_eq!(device.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn stop_rejection_is_reported_and_forces_socket_shutdown() {
        let (camera, outbound, inbound) = test_camera(0).await;
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            0,
            RecordingStreamKind::Sub,
            StartBehavior::Acknowledge,
            Vec::new(),
            StopBehavior::Reject(500),
        ));
        let mut replay = camera
            .start_recording_replay(&entry(), options(0))
            .await
            .unwrap();
        assert!(matches!(
            replay.get_data().await,
            Ok(Err(Error::RecordingReplayStopFailed { .. }))
        ));
        assert!(matches!(
            replay.shutdown().await,
            Err(Error::RecordingReplayStopFailed { .. })
        ));
        assert_eq!(device.await.unwrap(), 1);
        let _ = tokio::time::timeout(TEST_TIMEOUT, camera.join())
            .await
            .expect("failed STOP must close connection")
            .ok();
    }

    #[tokio::test]
    async fn stop_timeout_is_bounded_and_reported() {
        let (camera, outbound, inbound) = test_camera(0).await;
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            0,
            RecordingStreamKind::Sub,
            StartBehavior::Acknowledge,
            Vec::new(),
            StopBehavior::Ignore,
        ));
        let mut replay = camera
            .start_recording_replay(&entry(), options(0))
            .await
            .unwrap();
        assert!(matches!(
            replay.get_data().await,
            Ok(Err(Error::RecordingReplayStopFailed { .. }))
        ));
        assert!(matches!(
            replay.shutdown().await,
            Err(Error::RecordingReplayStopFailed { .. })
        ));
        assert_eq!(device.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn start_timeout_still_sends_stop() {
        let (camera, outbound, inbound) = test_camera(0).await;
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            0,
            RecordingStreamKind::Sub,
            StartBehavior::Ignore,
            Vec::new(),
            StopBehavior::Acknowledge,
        ));
        assert!(matches!(
            camera.start_recording_replay(&entry(), options(0)).await,
            Err(Error::TimeoutDisconnected)
        ));
        assert_eq!(device.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn rejected_start_is_preserved_after_stop() {
        let (camera, outbound, inbound) = test_camera(1).await;
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            1,
            RecordingStreamKind::Main,
            StartBehavior::Reject(400),
            Vec::new(),
            StopBehavior::Acknowledge,
        ));
        let mut replay_options = options(1);
        replay_options.stream = RecordingStreamKind::Main;
        assert!(matches!(
            camera
                .start_recording_replay(&entry(), replay_options)
                .await,
            Err(Error::CameraServiceUnavailable {
                id: MSG_ID_FILE_INFO_LIST_REPLAY,
                code: 400
            })
        ));
        assert_eq!(device.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn explicit_shutdown_is_idempotent_and_sends_one_stop() {
        let (camera, outbound, inbound) = test_camera(0).await;
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            0,
            RecordingStreamKind::Sub,
            StartBehavior::Acknowledge,
            Vec::new(),
            StopBehavior::Acknowledge,
        ));
        let mut replay = camera
            .start_recording_replay(&entry(), options(0))
            .await
            .unwrap();
        assert_eq!(
            replay.shutdown().await.unwrap(),
            RecordingReplayEnd::Cancelled
        );
        assert_eq!(
            replay.shutdown().await.unwrap(),
            RecordingReplayEnd::Cancelled
        );
        assert_eq!(device.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn dropping_handle_sends_stop_without_waiting_for_duration_cap() {
        let (camera, outbound, inbound) = test_camera(0).await;
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            0,
            RecordingStreamKind::Sub,
            StartBehavior::Acknowledge,
            Vec::new(),
            StopBehavior::Acknowledge,
        ));
        let mut replay_options = options(0);
        replay_options.max_duration = Duration::from_secs(1);
        let replay = camera
            .start_recording_replay(&entry(), replay_options)
            .await
            .unwrap();
        drop(replay);
        assert_eq!(device.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn cancelling_start_future_sends_stop() {
        let (camera, outbound, inbound) = test_camera(0).await;
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            0,
            RecordingStreamKind::Sub,
            StartBehavior::Ignore,
            Vec::new(),
            StopBehavior::Acknowledge,
        ));
        let start_camera = camera.clone();
        let mut replay_options = options(0);
        replay_options.start_timeout = Duration::from_secs(1);
        let start = tokio::spawn(async move {
            start_camera
                .start_recording_replay(&entry(), replay_options)
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        start.abort();
        assert!(start.await.unwrap_err().is_cancelled());
        assert_eq!(device.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn diagnostics_do_not_include_uid_credentials_or_recording_id() {
        let (camera, outbound, inbound) = test_camera(0).await;
        let device = tokio::spawn(drive_device(
            outbound,
            inbound,
            0,
            RecordingStreamKind::Sub,
            StartBehavior::Acknowledge,
            Vec::new(),
            StopBehavior::Acknowledge,
        ));
        let mut replay = camera
            .start_recording_replay(&entry(), options(0))
            .await
            .unwrap();
        let diagnostics = format!("{replay:?}");
        assert!(!diagnostics.contains("PRIVATE"));
        assert!(!diagnostics.contains("RecM01"));
        let _ = replay.shutdown().await;
        assert_eq!(device.await.unwrap(), 1);
    }
}
