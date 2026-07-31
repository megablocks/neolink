use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[cfg(test)]
use std::io::Write;

#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{anyhow, Context, Result};
use gstreamer::{
    glib::error::ErrorDomain, glib::translate::IntoGlib, prelude::*, Buffer, BufferFlags,
    BufferRef, ClockTime, MessageType, MessageView, PadProbeData, PadProbeReturn, PadProbeType,
    Pipeline, State,
};
use gstreamer_app::{AppSink, AppSinkCallbacks, AppSrc, AppStreamType};
use tokio_util::sync::CancellationToken;

const APP_SRC_MAX_BYTES: u64 = 16 * 1024 * 1024;
const EOS_TIMEOUT: ClockTime = ClockTime::from_seconds(15);
// Pump-owned output memory is capped by all three limits simultaneously: at
// most 256 samples across the writer's in-flight item plus its queue, at most
// 16 MiB per sample, and at most 32 MiB total across in-flight plus queued
// samples. The higher count absorbs mp4mux's bursts of tiny atom buffers without
// increasing the byte ceiling. appsink separately holds one upstream sample.
const OUTPUT_QUEUE_SAMPLES: usize = 256;
const OUTPUT_MAX_SAMPLE_BYTES: usize = 16 * 1024 * 1024;
const OUTPUT_MAX_QUEUED_BYTES: usize = 32 * 1024 * 1024;
const OUTPUT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const OUTPUT_STALL_TIMEOUT: Duration = Duration::from_secs(2);
const OUTPUT_JOIN_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OutputFailure {
    BrokenPipe,
    ConsumerStalled,
    Write,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PipelineFailure {
    H264Parse,
    AacParse,
    Mp4Mux,
    Output,
    Timeout,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SafeBusSource {
    H264Parse,
    AacParse,
    Mp4Mux,
    Output,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SafeBusDomain {
    Core,
    Library,
    Resource,
    Stream,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SafeBusError {
    source: SafeBusSource,
    domain: SafeBusDomain,
    code: i32,
}

impl SafeBusError {
    fn from_message(error: &gstreamer::message::Error) -> Self {
        let source = match error.src().map(|source| source.name()) {
            Some(name) if name.starts_with("h264parse") => SafeBusSource::H264Parse,
            Some(name) if name.starts_with("aacparse") => SafeBusSource::AacParse,
            Some(name) if name == "mux" => SafeBusSource::Mp4Mux,
            Some(name) if name == "outsink" => SafeBusSource::Output,
            _ => SafeBusSource::Other,
        };
        let error = error.error();
        let domain = if error.domain() == gstreamer::CoreError::domain() {
            SafeBusDomain::Core
        } else if error.domain() == gstreamer::LibraryError::domain() {
            SafeBusDomain::Library
        } else if error.domain() == gstreamer::ResourceError::domain() {
            SafeBusDomain::Resource
        } else if error.domain() == gstreamer::StreamError::domain() {
            SafeBusDomain::Stream
        } else {
            SafeBusDomain::Other
        };
        Self {
            source,
            domain,
            code: error.code(),
        }
    }

    fn failure(self) -> PipelineFailure {
        match self.source {
            SafeBusSource::H264Parse => PipelineFailure::H264Parse,
            SafeBusSource::AacParse => PipelineFailure::AacParse,
            SafeBusSource::Mp4Mux => PipelineFailure::Mp4Mux,
            SafeBusSource::Output => PipelineFailure::Output,
            SafeBusSource::Other => PipelineFailure::Other,
        }
    }
}

#[derive(Default)]
struct SinkStatus {
    failure: Mutex<Option<OutputFailure>>,
}

/// A production output target backed by a nonblocking duplicate of stdout.
pub(super) struct OutputTarget {
    #[cfg(unix)]
    fd: Option<OwnedFd>,
    #[cfg(test)]
    writer: Option<Box<dyn Write + Send>>,
}

impl OutputTarget {
    #[cfg(unix)]
    pub(super) fn stdout() -> Result<Self> {
        let stdout = std::io::stdout();
        // SAFETY: fcntl receives a valid borrowed stdout fd and returns a new
        // close-on-exec descriptor on success. Ownership transfers exactly
        // once to OwnedFd below.
        let duplicated = unsafe { libc::fcntl(stdout.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if duplicated < 0 {
            return Err(std::io::Error::last_os_error()).context("unable to duplicate stdout");
        }
        // SAFETY: duplicated was returned by F_DUPFD_CLOEXEC and has not been
        // wrapped or closed yet.
        let fd = unsafe { OwnedFd::from_raw_fd(duplicated) };
        // SAFETY: F_GETFL/F_SETFL operate on the valid owned descriptor and do
        // not retain pointers. O_NONBLOCK ensures pipe backpressure is bounded.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
        if flags < 0
            || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
        {
            return Err(std::io::Error::last_os_error())
                .context("unable to make stdout nonblocking");
        }
        Ok(Self {
            fd: Some(fd),
            #[cfg(test)]
            writer: None,
        })
    }

    #[cfg(not(unix))]
    pub(super) fn stdout() -> Result<Self> {
        Err(anyhow!(
            "recording export requires nonblocking stdout support"
        ))
    }

    #[cfg(all(test, unix))]
    pub(super) fn from_fd(fd: OwnedFd) -> Result<Self> {
        // SAFETY: F_GETFL/F_SETFL operate on the valid owned descriptor and do
        // not retain pointers.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
        if flags < 0
            || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
        {
            return Err(std::io::Error::last_os_error())
                .context("unable to make fixture output nonblocking");
        }
        Ok(Self {
            fd: Some(fd),
            writer: None,
        })
    }

    #[cfg(test)]
    pub(super) fn test_writer(writer: Box<dyn Write + Send>) -> Self {
        Self {
            #[cfg(unix)]
            fd: None,
            writer: Some(writer),
        }
    }

    fn write_all_bounded(
        &mut self,
        bytes: &[u8],
        cancelled: &CancellationToken,
    ) -> std::result::Result<(), OutputFailure> {
        #[cfg(test)]
        if let Some(writer) = self.writer.as_mut() {
            return writer.write_all(bytes).map_err(classify_io_error);
        }
        #[cfg(unix)]
        if let Some(fd) = self.fd.as_ref() {
            return write_nonblocking(fd, bytes, cancelled);
        }
        Err(OutputFailure::Write)
    }

    fn flush(&mut self) -> std::result::Result<(), OutputFailure> {
        #[cfg(test)]
        if let Some(writer) = self.writer.as_mut() {
            return writer.flush().map_err(classify_io_error);
        }
        // A raw pipe/socket/file descriptor has no userspace buffer here.
        Ok(())
    }

    pub(super) fn flush_without_output(&mut self) {
        let _ = self.flush();
    }
}

#[derive(Debug)]
enum OutputMessage {
    Bytes(Vec<u8>),
    Finish,
}

#[derive(Default)]
struct OutputBudget {
    bytes: AtomicUsize,
    samples: AtomicUsize,
}

impl OutputBudget {
    fn reserve(&self, amount: usize) -> bool {
        if amount > OUTPUT_MAX_SAMPLE_BYTES {
            return false;
        }
        if self
            .samples
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < OUTPUT_QUEUE_SAMPLES).then_some(current + 1)
            })
            .is_err()
        {
            return false;
        }
        let mut current = self.bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(amount) else {
                self.samples.fetch_sub(1, Ordering::AcqRel);
                return false;
            };
            if next > OUTPUT_MAX_QUEUED_BYTES {
                self.samples.fetch_sub(1, Ordering::AcqRel);
                return false;
            }
            match self.bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn release(&self, amount: usize) {
        self.bytes.fetch_sub(amount, Ordering::AcqRel);
        self.samples.fetch_sub(1, Ordering::AcqRel);
    }
}

struct OutputPump {
    sender: SyncSender<OutputMessage>,
    done: Receiver<()>,
    thread: Option<JoinHandle<()>>,
    status: Arc<SinkStatus>,
    cancelled: CancellationToken,
    #[cfg(test)]
    budget: Arc<OutputBudget>,
}

impl OutputPump {
    fn new(
        mut target: OutputTarget,
        status: Arc<SinkStatus>,
        cancelled: CancellationToken,
        budget: Arc<OutputBudget>,
    ) -> Result<Self> {
        // One item may be in the writer while 255 are queued; OutputBudget
        // enforces the shared 256-sample ceiling across both locations.
        let (sender, receiver) = mpsc::sync_channel(OUTPUT_QUEUE_SAMPLES - 1);
        let (done_sender, done) = mpsc::sync_channel(1);
        let worker_status = status.clone();
        let worker_cancelled = cancelled.clone();
        let worker_budget = budget.clone();
        let thread = thread::Builder::new()
            .name("recording-export-output".into())
            .spawn(move || {
                let result = output_loop(&mut target, &receiver, &worker_cancelled, &worker_budget);
                if let Err(failure) = result {
                    worker_status.set_failure(failure);
                    worker_cancelled.cancel();
                }
                let _ = done_sender.send(());
            })
            .context("unable to start recording export output thread")?;
        Ok(Self {
            sender,
            done,
            thread: Some(thread),
            status,
            cancelled,
            #[cfg(test)]
            budget,
        })
    }

    fn sender(&self) -> SyncSender<OutputMessage> {
        self.sender.clone()
    }

    fn finish(&mut self) -> Result<()> {
        let deadline = Instant::now() + OUTPUT_STALL_TIMEOUT;
        let mut finish = OutputMessage::Finish;
        loop {
            if self.status.failure().is_some() || self.cancelled.is_cancelled() {
                break;
            }
            match self.sender.try_send(finish) {
                Ok(()) => break,
                Err(TrySendError::Full(message)) if Instant::now() < deadline => {
                    finish = message;
                    thread::sleep(Duration::from_millis(10));
                }
                Err(TrySendError::Full(_)) => {
                    self.fail(OutputFailure::ConsumerStalled);
                    break;
                }
                Err(TrySendError::Disconnected(_)) => {
                    self.fail(OutputFailure::Write);
                    break;
                }
            }
        }
        self.wait_for_thread();
        match self.status.failure() {
            Some(OutputFailure::BrokenPipe) => Err(anyhow!("output stream closed")),
            Some(OutputFailure::ConsumerStalled) => Err(anyhow!("output consumer stalled")),
            Some(OutputFailure::Write) => Err(anyhow!("output stream write failed")),
            None => Ok(()),
        }
    }

    fn fail(&self, failure: OutputFailure) {
        self.status.set_failure(failure);
        self.cancelled.cancel();
    }

    fn wait_for_thread(&mut self) {
        if self.done.recv_timeout(OUTPUT_JOIN_TIMEOUT).is_err() {
            self.fail(OutputFailure::ConsumerStalled);
        }
        // The worker sends `done` as its final operation, so this short bounded
        // wait closes the tiny send-to-return race and lets us join normally.
        let deadline = Instant::now() + OUTPUT_POLL_INTERVAL * 4;
        while self
            .thread
            .as_ref()
            .is_some_and(|thread| !thread.is_finished())
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        if self.thread.as_ref().is_some_and(JoinHandle::is_finished) {
            let _ = self.thread.take().expect("thread checked above").join();
        }
    }

    fn abort(&mut self) {
        if self.thread.is_none() {
            return;
        }
        self.cancelled.cancel();
        self.wait_for_thread();
    }

    #[cfg(test)]
    fn reserved(&self) -> (usize, usize) {
        (
            self.budget.samples.load(Ordering::Acquire),
            self.budget.bytes.load(Ordering::Acquire),
        )
    }
}

impl Drop for OutputPump {
    fn drop(&mut self) {
        self.abort();
    }
}

impl SinkStatus {
    fn set_failure(&self, failure: OutputFailure) {
        let mut current = self
            .failure
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        current.get_or_insert(failure);
    }

    fn failure(&self) -> Option<OutputFailure> {
        *self
            .failure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

fn output_loop(
    target: &mut OutputTarget,
    receiver: &Receiver<OutputMessage>,
    cancelled: &CancellationToken,
    budget: &OutputBudget,
) -> std::result::Result<(), OutputFailure> {
    let result = loop {
        if cancelled.is_cancelled() {
            break Ok(());
        }
        let message = match receiver.recv_timeout(OUTPUT_POLL_INTERVAL) {
            Ok(message) => message,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break Ok(()),
        };
        match message {
            OutputMessage::Bytes(bytes) => {
                let result = target.write_all_bounded(&bytes, cancelled);
                budget.release(bytes.len());
                if let Err(failure) = result {
                    break Err(failure);
                }
            }
            OutputMessage::Finish => break target.flush(),
        }
    };
    for message in receiver.try_iter() {
        if let OutputMessage::Bytes(bytes) = message {
            budget.release(bytes.len());
        }
    }
    result
}

fn classify_io_error(error: std::io::Error) -> OutputFailure {
    if error.kind() == std::io::ErrorKind::BrokenPipe {
        OutputFailure::BrokenPipe
    } else {
        OutputFailure::Write
    }
}

#[cfg(unix)]
fn write_nonblocking(
    fd: &OwnedFd,
    mut bytes: &[u8],
    cancelled: &CancellationToken,
) -> std::result::Result<(), OutputFailure> {
    let mut last_progress = Instant::now();
    while !bytes.is_empty() {
        if cancelled.is_cancelled() {
            return Err(OutputFailure::ConsumerStalled);
        }
        // SAFETY: bytes is a live immutable slice for the duration of the call;
        // fd remains owned by OutputTarget. write retains neither pointer.
        let written = unsafe {
            libc::write(
                fd.as_raw_fd(),
                bytes.as_ptr().cast::<libc::c_void>(),
                bytes.len(),
            )
        };
        if written > 0 {
            bytes = &bytes[written as usize..];
            last_progress = Instant::now();
            continue;
        }
        if written == 0 {
            return Err(OutputFailure::Write);
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        if error.kind() != std::io::ErrorKind::WouldBlock {
            return Err(classify_io_error(error));
        }
        if last_progress.elapsed() >= OUTPUT_STALL_TIMEOUT {
            return Err(OutputFailure::ConsumerStalled);
        }
        let mut poll_fd = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLOUT,
            revents: 0,
        };
        // SAFETY: poll_fd points to one initialized pollfd for this call and no
        // alias mutates it concurrently. poll retains no pointers after return.
        let ready = unsafe {
            libc::poll(
                &mut poll_fd,
                1,
                OUTPUT_POLL_INTERVAL.as_millis() as libc::c_int,
            )
        };
        if ready < 0 && std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
            return Err(OutputFailure::Write);
        }
    }
    Ok(())
}

/// One fixed-track fragmented MP4 pipeline.
///
/// The topology is selected before `Playing`; tracks are never added after the
/// initial `ftyp`/`moov` bytes can leave the appsink.
pub(super) struct Fmp4Muxer {
    pipeline: Pipeline,
    video: AppSrc,
    audio: Option<AppSrc>,
    status: Arc<SinkStatus>,
    output_cancelled: CancellationToken,
    output: OutputPump,
    pipeline_failure: Option<PipelineFailure>,
    #[cfg(test)]
    force_eos_failure: bool,
    finished: bool,
}

#[derive(Debug)]
struct VideoRetimestamp {
    nominal_duration_us: u64,
    minimum_forward_delta_us: u64,
    last_pts_us: Option<u64>,
}

impl VideoRetimestamp {
    fn new(fps: u32) -> Self {
        let nominal_duration_us = 1_000_000 / u64::from(fps.clamp(1, 120));
        Self {
            nominal_duration_us,
            minimum_forward_delta_us: (nominal_duration_us / 2).max(1_000_000 / 120),
            last_pts_us: None,
        }
    }

    fn next_pts_us(&mut self, candidate: Option<u64>) -> u64 {
        let pts = match self.last_pts_us {
            None => candidate.unwrap_or(0),
            Some(last) => candidate
                .filter(|candidate| {
                    *candidate > last
                        && candidate.saturating_sub(last) >= self.minimum_forward_delta_us
                })
                .unwrap_or_else(|| last.saturating_add(self.nominal_duration_us)),
        };
        self.last_pts_us = Some(pts);
        pts
    }

    fn apply(&mut self, buffer: &mut BufferRef) {
        let pts_us = self.next_pts_us(buffer.pts().map(ClockTime::useconds));
        let timestamp = ClockTime::from_useconds(pts_us);
        buffer.set_pts(timestamp);
        buffer.set_dts(timestamp);
        buffer.set_duration(ClockTime::from_useconds(self.nominal_duration_us));
    }
}

impl Fmp4Muxer {
    pub(super) fn new(include_audio: bool, fps: u32, output_target: OutputTarget) -> Result<Self> {
        gstreamer::init().context("GStreamer initialization failed")?;
        // This command promises controlled, redacted stderr even when its
        // environment contains GST_DEBUG settings. No recording bytes, caps,
        // or caller-selected identifiers are permitted through GStreamer's
        // process-global debug logger.
        // SAFETY: GStreamer is initialized above; these process-global debug
        // setters accept plain enum/boolean values and retain no Rust pointers.
        unsafe {
            gstreamer::ffi::gst_debug_set_default_threshold(
                gstreamer::DebugLevel::None.into_glib(),
            );
            gstreamer::ffi::gst_debug_set_active(false.into_glib());
        }

        let audio_branch = if include_audio {
            "appsrc name=audiosrc is-live=false format=time block=true max-bytes=16777216 \
             caps=\"audio/mpeg,mpegversion=(int)4,stream-format=(string)adts\" \
             ! aacparse \
             ! audio/mpeg,mpegversion=(int)4,stream-format=(string)raw \
             ! queue max-size-bytes=16777216 max-size-buffers=0 max-size-time=0 \
             ! mux.audio_0 "
        } else {
            ""
        };
        let launch = format!(
            "appsrc name=videosrc is-live=false format=time block=true max-bytes=16777216 \
             caps=\"video/x-h264,stream-format=(string)byte-stream,framerate=(fraction){fps}/1\" \
             ! h264parse name=h264parse-recording config-interval=-1 disable-passthrough=true \
             ! video/x-h264,stream-format=(string)avc,alignment=(string)au,framerate=(fraction){fps}/1 \
             ! queue max-size-bytes=16777216 max-size-buffers=0 max-size-time=0 \
             ! mux.video_0 \
             {audio_branch} \
             mp4mux name=mux fragment-duration=1000 streamable=true \
             ! appsink name=outsink sync=false max-buffers=1 drop=false"
        );
        let pipeline = gstreamer::parse::launch(&launch)
            .context("required GStreamer MP4 elements are unavailable")?
            .dynamic_cast::<Pipeline>()
            .map_err(|_| anyhow!("GStreamer export graph is not a pipeline"))?;
        let video = appsrc(&pipeline, "videosrc")?;
        let audio = include_audio
            .then(|| appsrc(&pipeline, "audiosrc"))
            .transpose()?;
        configure_source(&video);
        if let Some(audio) = audio.as_ref() {
            configure_source(audio);
        }
        install_video_retimestamp(&pipeline, fps)?;

        let sink = pipeline
            .by_name("outsink")
            .context("GStreamer export graph has no output sink")?
            .dynamic_cast::<AppSink>()
            .map_err(|_| anyhow!("GStreamer export output is not an appsink"))?;
        let status = Arc::new(SinkStatus::default());
        let callback_status = status.clone();
        let output_cancelled = CancellationToken::new();
        let callback_cancelled = output_cancelled.clone();
        let budget = Arc::new(OutputBudget::default());
        let callback_budget = budget.clone();
        let output = OutputPump::new(
            output_target,
            status.clone(),
            output_cancelled.clone(),
            budget,
        )?;
        let output_sender = output.sender();
        sink.set_callbacks(
            AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let sample = sink.pull_sample().map_err(|_| gstreamer::FlowError::Eos)?;
                    let buffer = sample.buffer().ok_or(gstreamer::FlowError::Error)?;
                    let map = buffer
                        .map_readable()
                        .map_err(|_| gstreamer::FlowError::Error)?;
                    let bytes = map.as_slice();
                    if !callback_budget.reserve(bytes.len()) {
                        callback_status.set_failure(OutputFailure::ConsumerStalled);
                        callback_cancelled.cancel();
                        return Err(gstreamer::FlowError::Error);
                    }
                    let amount = bytes.len();
                    match output_sender.try_send(OutputMessage::Bytes(bytes.to_vec())) {
                        Ok(()) => Ok(gstreamer::FlowSuccess::Ok),
                        Err(TrySendError::Full(_)) => {
                            callback_budget.release(amount);
                            callback_status.set_failure(OutputFailure::ConsumerStalled);
                            callback_cancelled.cancel();
                            Err(gstreamer::FlowError::Error)
                        }
                        Err(TrySendError::Disconnected(_)) => {
                            callback_budget.release(amount);
                            callback_status.set_failure(OutputFailure::Write);
                            callback_cancelled.cancel();
                            Err(gstreamer::FlowError::Error)
                        }
                    }
                })
                .build(),
        );

        pipeline
            .set_state(State::Playing)
            .context("GStreamer export pipeline failed to start")?;
        Ok(Self {
            pipeline,
            video,
            audio,
            status,
            output_cancelled,
            output,
            pipeline_failure: None,
            #[cfg(test)]
            force_eos_failure: false,
            finished: false,
        })
    }

    pub(super) fn output_cancelled(&self) -> CancellationToken {
        self.output_cancelled.clone()
    }

    pub(super) fn output_failure(&self) -> Option<OutputFailure> {
        self.status.failure()
    }

    pub(super) fn pipeline_failure(&self) -> Option<PipelineFailure> {
        self.pipeline_failure
    }

    #[cfg(test)]
    pub(super) fn test_pipeline(&self) -> Pipeline {
        self.pipeline.clone()
    }

    #[cfg(test)]
    pub(super) fn test_output_reserved(&self) -> (usize, usize) {
        self.output.reserved()
    }

    #[cfg(test)]
    pub(super) fn test_force_eos_failure(&mut self) {
        self.force_eos_failure = true;
    }

    pub(super) fn push_video(
        &self,
        data: Vec<u8>,
        pts_us: u64,
        duration_us: u64,
        keyframe: bool,
    ) -> Result<()> {
        let mut buffer = timed_buffer(data, pts_us, duration_us)?;
        if !keyframe {
            buffer
                .get_mut()
                .expect("new buffer is uniquely owned")
                .set_flags(BufferFlags::DELTA_UNIT);
        }
        self.video
            .push_buffer(buffer)
            .map(|_| ())
            .map_err(|_| anyhow!("GStreamer rejected video input"))
    }

    pub(super) fn push_aac(&self, data: Vec<u8>, pts_us: u64, duration_us: u64) -> Result<()> {
        let source = self
            .audio
            .as_ref()
            .context("AAC arrived for a video-only export")?;
        source
            .push_buffer(timed_buffer(data, pts_us, duration_us)?)
            .map(|_| ())
            .map_err(|_| anyhow!("GStreamer rejected AAC input"))
    }

    pub(super) fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        #[cfg(test)]
        let forced_eos_failure = self.force_eos_failure;
        #[cfg(not(test))]
        let forced_eos_failure = false;
        let video_eos_failed = forced_eos_failure || self.video.end_of_stream().is_err();
        let audio_eos_failed = forced_eos_failure
            || self
                .audio
                .as_ref()
                .is_some_and(|audio| audio.end_of_stream().is_err());
        let eos_failed = video_eos_failed || audio_eos_failed;

        let bus = self
            .pipeline
            .bus()
            .context("GStreamer pipeline has no bus")?;
        let message = bus.timed_pop_filtered(EOS_TIMEOUT, &[MessageType::Eos, MessageType::Error]);
        let result = match message.as_ref().map(|message| message.view()) {
            Some(MessageView::Eos(_)) => Ok(()),
            Some(MessageView::Error(_)) if self.status.failure().is_some() => {
                self.pipeline_failure = Some(PipelineFailure::Output);
                Err(anyhow!("output stream closed"))
            }
            Some(MessageView::Error(error)) => {
                self.pipeline_failure = Some(SafeBusError::from_message(error).failure());
                Err(anyhow!("GStreamer failed while finalizing MP4"))
            }
            _ if !eos_failed => {
                self.pipeline_failure = Some(PipelineFailure::Timeout);
                Err(anyhow!("GStreamer MP4 finalization timed out"))
            }
            _ => Err(anyhow!("GStreamer rejected input EOS")),
        };
        let state_result = self.pipeline.set_state(State::Null);
        let output_result = self.output.finish();
        self.finished = true;
        result?;
        state_result.context("GStreamer export pipeline failed to stop")?;
        output_result?;
        Ok(())
    }
}

fn install_video_retimestamp(pipeline: &Pipeline, fps: u32) -> Result<()> {
    let parser = pipeline
        .by_name("h264parse-recording")
        .context("GStreamer export graph has no H.264 parser")?;
    let source = parser
        .static_pad("src")
        .context("GStreamer H.264 parser has no source pad")?;
    let state = Mutex::new(VideoRetimestamp::new(fps));
    source
        .add_probe(PadProbeType::BUFFER, move |_pad, info| {
            if let Some(PadProbeData::Buffer(buffer)) = info.data.as_mut() {
                state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .apply(buffer.make_mut());
            }
            PadProbeReturn::Ok
        })
        .context("unable to install H.264 retimestamp probe")?;
    Ok(())
}

impl Drop for Fmp4Muxer {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.pipeline.set_state(State::Null);
            self.output.abort();
        }
    }
}

fn appsrc(pipeline: &Pipeline, name: &str) -> Result<AppSrc> {
    pipeline
        .by_name(name)
        .with_context(|| format!("GStreamer export graph has no {name}"))?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("GStreamer export source has the wrong type"))
}

fn configure_source(source: &AppSrc) {
    source.set_stream_type(AppStreamType::Stream);
    source.set_format(gstreamer::Format::Time);
    source.set_block(true);
    source.set_max_bytes(APP_SRC_MAX_BYTES);
}

fn timed_buffer(data: Vec<u8>, pts_us: u64, duration_us: u64) -> Result<Buffer> {
    let mut buffer = Buffer::from_mut_slice(data);
    let buffer_ref = buffer
        .get_mut()
        .context("new GStreamer buffer is unexpectedly shared")?;
    let timestamp = ClockTime::from_useconds(pts_us);
    buffer_ref.set_pts(timestamp);
    buffer_ref.set_dts(timestamp);
    buffer_ref.set_duration(ClockTime::from_useconds(duration_us.max(1)));
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    struct SlowWriter {
        started: Arc<AtomicBool>,
        first: bool,
    }

    impl Write for SlowWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.started.store(true, Ordering::Release);
            if std::mem::take(&mut self.first) {
                thread::sleep(Duration::from_millis(500));
            }
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn timed_buffers_are_timestamped_and_mark_delta_frames() {
        gstreamer::init().unwrap();
        let mut buffer = timed_buffer(vec![1, 2, 3], 123, 40_000).unwrap();
        buffer.get_mut().unwrap().set_flags(BufferFlags::DELTA_UNIT);
        assert_eq!(buffer.pts(), Some(ClockTime::from_useconds(123)));
        assert_eq!(buffer.dts(), Some(ClockTime::from_useconds(123)));
        assert_eq!(buffer.duration(), Some(ClockTime::from_useconds(40_000)));
        assert!(buffer.flags().contains(BufferFlags::DELTA_UNIT));
    }

    #[test]
    fn video_retimestamp_handles_missing_duplicate_tiny_sparse_and_backward_candidates() {
        let mut timestamps = VideoRetimestamp::new(25);
        assert_eq!(timestamps.next_pts_us(None), 0);
        assert_eq!(timestamps.next_pts_us(Some(0)), 40_000);
        assert_eq!(timestamps.next_pts_us(Some(45_000)), 80_000);
        assert_eq!(timestamps.next_pts_us(Some(7_975_000)), 7_975_000);
        assert_eq!(timestamps.next_pts_us(Some(100)), 8_015_000);

        let mut first_candidate = VideoRetimestamp::new(20);
        assert_eq!(first_candidate.next_pts_us(Some(123_000)), 123_000);
    }

    #[test]
    fn video_retimestamp_sets_timing_without_changing_parser_flags() {
        gstreamer::init().unwrap();
        let mut timestamps = VideoRetimestamp::new(25);
        let mut buffer = Buffer::from_mut_slice(vec![1, 2, 3]);
        buffer
            .get_mut()
            .unwrap()
            .set_flags(BufferFlags::DELTA_UNIT | BufferFlags::MARKER);
        timestamps.apply(buffer.get_mut().unwrap());
        assert_eq!(buffer.pts(), Some(ClockTime::ZERO));
        assert_eq!(buffer.dts(), Some(ClockTime::ZERO));
        assert_eq!(buffer.duration(), Some(ClockTime::from_useconds(40_000)));
        assert!(buffer.flags().contains(BufferFlags::DELTA_UNIT));
        assert!(buffer.flags().contains(BufferFlags::MARKER));
    }

    #[test]
    fn video_input_caps_omit_unproven_alignment_and_keep_validated_fps() {
        let muxer = Fmp4Muxer::new(
            false,
            20,
            OutputTarget::test_writer(Box::new(std::io::sink())),
        )
        .unwrap();
        let caps = muxer.video.caps().expect("video appsrc caps");
        let structure = caps.structure(0).expect("video appsrc caps structure");
        assert_eq!(
            structure.get::<&str>("stream-format").unwrap(),
            "byte-stream"
        );
        assert!(!structure.has_field("alignment"));
        assert_eq!(
            structure.get::<gstreamer::Fraction>("framerate").unwrap(),
            gstreamer::Fraction::new(20, 1)
        );
    }

    #[test]
    fn bus_error_classification_uses_only_allowlisted_metadata() {
        gstreamer::init().unwrap();
        let source = gstreamer::ElementFactory::make("identity")
            .name("h264parse-fixture")
            .build()
            .unwrap();
        let message = gstreamer::message::Error::builder(
            gstreamer::StreamError::Decode,
            "private fixture recording identifier",
        )
        .src(&source)
        .debug("private fixture path and protocol details")
        .build();
        let MessageView::Error(error) = message.view() else {
            panic!("fixture message was not an error");
        };
        let safe = SafeBusError::from_message(error);
        assert_eq!(safe.source, SafeBusSource::H264Parse);
        assert_eq!(safe.domain, SafeBusDomain::Stream);
        assert_eq!(safe.code, gstreamer::StreamError::Decode.code());
        assert_eq!(safe.failure(), PipelineFailure::H264Parse);
        let public = format!("{:?}", safe.failure());
        assert!(!public.contains("private"));
        assert!(!public.contains("fixture path"));
    }

    #[test]
    fn safe_bus_category_precedes_forced_eos_send_failure() {
        gstreamer::init().unwrap();
        let mut muxer = Fmp4Muxer::new(
            false,
            25,
            OutputTarget::test_writer(Box::new(Vec::<u8>::new())),
        )
        .unwrap();
        let parser = muxer
            .pipeline
            .iterate_elements()
            .into_iter()
            .flatten()
            .find(|element| element.name().starts_with("h264parse"))
            .expect("fixture pipeline has an H.264 parser");
        parser
            .post_message(
                gstreamer::message::Error::builder(
                    gstreamer::StreamError::Decode,
                    "private fixture recording identifier",
                )
                .src(&parser)
                .debug("private fixture path and protocol details")
                .build(),
            )
            .unwrap();
        muxer.test_force_eos_failure();
        assert!(muxer.finish().is_err());
        assert_eq!(muxer.pipeline_failure(), Some(PipelineFailure::H264Parse));
    }

    #[test]
    fn burst_above_eight_with_temporarily_full_queue_drains_and_finishes() {
        let started = Arc::new(AtomicBool::new(false));
        let status = Arc::new(SinkStatus::default());
        let cancelled = CancellationToken::new();
        let budget = Arc::new(OutputBudget::default());
        let mut pump = OutputPump::new(
            OutputTarget::test_writer(Box::new(SlowWriter {
                started: started.clone(),
                first: true,
            })),
            status,
            cancelled,
            budget.clone(),
        )
        .unwrap();
        let sender = pump.sender();
        assert!(budget.reserve(1));
        sender.try_send(OutputMessage::Bytes(vec![0])).unwrap();
        let start_deadline = Instant::now() + Duration::from_secs(1);
        while !started.load(Ordering::Acquire) && Instant::now() < start_deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(started.load(Ordering::Acquire));
        for _ in 0..OUTPUT_QUEUE_SAMPLES - 1 {
            assert!(budget.reserve(1));
            sender.try_send(OutputMessage::Bytes(vec![0])).unwrap();
        }
        assert_eq!(
            pump.reserved(),
            (OUTPUT_QUEUE_SAMPLES, OUTPUT_QUEUE_SAMPLES)
        );
        assert!(OUTPUT_QUEUE_SAMPLES > 8);
        pump.finish().unwrap();
        assert_eq!(pump.reserved(), (0, 0));
    }

    #[test]
    fn appsink_is_limited_to_one_outside_budget_sample() {
        let muxer = Fmp4Muxer::new(
            false,
            25,
            OutputTarget::test_writer(Box::new(std::io::sink())),
        )
        .unwrap();
        let sink = muxer
            .test_pipeline()
            .by_name("outsink")
            .unwrap()
            .dynamic_cast::<AppSink>()
            .unwrap();
        assert_eq!(sink.property::<u32>("max-buffers"), 1);
    }
}
