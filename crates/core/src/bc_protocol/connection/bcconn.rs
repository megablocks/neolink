use super::{BcSubscription, BcSubscriptionItem};
use crate::{
    bc::{codex::DecodedBc, de::MAX_BODY_LEN, model::*},
    Error, Result,
};
use futures::future::BoxFuture;
use futures::sink::{Sink, SinkExt};
use futures::stream::{Stream, StreamExt};
use log::*;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{
    mpsc::{channel, error::TrySendError, Sender},
    OwnedSemaphorePermit, Semaphore,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use tokio::{sync::RwLock, task::JoinSet};

type MsgHandler = dyn 'static + Send + Sync + for<'a> Fn(&'a Bc) -> BoxFuture<'a, Option<Bc>>;

/// Buffer capacity for a subscriber's incoming-message channel.
const SUB_CHANNEL_CAP: usize = 500;
/// Total accounted raw BC bytes allowed in the connection command queue and in
/// the poller command currently being routed. The source task may own one
/// already-parsed envelope while waiting for this budget.
const CONNECTION_INGRESS_MAX_BUFFERED_BYTES: usize = 64 * 1024 * 1024;

/// Conservatively account one decoded BC envelope without serializing it again.
///
/// The parser-provided wire body length is used whenever available. For a
/// manually constructed/test envelope without that sidecar, a binary body
/// without an extension retains its exact decoded length; any XML, extension,
/// legacy, mixed, or header-only envelope is charged the full parser body
/// ceiling. Decoded strings and nested vectors therefore cannot bypass the raw
/// byte budgets merely because their wire representation is unavailable.
fn bc_envelope_accounted_bytes(value: &Bc, wire_body_len: Option<u32>) -> u32 {
    let body_bytes = wire_body_len.map_or_else(
        || match &value.body {
            BcBody::ModernMsg(ModernMsg {
                extension: None,
                payload: Some(BcPayloads::Binary(data)),
            }) => data.len(),
            _ => MAX_BODY_LEN as usize,
        },
        |bytes| bytes as usize,
    );
    u32::try_from(
        body_bytes
            .checked_add(std::mem::size_of::<Bc>())
            .expect("BC accounting size cannot overflow usize"),
    )
    .expect("BC accounting size is bounded below u32::MAX")
}

struct IngressItem {
    result: Option<Result<Bc>>,
    accounted_bytes: Option<u32>,
    _reservation: Option<OwnedSemaphorePermit>,
}

impl IngressItem {
    async fn reserve(
        value: Result<Bc>,
        wire_body_len: Option<u32>,
        budget: Arc<Semaphore>,
    ) -> Result<Self> {
        let accounted_bytes = match &value {
            Ok(response) => Some(bc_envelope_accounted_bytes(response, wire_body_len)),
            Err(_) => None,
        };
        let reservation = match &value {
            Ok(_) => Some(
                budget
                    .acquire_many_owned(accounted_bytes.expect("successful BC has accounting"))
                    .await
                    .map_err(|_| Error::ConnectionShutdown)?,
            ),
            Err(_) => None,
        };
        Ok(Self {
            result: Some(value),
            accounted_bytes,
            _reservation: reservation,
        })
    }

    fn into_parts(mut self) -> (Result<Bc>, Option<u32>, Option<OwnedSemaphorePermit>) {
        (
            self.result
                .take()
                .expect("ingress item is consumed exactly once"),
            self.accounted_bytes,
            self._reservation.take(),
        )
    }
}

#[derive(Default)]
struct Subscriber {
    /// Subscribers based on their ID and their num
    /// First filtered by ID then number
    /// If num is None it will be upgraded to a Some based on the number the
    /// camera assigns
    num: BTreeMap<u32, BTreeMap<Option<u16>, SubscriberChannel>>,
    /// Subscribers based on their ID
    id: BTreeMap<u32, Arc<MsgHandler>>,
}

#[derive(Clone)]
struct SubscriberChannel {
    sender: Sender<BcSubscriptionItem>,
    /// Optional fail-fast signal for loss-intolerant bounded consumers.
    ///
    /// The connection poller must never await one subscriber. Generic live
    /// streams therefore retain their historical best-effort drop behavior,
    /// while recording replay supplies this signal and terminates immediately
    /// if its deliberately small raw queue ever overflows.
    overflow: Option<CancellationToken>,
    /// Conservatively accounted BC envelope bytes allowed to wait in this raw
    /// subscription. Generic subscriptions do not set a byte budget.
    byte_budget: Option<Arc<Semaphore>>,
}

impl SubscriberChannel {
    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    fn try_send(&self, value: Result<Bc>, accounted_bytes: Option<u32>) -> SubscriberSendResult {
        let bytes = match &value {
            Ok(response) => {
                Some(accounted_bytes.unwrap_or_else(|| bc_envelope_accounted_bytes(response, None)))
            }
            Err(_) => None,
        };
        let reservation = if let (Some(bytes), Some(byte_budget)) = (bytes, &self.byte_budget) {
            match byte_budget.clone().try_acquire_many_owned(bytes) {
                Ok(reservation) => Some(reservation),
                Err(_) => {
                    self.signal_overflow();
                    return SubscriberSendResult::Full;
                }
            }
        } else {
            None
        };
        match self
            .sender
            .try_send(BcSubscriptionItem::new(value, reservation))
        {
            Ok(()) => SubscriberSendResult::Sent,
            Err(TrySendError::Full(_)) => {
                self.signal_overflow();
                SubscriberSendResult::Full
            }
            Err(TrySendError::Closed(_)) => SubscriberSendResult::Closed,
        }
    }

    fn capacity(&self) -> usize {
        self.sender.capacity()
    }

    fn max_capacity(&self) -> usize {
        self.sender.max_capacity()
    }

    fn signal_overflow(&self) {
        if let Some(overflow) = &self.overflow {
            overflow.cancel();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubscriberSendResult {
    Sent,
    Full,
    Closed,
}

pub(crate) type BcConnSink = Box<dyn Sink<Bc, Error = Error> + Send + Sync + Unpin>;
pub(crate) type BcConnSource = Box<dyn Stream<Item = Result<DecodedBc>> + Send + Sync + Unpin>;

/// A shareable connection to a camera.  Handles serialization of messages.  To send/receive, call
/// .[subscribe()] with a message number.  You can use the BcSubscription to send or receive only
/// messages with that number; each incoming message is routed to its appropriate subscriber.
///
/// There can be only one subscriber per kind of message at a time.
pub struct BcConnection {
    sink: Sender<Result<Bc>>,
    poll_commander: Sender<PollCommand>,
    rx_thread: RwLock<JoinSet<Result<()>>>,
    cancel: CancellationToken,
}

impl BcConnection {
    pub async fn new(mut sink: BcConnSink, mut source: BcConnSource) -> Result<BcConnection> {
        let (sinker, sinker_rx) = channel::<Result<Bc>>(500);
        let cancel = CancellationToken::new();

        // Raised from 200 -> 1000 for parity with the now non-blocking poll loop:
        // the poller never awaits a slow subscriber, so the only place backpressure
        // can build is this command queue; a deeper queue tolerates short bursts of
        // camera traffic without dropping inbound packets at the source stream.
        let (poll_commander, poll_commanded) = channel(1000);
        let ingress_byte_budget = Arc::new(Semaphore::new(CONNECTION_INGRESS_MAX_BUFFERED_BYTES));
        let mut poller = Poller {
            subscribers: Default::default(),
            sink: sinker.clone(),
            reciever: ReceiverStream::new(poll_commanded),
            last_full_warn: None,
            dropped_full: 0,
        };

        let mut rx_thread = JoinSet::<Result<()>>::new();
        let thread_poll_commander = poll_commander.clone();
        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => {
                    Result::Ok(())
                },
                v = async {
                    let sender = thread_poll_commander;
                    while let Some(decoded) = source.next().await {
                        let (bc, wire_body_len) = match decoded {
                            Ok(decoded) => (Ok(decoded.message), Some(decoded.wire_body_len)),
                            Err(error) => (Err(error), None),
                        };
                        let ingress = IngressItem::reserve(
                            bc,
                            wire_body_len,
                            ingress_byte_budget.clone(),
                        )
                        .await?;
                        sender.send(PollCommand::Bc(Box::new(ingress))).await?;
                    }
                    Result::Ok(())
                } => v
            }
        });

        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    let mut stream = ReceiverStream::new(sinker_rx);
                    while let Some(packet) = stream.next().await {
                        sink.send(packet?).await?;
                    }
                    Ok(())
                } => v
            }
        });

        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    // `poller.run()` loops internally until its command channel
                    // either errors or is exhausted. An `Ok(())` return means the
                    // channel closed (all senders dropped) — i.e. the connection is
                    // gone and no further commands will ever arrive. Re-looping here
                    // would re-enter `run()`, whose `reciever.next().await` is now
                    // immediately `Ready(None)` forever: a single poll spins without
                    // yielding, pinning a core AND starving the `select!` so even
                    // `thread_cancel` can't stop it. So treat `Ok(())` as terminal.
                    let res = poller.run().await;
                    trace!("Polling has ended: {res:?}");
                    res
                }=> v
            }
        });

        Ok(BcConnection {
            sink: sinker,
            poll_commander,
            rx_thread: RwLock::new(rx_thread),
            cancel,
        })
    }

    pub(super) async fn send(&self, bc: Bc) -> crate::Result<()> {
        self.sink.send(Ok(bc)).await?;
        Ok(())
    }

    pub async fn subscribe(&self, msg_id: u32, msg_num: u16) -> Result<BcSubscription<'_>> {
        self.subscribe_inner(msg_id, Some(msg_num), SUB_CHANNEL_CAP, None, None)
            .await
    }

    /// Subscribe with a small explicit capacity and a loss notification.
    ///
    /// This is reserved for protocols such as stored-recording replay where
    /// dropping one opaque `Bc` envelope would corrupt a byte stream. The
    /// poller remains non-blocking, but cancellation of the returned token
    /// makes overflow observable so the protocol can STOP instead of silently
    /// continuing with missing data.
    pub(crate) async fn subscribe_bounded(
        &self,
        msg_id: u32,
        msg_num: u16,
        capacity: usize,
        max_buffered_bytes: usize,
    ) -> Result<(BcSubscription<'_>, CancellationToken)> {
        if capacity == 0 || max_buffered_bytes == 0 || max_buffered_bytes > u32::MAX as usize {
            return Err(Error::Other("Subscription bounds are invalid"));
        }
        let overflow = CancellationToken::new();
        let subscription = self
            .subscribe_inner(
                msg_id,
                Some(msg_num),
                capacity,
                Some(overflow.clone()),
                Some(Arc::new(Semaphore::new(max_buffered_bytes))),
            )
            .await?;
        Ok((subscription, overflow))
    }

    async fn subscribe_inner(
        &self,
        msg_id: u32,
        msg_num: Option<u16>,
        capacity: usize,
        overflow: Option<CancellationToken>,
        byte_budget: Option<Arc<Semaphore>>,
    ) -> Result<BcSubscription<'_>> {
        let (tx, rx) = channel(capacity);
        self.poll_commander
            .send(PollCommand::AddSubscriber(
                msg_id,
                msg_num,
                SubscriberChannel {
                    sender: tx,
                    overflow,
                    byte_budget,
                },
            ))
            .await?;
        Ok(BcSubscription::new(rx, msg_num.map(u32::from), self))
    }

    /// Some messages are initiated by the camera. This creates a handler for them
    /// It requires a closure that will be used to handle the message
    /// and return either None or Some(Bc) reply
    pub async fn handle_msg<T>(&self, msg_id: u32, handler: T) -> Result<()>
    where
        T: 'static + Send + Sync + for<'a> Fn(&'a Bc) -> BoxFuture<'a, Option<Bc>>,
    {
        self.poll_commander
            .send(PollCommand::AddHandler(msg_id, Arc::new(handler)))
            .await?;
        Ok(())
    }

    /// Some times we want to wait for a reply on a new message ID
    /// to do this we wait for the next packet with a certain ID
    /// grab it's message ID and then subscribe to that ID
    ///
    /// The command Snap that grabs a jpeg payload is an example of this
    ///
    /// This function creates a temporary handle to grab this single message
    pub async fn subscribe_to_id(&self, msg_id: u32) -> Result<BcSubscription<'_>> {
        self.subscribe_inner(msg_id, None, SUB_CHANNEL_CAP, None, None)
            .await
    }

    pub(crate) async fn join(&self) -> Result<()> {
        let mut locked_threads = self.rx_thread.write().await;
        while let Some(res) = locked_threads.join_next().await {
            match res {
                Err(e) => {
                    locked_threads.abort_all();
                    return Err(e.into());
                }
                Ok(Err(e)) => {
                    locked_threads.abort_all();
                    return Err(e);
                }
                Ok(Ok(())) => {}
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        // Cancellation must happen before any potentially blocking queue
        // operation. A saturated poll command queue previously prevented the
        // transport tasks from ever seeing cancellation, including during the
        // replay fail-closed path that is specifically meant to stop a camera
        // continuing to send media.
        self.cancel.cancel();
        let _ = self.poll_commander.try_send(PollCommand::Disconnect);
        let mut locked_threads = self.rx_thread.write().await;
        while locked_threads.join_next().await.is_some() {}
        Ok(())
    }
}

impl Drop for BcConnection {
    fn drop(&mut self) {
        log::trace!("Drop BcConnection");
        self.cancel.cancel();

        let poll_commander = self.poll_commander.clone();
        let _gt = tokio::runtime::Handle::current().enter();
        let mut threads = std::mem::take(&mut self.rx_thread);
        tokio::task::spawn(async move {
            let _ = poll_commander.try_send(PollCommand::Disconnect);
            let locked_threads = threads.get_mut();
            while locked_threads.join_next().await.is_some() {}
            log::trace!("Dropped BcConnection");
        });
    }
}

enum PollCommand {
    Bc(Box<IngressItem>),
    AddHandler(u32, Arc<MsgHandler>),
    AddSubscriber(u32, Option<u16>, SubscriberChannel),
    Disconnect,
}

impl std::fmt::Debug for PollCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollCommand::Bc(_) => f.write_str("PollCommand::Bc"),
            PollCommand::AddHandler(_, _) => f.write_str("PollCommand::AddHandler"),
            PollCommand::AddSubscriber(_, _, _) => f.write_str("PollCommand::AddSubscriber"),
            PollCommand::Disconnect => f.write_str("PollCommand::Disconnect"),
        }
    }
}

struct Poller {
    subscribers: Subscriber,
    sink: Sender<Result<Bc>>,
    reciever: ReceiverStream<PollCommand>,
    /// Last time we emitted a "subscriber channel full" warning. Delivery to
    /// subscribers is non-blocking (see `run`), so a stalled consumer makes us
    /// drop its messages; this rate-limits the resulting log spam.
    last_full_warn: Option<std::time::Instant>,
    /// Count of messages dropped (across all subscribers) since the last warning.
    dropped_full: u64,
}

struct TraceResponse<'a>(&'a Bc);

impl std::fmt::Debug for TraceResponse<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if contains_file_info_list(self.0) {
            formatter.write_str("<FileInfoList response redacted>")
        } else {
            std::fmt::Debug::fmt(self.0, formatter)
        }
    }
}

fn contains_file_info_list(response: &Bc) -> bool {
    is_file_info_list_message(response.meta.msg_id)
        || matches!(
            &response.body,
            BcBody::ModernMsg(ModernMsg {
                payload: Some(BcPayloads::BcXml(BcXml {
                    file_info_list: Some(_),
                    ..
                })),
                ..
            })
        )
}

impl Poller {
    async fn run(&mut self) -> Result<()> {
        let cancel = CancellationToken::new();
        let _dropguard = cancel.clone().drop_guard();
        while let Some(command) = self.reciever.next().await {
            // Clean Up subscribers
            self.subscribers
                .num
                .iter_mut()
                .for_each(|(_, channels)| channels.retain(|_, channel| !channel.is_closed()));
            self.subscribers
                .num
                .retain(|_, channels| !channels.is_empty());
            // Handle the command
            match command {
                PollCommand::Bc(ingress) => {
                    let (response, accounted_bytes, ingress_reservation) = ingress.into_parts();
                    match response {
                        Ok(response) => {
                            let msg_id = response.meta.msg_id;
                            let msg_num = response.meta.msg_num;
                            log::trace!(
                                "Looking for ID: {} with num: {}, in {:?} and {:?}",
                                msg_id,
                                msg_num,
                                self.subscribers.id.keys().to_owned(),
                                self.subscribers
                                    .num
                                    .iter()
                                    .map(|(k, v)| (k, v.keys()))
                                    .collect::<Vec<_>>(),
                            );
                            match (
                                self.subscribers.id.get(&msg_id),
                                self.subscribers.num.get_mut(&msg_id), // Both filter first on ID
                            ) {
                                (Some(occ), _) => {
                                    log::trace!("Calling ID callback");
                                    let occ = occ.clone();
                                    let sink = self.sink.clone();
                                    let ingress_reservation = ingress_reservation;
                                    // Move this on another thread coz I have NO idea
                                    // how long the callback will run for
                                    // and we must NOT hang
                                    let cancel = cancel.clone();
                                    tokio::task::spawn(async move {
                                        // A handler owns the decoded envelope outside the poll
                                        // queue, so retain its ingress charge until the handler
                                        // has finished with the message.
                                        let _ingress_reservation = ingress_reservation;
                                        tokio::select! {
                                            _ = cancel.cancelled() => Result::Ok(()),
                                            v = occ(&response) => {
                                                if let Some(reply) = v {
                                                    assert!(reply.meta.msg_num == response.meta.msg_num);
                                                    sink.send(Ok(reply)).await?;
                                                }
                                                Result::Ok(())
                                            }
                                        }
                                    });
                                    log::trace!("Called ID callback");
                                }
                                (None, Some(occ)) => {
                                    let sender = if let Some(sender) =
                                        occ.get(&Some(msg_num)).filter(|a| !a.is_closed()).cloned()
                                    {
                                        // Connection with id exists and is not closed
                                        Some(sender)
                                    } else if let Some(sender) = occ.get(&None).cloned() {
                                        // Upgrade a None to a known MsgID
                                        occ.remove(&None);
                                        occ.insert(Some(msg_num), sender.clone());
                                        Some(sender)
                                    } else if occ
                                        .get(&Some(msg_num))
                                        .map(|a| a.is_closed())
                                        .unwrap_or(false)
                                    {
                                        // Connection is closed and there is no None to replace it
                                        // Remove it for cleanup and report no sender
                                        occ.remove(&Some(msg_num));
                                        None
                                    } else {
                                        None
                                    };
                                    if let Some(sender) = sender {
                                        // Non-blocking delivery: the poll loop must NEVER await a
                                        // single consumer. A slow/stalled subscriber (e.g. an
                                        // overwhelmed RTSP client's video subscription) would
                                        // otherwise block this loop and starve camera
                                        // keepalive/control traffic, causing the camera to drop
                                        // the session and forcing a reconnect cycle. So we
                                        // `try_send` and, when the subscriber's channel is full,
                                        // drop the message for that subscriber (rate-limited warn).
                                        //
                                        // Dropping is NOT keyframe-aware here: at this layer the
                                        // payload is an opaque `Bc` message, so we cannot cheaply
                                        // tell a keyframe from a P-frame. Keyframe-aware dropping
                                        // already happens downstream in the RTSP relay
                                        // (`drop_until_keyframe`); here we only guarantee the poll
                                        // loop never blocks.
                                        match sender.try_send(Ok(response), accounted_bytes) {
                                            SubscriberSendResult::Sent => {
                                                trace!(
                                                    "Remaining: {} of {} message space for {} (ID: {})",
                                                    sender.capacity(),
                                                    sender.max_capacity(),
                                                    &msg_num,
                                                    &msg_id
                                                );
                                            }
                                            SubscriberSendResult::Full => {
                                                self.dropped_full += 1;
                                                let now = std::time::Instant::now();
                                                let should_warn = self
                                                    .last_full_warn
                                                    .map(|t| {
                                                        now.duration_since(t)
                                                            >= std::time::Duration::from_secs(5)
                                                    })
                                                    .unwrap_or(true);
                                                if should_warn {
                                                    warn!(
                                                        "Subscriber channel full for num {} (ID: {}); dropped {} message(s) to keep the poll loop responsive (camera keepalive/control must not stall)",
                                                        &msg_num, &msg_id, self.dropped_full
                                                    );
                                                    self.last_full_warn = Some(now);
                                                    self.dropped_full = 0;
                                                }
                                            }
                                            SubscriberSendResult::Closed => {
                                                // Subscriber went away; it is removed from the map
                                                // at the top of the next loop iteration.
                                                trace!(
                                                    "Subscriber channel closed for num {} (ID: {})",
                                                    &msg_num,
                                                    &msg_id
                                                );
                                            }
                                        }
                                    } else {
                                        trace!(
                                            "Ignoring uninteresting message id {} (number: {})",
                                            msg_id,
                                            msg_num
                                        );
                                        trace!("Contents: {:?}", TraceResponse(&response));
                                    }
                                }
                                (None, None) => {
                                    trace!(
                                        "Ignoring uninteresting message id {} (number: {})",
                                        msg_id,
                                        msg_num
                                    );
                                    trace!("Contents: {:?}", TraceResponse(&response));
                                }
                            }
                        }
                        Err(e) => {
                            // Terminal: broadcast the error and tear down. Use try_send so a
                            // full subscriber channel can't block teardown either; any
                            // subscriber that misses this will observe the channel close.
                            for sub in self.subscribers.num.values() {
                                for sender in sub.values() {
                                    let _ = sender.try_send(Err(e.clone()), None);
                                }
                            }
                            self.subscribers.num.clear();
                            self.subscribers.id.clear();
                            return Err(e);
                        }
                    }
                }
                PollCommand::AddHandler(msg_id, handler) => {
                    match self.subscribers.id.entry(msg_id) {
                        Entry::Vacant(vac_entry) => {
                            vac_entry.insert(handler);
                        }
                        Entry::Occupied(_) => {
                            return Err(Error::SimultaneousSubscriptionId { msg_id });
                        }
                    };
                }
                PollCommand::AddSubscriber(msg_id, msg_num, subscriber) => {
                    match self
                        .subscribers
                        .num
                        .entry(msg_id)
                        .or_default()
                        .entry(msg_num)
                    {
                        Entry::Vacant(vac_entry) => {
                            vac_entry.insert(subscriber);
                        }
                        Entry::Occupied(mut occ_entry) => {
                            if occ_entry.get().is_closed() {
                                occ_entry.insert(subscriber);
                            } else {
                                // log::error!("Failed to subscribe in bcconn to {:?} for {:?}", msg_num, msg_id);
                                let _ = subscriber
                                    .sender
                                    .send(BcSubscriptionItem::new(
                                        Err(Error::SimultaneousSubscription { msg_num }),
                                        None,
                                    ))
                                    .await;
                            }
                        }
                    };
                }
                PollCommand::Disconnect => {
                    return Err(Error::ConnectionShutdown);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bc::{
        model::{Bc, BcBody, BcMeta, BcPayloads, ModernMsg},
        xml::{BcXml, FileInfo, FileInfoList},
    };
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::task::Poll;
    use tokio::time::{timeout, Duration};

    fn make_bc(msg_id: u32, msg_num: u16) -> Bc {
        Bc {
            meta: BcMeta {
                msg_id,
                channel_id: 0,
                stream_type: 0,
                response_code: 0,
                msg_num,
                class: 0x6614,
            },
            body: BcBody::ModernMsg(ModernMsg::default()),
        }
    }

    fn make_binary_bc(msg_id: u32, msg_num: u16, bytes: usize) -> Bc {
        Bc {
            meta: make_bc(msg_id, msg_num).meta,
            body: BcBody::ModernMsg(ModernMsg {
                extension: None,
                payload: Some(BcPayloads::Binary(vec![0x55; bytes])),
            }),
        }
    }

    fn make_xml_bc(msg_id: u32, msg_num: u16, bytes: usize) -> Bc {
        Bc {
            meta: make_bc(msg_id, msg_num).meta,
            body: BcBody::ModernMsg(ModernMsg {
                extension: None,
                payload: Some(BcPayloads::BcXml(BcXml {
                    file_info_list: Some(FileInfoList {
                        file_info: vec![FileInfo {
                            id: Some("x".repeat(bytes)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
            }),
        }
    }

    fn make_extension_bc(msg_id: u32, msg_num: u16, bytes: usize) -> Bc {
        Bc {
            meta: make_bc(msg_id, msg_num).meta,
            body: BcBody::ModernMsg(ModernMsg {
                extension: Some(Extension {
                    token: Some("x".repeat(bytes)),
                    ..Default::default()
                }),
                payload: None,
            }),
        }
    }

    fn make_mixed_bc(msg_id: u32, msg_num: u16, bytes: usize) -> Bc {
        Bc {
            meta: make_bc(msg_id, msg_num).meta,
            body: BcBody::ModernMsg(ModernMsg {
                extension: Some(Extension {
                    binary_data: Some(1),
                    token: Some("x".repeat(bytes)),
                    ..Default::default()
                }),
                payload: Some(BcPayloads::Binary(vec![0x55; bytes])),
            }),
        }
    }

    fn unreserved_ingress(value: Result<Bc>) -> IngressItem {
        let accounted_bytes = value
            .as_ref()
            .ok()
            .map(|response| bc_envelope_accounted_bytes(response, None));
        IngressItem {
            result: Some(value),
            accounted_bytes,
            _reservation: None,
        }
    }

    #[test]
    fn unmatched_recording_responses_are_redacted() {
        let response = Bc {
            meta: BcMeta {
                msg_id: MSG_ID_FILE_INFO_LIST_GET,
                ..make_bc(0, 0).meta
            },
            body: BcBody::ModernMsg(ModernMsg {
                extension: None,
                payload: Some(BcPayloads::BcXml(BcXml {
                    file_info_list: Some(FileInfoList {
                        file_info: vec![FileInfo {
                            id: Some("/fixture/private-recording-id".to_owned()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
            }),
        };
        let output = format!("{:?}", TraceResponse(&response));

        assert_eq!(output, "<FileInfoList response redacted>");
        assert!(!output.contains("private-recording-id"));
    }

    #[test]
    fn typed_recording_payload_is_redacted_even_with_an_unknown_message_id() {
        let response = Bc {
            meta: make_bc(999, 0).meta,
            body: BcBody::ModernMsg(ModernMsg {
                extension: None,
                payload: Some(BcPayloads::BcXml(BcXml {
                    file_info_list: Some(FileInfoList::default()),
                    ..Default::default()
                })),
            }),
        };

        assert_eq!(
            format!("{:?}", TraceResponse(&response)),
            "<FileInfoList response redacted>"
        );
    }

    #[test]
    fn replay_start_and_stop_ids_are_redacted_without_typed_xml() {
        for msg_id in [MSG_ID_FILE_INFO_LIST_REPLAY, MSG_ID_FILE_INFO_LIST_STOP] {
            let response = Bc {
                meta: BcMeta {
                    msg_id,
                    ..make_bc(0, 0).meta
                },
                body: BcBody::ModernMsg(ModernMsg {
                    extension: None,
                    payload: Some(BcPayloads::Binary(
                        b"PRIVATE_FIXTURE_RECORDING_BYTES".to_vec(),
                    )),
                }),
            };
            let output = format!("{:?}", TraceResponse(&response));
            assert_eq!(output, "<FileInfoList response redacted>");
            assert!(!output.contains("PRIVATE"));
        }
    }

    /// Regression test for the keepalive-starvation bug (upstream #399): a
    /// subscriber whose channel is full must NOT block the poll loop. We feed
    /// many messages to an undrained (capacity-1) subscriber and assert that
    /// `Poller::run` still drains its command queue and returns promptly. With
    /// the old blocking `sender.send().await`, the second message would wedge
    /// the loop forever and the timeout below would fire.
    #[tokio::test]
    async fn poller_does_not_block_on_full_subscriber() {
        let (cmd_tx, cmd_rx) = channel::<PollCommand>(1000);
        let (sink_tx, _sink_rx) = channel::<Result<Bc>>(500);

        let mut poller = Poller {
            subscribers: Default::default(),
            sink: sink_tx,
            reciever: ReceiverStream::new(cmd_rx),
            last_full_warn: None,
            dropped_full: 0,
        };

        let msg_id = 42u32;
        let msg_num = 7u16;

        // A capacity-1 subscriber channel we deliberately never drain. Hold the
        // receiver so the channel stays *open* (not closed) -> the Full path,
        // not the Closed path, is exercised.
        let (sub_tx, mut sub_rx) = channel::<BcSubscriptionItem>(1);
        let overflow = CancellationToken::new();
        cmd_tx
            .send(PollCommand::AddSubscriber(
                msg_id,
                Some(msg_num),
                SubscriberChannel {
                    sender: sub_tx,
                    overflow: Some(overflow.clone()),
                    byte_budget: None,
                },
            ))
            .await
            .unwrap();

        // Far more messages than the subscriber can hold.
        for _ in 0..100 {
            cmd_tx
                .send(PollCommand::Bc(Box::new(unreserved_ingress(Ok(make_bc(
                    msg_id, msg_num,
                ))))))
                .await
                .unwrap();
        }
        // Close the command queue so run() terminates once drained.
        drop(cmd_tx);

        // Must complete well within the timeout; a blocked poll loop never would.
        let res = timeout(Duration::from_secs(5), poller.run()).await;
        assert!(
            res.is_ok(),
            "Poller::run blocked on a full subscriber channel (keepalive-starvation regression)"
        );
        assert!(res.unwrap().is_ok(), "Poller::run returned an error");

        // Exactly one message made it into the capacity-1 channel; the rest were
        // dropped non-blockingly rather than wedging the loop.
        let mut received = 0;
        while sub_rx.try_recv().is_ok() {
            received += 1;
        }
        assert_eq!(
            received, 1,
            "expected exactly one buffered message in the full subscriber channel"
        );
        assert!(
            overflow.is_cancelled(),
            "a loss-intolerant bounded subscriber must observe overflow"
        );
    }

    #[tokio::test]
    async fn bounded_subscriber_byte_reservations_signal_and_release_on_drop() {
        let (cmd_tx, cmd_rx) = channel::<PollCommand>(10);
        let (sink_tx, _sink_rx) = channel::<Result<Bc>>(1);
        let mut poller = Poller {
            subscribers: Default::default(),
            sink: sink_tx,
            reciever: ReceiverStream::new(cmd_rx),
            last_full_warn: None,
            dropped_full: 0,
        };
        let (sub_tx, sub_rx) = channel::<BcSubscriptionItem>(10);
        let overflow = CancellationToken::new();
        let accounted = bc_envelope_accounted_bytes(&make_binary_bc(42, 7, 3), None) as usize;
        let byte_budget = Arc::new(Semaphore::new(accounted + 2));
        cmd_tx
            .send(PollCommand::AddSubscriber(
                42,
                Some(7),
                SubscriberChannel {
                    sender: sub_tx,
                    overflow: Some(overflow.clone()),
                    byte_budget: Some(byte_budget.clone()),
                },
            ))
            .await
            .unwrap();
        cmd_tx
            .send(PollCommand::Bc(Box::new(unreserved_ingress(Ok(
                make_binary_bc(42, 7, 3),
            )))))
            .await
            .unwrap();
        cmd_tx
            .send(PollCommand::Bc(Box::new(unreserved_ingress(Ok(
                make_binary_bc(42, 7, 3),
            )))))
            .await
            .unwrap();
        drop(cmd_tx);

        timeout(Duration::from_secs(1), poller.run())
            .await
            .expect("poller must not block on byte-budget exhaustion")
            .expect("poller result");
        assert!(overflow.is_cancelled());
        assert_eq!(byte_budget.available_permits(), 2);
        drop(sub_rx);
        assert_eq!(
            byte_budget.available_permits(),
            accounted + 2,
            "dropping the raw queue must release its byte reservation"
        );
    }

    #[tokio::test]
    async fn bounded_subscriber_accounts_xml_extension_and_mixed_envelopes() {
        for response in [
            make_xml_bc(42, 7, 2 * 1024),
            make_extension_bc(42, 7, 2 * 1024),
            make_mixed_bc(42, 7, 2 * 1024),
        ] {
            let accounted = bc_envelope_accounted_bytes(&response, Some(2 * 1024));
            assert_eq!(
                accounted as usize,
                2 * 1024 + std::mem::size_of::<Bc>(),
                "parser-preserved body length must be carried into raw accounting"
            );
            let (sender, mut receiver) = channel::<BcSubscriptionItem>(1);
            let overflow = CancellationToken::new();
            let budget = Arc::new(Semaphore::new(1024));
            let subscriber = SubscriberChannel {
                sender,
                overflow: Some(overflow.clone()),
                byte_budget: Some(budget.clone()),
            };

            assert_eq!(
                subscriber.try_send(Ok(response), Some(accounted)),
                SubscriberSendResult::Full
            );
            assert!(overflow.is_cancelled());
            assert!(receiver.try_recv().is_err());
            assert_eq!(budget.available_permits(), 1024);
        }
    }

    #[tokio::test]
    async fn raw_reservation_releases_on_dequeue_and_failed_enqueue() {
        let response = make_binary_bc(42, 7, 128);
        let accounted = bc_envelope_accounted_bytes(&response, None) as usize;
        let budget = Arc::new(Semaphore::new(accounted));
        let (sender, mut receiver) = channel::<BcSubscriptionItem>(1);
        let subscriber = SubscriberChannel {
            sender,
            overflow: Some(CancellationToken::new()),
            byte_budget: Some(budget.clone()),
        };

        assert_eq!(
            subscriber.try_send(Ok(response), None),
            SubscriberSendResult::Sent
        );
        assert_eq!(budget.available_permits(), 0);
        let item = receiver.recv().await.expect("queued raw envelope");
        assert_eq!(budget.available_permits(), 0);
        let _response = item.into_result().expect("raw envelope result");
        assert_eq!(
            budget.available_permits(),
            accounted,
            "raw dequeue must release the queue reservation"
        );

        drop(receiver);
        assert_eq!(
            subscriber.try_send(Ok(make_binary_bc(42, 7, 128)), None),
            SubscriberSendResult::Closed
        );
        assert_eq!(
            budget.available_permits(),
            accounted,
            "failed raw enqueue must release its attempted reservation"
        );
    }

    #[tokio::test]
    async fn ingress_budget_backpressures_and_releases_on_dequeue_drop() {
        let response = make_binary_bc(42, 7, 128);
        let accounted = bc_envelope_accounted_bytes(&response, None) as usize;
        let budget = Arc::new(Semaphore::new(accounted * 2));
        let first = IngressItem::reserve(Ok(response), None, budget.clone())
            .await
            .unwrap();
        let second = IngressItem::reserve(Ok(make_binary_bc(42, 7, 128)), None, budget.clone())
            .await
            .unwrap();
        assert_eq!(budget.available_permits(), 0);

        let mut third = Box::pin(IngressItem::reserve(
            Ok(make_binary_bc(42, 7, 128)),
            None,
            budget.clone(),
        ));
        assert!(matches!(futures::poll!(&mut third), Poll::Pending));
        drop(first);
        let third = timeout(Duration::from_secs(1), third)
            .await
            .expect("released ingress budget must wake a waiter")
            .unwrap();
        assert_eq!(budget.available_permits(), 0);

        let (sender, mut receiver) = channel::<PollCommand>(1);
        sender
            .send(PollCommand::Bc(Box::new(second)))
            .await
            .unwrap();
        let queued = receiver.recv().await.expect("queued ingress envelope");
        assert_eq!(budget.available_permits(), 0);
        drop(queued);
        assert_eq!(budget.available_permits(), accounted);
        drop(third);
        assert_eq!(budget.available_permits(), accounted * 2);
    }

    #[tokio::test]
    async fn ingress_reservation_releases_on_failed_enqueue_and_cancellation() {
        let response = make_binary_bc(42, 7, 128);
        let accounted = bc_envelope_accounted_bytes(&response, None) as usize;
        let budget = Arc::new(Semaphore::new(accounted));

        let (closed_sender, closed_receiver) = channel::<PollCommand>(1);
        drop(closed_receiver);
        let item = IngressItem::reserve(Ok(response), None, budget.clone())
            .await
            .unwrap();
        assert_eq!(budget.available_permits(), 0);
        assert!(closed_sender
            .send(PollCommand::Bc(Box::new(item)))
            .await
            .is_err());
        assert_eq!(budget.available_permits(), accounted);

        let (blocked_sender, _blocked_receiver) = channel::<PollCommand>(1);
        blocked_sender.send(PollCommand::Disconnect).await.unwrap();
        let mut enqueue = Box::pin(async {
            let item =
                IngressItem::reserve(Ok(make_binary_bc(42, 7, 128)), None, budget.clone()).await?;
            blocked_sender.send(PollCommand::Bc(Box::new(item))).await?;
            Result::Ok(())
        });
        assert!(matches!(futures::poll!(&mut enqueue), Poll::Pending));
        assert_eq!(budget.available_permits(), 0);
        drop(enqueue);
        assert_eq!(
            budget.available_permits(),
            accounted,
            "cancelling an ingress enqueue must release its reservation"
        );
    }

    fn saturated_connection() -> (BcConnection, CancellationToken, Arc<AtomicBool>) {
        let (sink, _sink_rx) = channel::<Result<Bc>>(1);
        let (poll_commander, poll_commanded) = channel(1);
        poll_commander
            .try_send(PollCommand::Disconnect)
            .expect("prefill command queue");

        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let exited = Arc::new(AtomicBool::new(false));
        let task_exited = exited.clone();
        let mut rx_thread = JoinSet::new();
        rx_thread.spawn(async move {
            // Retain the receiver so the prefilled queue is genuinely full.
            let _poll_commanded = poll_commanded;
            task_cancel.cancelled().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            task_exited.store(true, Ordering::SeqCst);
            Ok(())
        });

        (
            BcConnection {
                sink,
                poll_commander,
                rx_thread: RwLock::new(rx_thread),
                cancel: cancel.clone(),
            },
            cancel,
            exited,
        )
    }

    #[tokio::test]
    async fn shutdown_cancels_before_touching_a_saturated_command_queue() {
        let (connection, cancel, exited) = saturated_connection();
        let mut shutdown = Box::pin(connection.shutdown());

        assert!(matches!(futures::poll!(&mut shutdown), Poll::Pending));
        assert!(
            cancel.is_cancelled(),
            "transport cancellation must be synchronous"
        );
        timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("shutdown must remain bounded")
            .expect("shutdown result");
        assert!(exited.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn drop_cancels_before_notifying_a_saturated_command_queue() {
        let (connection, cancel, exited) = saturated_connection();
        drop(connection);

        assert!(
            cancel.is_cancelled(),
            "Drop must synchronously cancel transport tasks"
        );
        timeout(Duration::from_secs(1), async {
            while !exited.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Drop cleanup must remain bounded");
    }
}
