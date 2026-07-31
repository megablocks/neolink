use super::{BcCamera, Error, Result};
use crate::bc::{
    model::*,
    xml::{FileInfo, FileInfoList},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, future::Future, sync::Arc, time::Duration};

pub use crate::bc::xml::FileDateTime;

const FILE_INFO_LIST_VERSION: &str = "1.1";
const FILE_INFO_LIST_HOST_CHANNEL: u8 = 250;
const RECORDING_REPLY_TIMEOUT: Duration = Duration::from_secs(15);
pub(super) const RECORDING_UID_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(15);
const TYPICAL_PAGE_SIZE: usize = 40;

/// Default maximum number of FileInfoList pages requested in one search.
pub const DEFAULT_RECORDING_MAX_PAGES: usize = 50;
/// Hard safety ceiling for FileInfoList pages requested in one search.
pub const HARD_RECORDING_MAX_PAGES: usize = 250;
/// Default maximum number of unique entries returned in one search.
pub const DEFAULT_RECORDING_MAX_ENTRIES: usize = 2_000;
/// Hard safety ceiling for unique entries returned in one search.
pub const HARD_RECORDING_MAX_ENTRIES: usize = 10_000;

/// Recording stream requested from the camera.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum RecordingStreamKind {
    /// Main/high-quality recording stream.
    Main,
    /// Sub/fluent recording stream.
    #[default]
    Sub,
}

impl RecordingStreamKind {
    fn as_protocol_str(self) -> &'static str {
        match self {
            Self::Main => "mainStream",
            Self::Sub => "subStream",
        }
    }
}

/// Limits and filters for a single same-day recording metadata search.
#[derive(Clone, Debug)]
pub struct RecordingSearchOptions {
    /// Logical camera channel.
    pub channel: u8,
    /// Camera-local inclusive start timestamp.
    pub start: FileDateTime,
    /// Camera-local inclusive end timestamp.
    pub end: FileDateTime,
    /// Recording stream to search.
    pub stream: RecordingStreamKind,
    /// Comma-separated recording classes accepted by the camera.
    pub record_types: String,
    /// Maximum page requests for this search.
    pub max_pages: usize,
    /// Maximum unique entries retained for this search.
    pub max_entries: usize,
}

impl Default for RecordingSearchOptions {
    /// Return a valid, bounded full-day placeholder query for 2000-01-01.
    ///
    /// Callers should replace the date/channel fields for their intended search.
    fn default() -> Self {
        Self {
            channel: 0,
            start: FileDateTime {
                year: 2000,
                month: 1,
                day: 1,
                hour: 0,
                minute: 0,
                second: 0,
            },
            end: FileDateTime {
                year: 2000,
                month: 1,
                day: 1,
                hour: 23,
                minute: 59,
                second: 59,
            },
            stream: RecordingStreamKind::Sub,
            record_types:
                "manual, sched, io, md, people, face, vehicle, dog_cat, visitor, other, package"
                    .to_owned(),
            max_pages: DEFAULT_RECORDING_MAX_PAGES,
            max_entries: DEFAULT_RECORDING_MAX_ENTRIES,
        }
    }
}

impl RecordingSearchOptions {
    fn validate(&self) -> Result<()> {
        if !valid_datetime(self.start) || !valid_datetime(self.end) {
            return Err(Error::Other("Invalid recording search timestamp"));
        }
        if (self.start.year, self.start.month, self.start.day)
            != (self.end.year, self.end.month, self.end.day)
        {
            return Err(Error::Other(
                "Recording search start and end must be on the same camera-local day",
            ));
        }
        if self.start > self.end {
            return Err(Error::Other(
                "Recording search start must not be after its end",
            ));
        }
        if self.record_types.trim().is_empty() {
            return Err(Error::Other("Recording search types must not be empty"));
        }
        if self.channel > 31 {
            return Err(Error::Other(
                "Recording search channel must be between 0 and 31",
            ));
        }
        if !(1..=HARD_RECORDING_MAX_PAGES).contains(&self.max_pages) {
            return Err(Error::Other(
                "Recording search max_pages is outside its safety ceiling",
            ));
        }
        if !(1..=HARD_RECORDING_MAX_ENTRIES).contains(&self.max_entries) {
            return Err(Error::Other(
                "Recording search max_entries is outside its safety ceiling",
            ));
        }
        Ok(())
    }
}

/// Why a bounded recording search stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingSearchEnd {
    /// Camera explicitly marked the result complete.
    Finished,
    /// Camera returned fewer entries than a normal full page.
    ShortPage,
    /// Camera returned an explicit empty/end response.
    EndOfResults,
    /// A page repeated only entries already seen.
    Stalled,
    /// Caller-provided page ceiling was reached.
    PageLimit,
    /// Caller-provided unique-entry ceiling was reached.
    EntryLimit,
}

/// One recording metadata entry.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordingEntry {
    /// Camera-provided stable identifier/path, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Camera-provided display name, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Camera-provided filename/path variant, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    /// Camera-provided recording class, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_type: Option<String>,
    /// Camera-provided size in bytes, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Camera-local recording start, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<FileDateTime>,
    /// Camera-local recording end, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<FileDateTime>,
}

impl RecordingEntry {
    fn from_file_info(value: &FileInfo) -> Option<Self> {
        if value.id.is_none() && value.name.is_none() && value.file_name.is_none() {
            return None;
        }
        Some(Self {
            id: value.id.clone(),
            name: value.name.clone(),
            file_name: value.file_name.clone(),
            record_type: value
                .type_
                .clone()
                .or_else(|| value.record_type.clone())
                .or_else(|| value.alarm_type.clone()),
            size_bytes: value.size.or(value.file_size),
            start: value.start_time,
            end: value.end_time,
        })
    }

    fn unique_key(&self) -> &str {
        self.id
            .as_deref()
            .or(self.file_name.as_deref())
            .or(self.name.as_deref())
            .expect("recording entries always have an identifier")
    }
}

/// Bounded recording metadata search result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordingSearchResult {
    /// Unique recording entries.
    pub entries: Vec<RecordingEntry>,
    /// Number of page responses consumed.
    pub pages: usize,
    /// Why pagination stopped.
    pub end: RecordingSearchEnd,
}

impl RecordingSearchResult {
    /// Whether the camera indicated a natural end instead of a safety/stall stop.
    pub fn complete(&self) -> bool {
        matches!(
            self.end,
            RecordingSearchEnd::Finished
                | RecordingSearchEnd::ShortPage
                | RecordingSearchEnd::EndOfResults
        )
    }

    /// Earliest explicit start timestamp among returned entries.
    pub fn earliest(&self) -> Option<FileDateTime> {
        self.entries.iter().filter_map(|entry| entry.start).min()
    }

    /// Latest explicit end timestamp among returned entries.
    pub fn latest(&self) -> Option<FileDateTime> {
        self.entries.iter().filter_map(|entry| entry.end).max()
    }
}

impl std::fmt::Display for RecordingSearchEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Finished => "finished",
            Self::ShortPage => "short_page",
            Self::EndOfResults => "end_of_results",
            Self::Stalled => "stalled",
            Self::PageLimit => "page_limit",
            Self::EntryLimit => "entry_limit",
        };
        f.write_str(value)
    }
}

#[derive(Debug)]
enum FileInfoCommandReply {
    Xml(FileInfoList),
    Empty,
    End,
}

#[derive(Clone)]
struct SearchRequest {
    uid: String,
    options: RecordingSearchOptions,
}

impl BcCamera {
    /// List stored recording metadata without downloading recording content.
    ///
    /// FileInfoList searches are scoped to one camera-local calendar day.
    /// After a successful OPEN, a best-effort CLOSE is attempted on every
    /// completed code path, including pagination errors and configured safety
    /// ceilings. Cancelling the future can interrupt that cleanup.
    ///
    /// A non-empty UID supplied through [`super::BcCameraOpt`] is reused
    /// directly. Otherwise the UID is queried for
    /// [`RecordingSearchOptions::channel`] with a fixed timeout. UID resolution
    /// completes before the camera's stateful recording cursor lock is taken.
    pub async fn search_recordings(
        &self,
        options: RecordingSearchOptions,
    ) -> Result<RecordingSearchResult> {
        search_recordings_with(
            self.configured_uid.as_deref(),
            options,
            &self.recording_search_lock,
            |channel| self.uid_for_channel(channel),
            |msg_id, payload| self.send_file_info_list(msg_id, payload),
        )
        .await
    }

    async fn send_file_info_list(
        &self,
        msg_id: u32,
        file_info_list: FileInfoList,
    ) -> Result<FileInfoCommandReply> {
        let connection = self.get_connection();
        let msg_num = self.new_message_num();
        let mut subscription = connection.subscribe(msg_id, msg_num).await?;
        subscription
            .send(Bc {
                meta: BcMeta {
                    msg_id,
                    channel_id: FILE_INFO_LIST_HOST_CHANNEL,
                    msg_num,
                    response_code: 0,
                    stream_type: 0,
                    class: 0x6414,
                },
                body: BcBody::ModernMsg(ModernMsg {
                    extension: None,
                    payload: Some(BcPayloads::BcXml(BcXml {
                        file_info_list: Some(file_info_list),
                        ..Default::default()
                    })),
                }),
            })
            .await?;

        let reply = tokio::time::timeout(RECORDING_REPLY_TIMEOUT, subscription.recv())
            .await
            .map_err(|_| Error::TimeoutDisconnected)??;
        let response_code = reply.meta.response_code;
        if msg_id == MSG_ID_FILE_INFO_LIST_CLOSE && response_code == 200 {
            return Ok(FileInfoCommandReply::Empty);
        }
        let payload = match reply.body {
            BcBody::ModernMsg(ModernMsg { payload, .. }) => payload,
            _ => {
                return Err(Error::Other(
                    "FileInfoList camera reply was not a modern message",
                ))
            }
        };

        classify_file_info_reply(msg_id, response_code, payload)
    }
}

fn classify_file_info_reply(
    msg_id: u32,
    response_code: u16,
    payload: Option<BcPayloads>,
) -> Result<FileInfoCommandReply> {
    if msg_id == MSG_ID_FILE_INFO_LIST_GET && response_code == 400 && payload.is_none() {
        return Ok(FileInfoCommandReply::End);
    }
    if response_code != 200 {
        return Err(Error::CameraServiceUnavailable {
            id: msg_id,
            code: response_code,
        });
    }
    if msg_id == MSG_ID_FILE_INFO_LIST_CLOSE {
        return Ok(FileInfoCommandReply::Empty);
    }

    match payload {
        Some(BcPayloads::BcXml(BcXml {
            file_info_list: Some(list),
            ..
        })) => Ok(FileInfoCommandReply::Xml(list)),
        None | Some(BcPayloads::BcXml(_)) => Ok(FileInfoCommandReply::Empty),
        Some(BcPayloads::Binary(_)) => Err(Error::Other(
            "FileInfoList camera reply had an unexpected binary payload",
        )),
    }
}

async fn with_recording_lock<T>(
    lock: &tokio::sync::Mutex<()>,
    operation: impl Future<Output = T>,
) -> T {
    let _guard = lock.lock().await;
    operation.await
}

async fn search_recordings_with<Resolve, ResolveFut, Send, SendFut>(
    configured_uid: Option<&str>,
    options: RecordingSearchOptions,
    recording_search_lock: &tokio::sync::Mutex<()>,
    discover_uid: Resolve,
    send: Send,
) -> Result<RecordingSearchResult>
where
    Resolve: FnOnce(u8) -> ResolveFut,
    ResolveFut: Future<Output = Result<String>>,
    Send: FnMut(u32, FileInfoList) -> SendFut,
    SendFut: Future<Output = Result<FileInfoCommandReply>>,
{
    options.validate()?;
    let channel = options.channel;
    let uid = resolve_recording_uid(
        configured_uid,
        channel,
        RECORDING_UID_DISCOVERY_TIMEOUT,
        discover_uid,
    )
    .await?;
    let request = SearchRequest { uid, options };

    with_recording_lock(recording_search_lock, execute_search(request, send)).await
}

pub(super) async fn resolve_recording_uid<Resolve, ResolveFut>(
    configured_uid: Option<&str>,
    channel: u8,
    timeout: Duration,
    discover_uid: Resolve,
) -> Result<String>
where
    Resolve: FnOnce(u8) -> ResolveFut,
    ResolveFut: Future<Output = Result<String>>,
{
    if let Some(uid) = configured_uid.map(str::trim).filter(|uid| !uid.is_empty()) {
        return Ok(uid.to_owned());
    }

    let discovered_uid = tokio::time::timeout(timeout, discover_uid(channel))
        .await
        .map_err(|_| Error::TimeoutDisconnected)??;
    let discovered_uid = discovered_uid.trim();
    if discovered_uid.is_empty() {
        return Err(Error::Other("Camera returned an empty UID"));
    }
    Ok(discovered_uid.to_owned())
}

async fn execute_search<F, Fut>(
    request: SearchRequest,
    mut send: F,
) -> Result<RecordingSearchResult>
where
    F: FnMut(u32, FileInfoList) -> Fut,
    Fut: Future<Output = Result<FileInfoCommandReply>>,
{
    let open_reply = send(MSG_ID_FILE_INFO_LIST_OPEN, build_open_request(&request)).await?;
    let handle = match open_reply {
        FileInfoCommandReply::Xml(list) => find_handle(&list),
        FileInfoCommandReply::Empty | FileInfoCommandReply::End => None,
    }
    .ok_or(Error::Other(
        "FileInfoList open response did not contain a cursor handle",
    ))?;
    let page_request = build_page_request(&request, handle);

    let search_result = paginate(&request.options, &page_request, &mut send).await;
    let close_result = send(MSG_ID_FILE_INFO_LIST_CLOSE, page_request).await;

    match (search_result, close_result) {
        (Ok(result), Ok(FileInfoCommandReply::Xml(_) | FileInfoCommandReply::Empty)) => Ok(result),
        (Ok(_), Ok(FileInfoCommandReply::End)) => Err(Error::RecordingCloseFailed {
            close: Arc::new(Error::Other(
                "FileInfoList close returned an end-of-results response",
            )),
        }),
        (Ok(_), Err(close)) => Err(Error::RecordingCloseFailed {
            close: Arc::new(close),
        }),
        (Err(search), Ok(_)) => Err(search),
        (Err(search), Err(close)) => Err(Error::RecordingSearchAndCloseFailed {
            search: Arc::new(search),
            close: Arc::new(close),
        }),
    }
}

async fn paginate<F, Fut>(
    options: &RecordingSearchOptions,
    page_request: &FileInfoList,
    send: &mut F,
) -> Result<RecordingSearchResult>
where
    F: FnMut(u32, FileInfoList) -> Fut,
    Fut: Future<Output = Result<FileInfoCommandReply>>,
{
    let mut entries = Vec::new();
    let mut seen = HashSet::new();

    for page_index in 0..options.max_pages {
        let response = send(MSG_ID_FILE_INFO_LIST_GET, page_request.clone()).await?;
        let pages = page_index + 1;
        let list = match response {
            FileInfoCommandReply::End => {
                return Ok(RecordingSearchResult {
                    entries,
                    pages,
                    end: RecordingSearchEnd::EndOfResults,
                })
            }
            FileInfoCommandReply::Empty => {
                return Ok(RecordingSearchResult {
                    entries,
                    pages,
                    end: RecordingSearchEnd::ShortPage,
                })
            }
            FileInfoCommandReply::Xml(list) => list,
        };

        let completion = completion_marker(&list);
        let mut page_entries = Vec::new();
        collect_entries(&list, &mut page_entries);
        let raw_page_len = page_entries.len();
        let before = entries.len();

        for entry in page_entries {
            if seen.insert(entry.unique_key().to_owned()) {
                if entries.len() == options.max_entries {
                    return Ok(RecordingSearchResult {
                        entries,
                        pages,
                        end: RecordingSearchEnd::EntryLimit,
                    });
                }
                entries.push(entry);
            }
        }

        if completion == Some(true) {
            return Ok(RecordingSearchResult {
                entries,
                pages,
                end: RecordingSearchEnd::Finished,
            });
        }
        if completion.is_none() && raw_page_len < TYPICAL_PAGE_SIZE {
            return Ok(RecordingSearchResult {
                entries,
                pages,
                end: RecordingSearchEnd::ShortPage,
            });
        }
        if entries.len() == before {
            return Ok(RecordingSearchResult {
                entries,
                pages,
                end: RecordingSearchEnd::Stalled,
            });
        }
    }

    Ok(RecordingSearchResult {
        entries,
        pages: options.max_pages,
        end: RecordingSearchEnd::PageLimit,
    })
}

fn build_open_request(request: &SearchRequest) -> FileInfoList {
    FileInfoList {
        version: Some(FILE_INFO_LIST_VERSION.to_owned()),
        file_info: vec![FileInfo {
            uid: Some(request.uid.trim().to_owned()),
            search_ai_track: Some(1),
            channel_id: Some(request.options.channel),
            logic_chn_bitmap: Some(255),
            stream_type: Some(request.options.stream.as_protocol_str().to_owned()),
            record_type: Some(request.options.record_types.clone()),
            start_time: Some(request.options.start),
            end_time: Some(request.options.end),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn build_page_request(request: &SearchRequest, handle: u32) -> FileInfoList {
    FileInfoList {
        version: Some(FILE_INFO_LIST_VERSION.to_owned()),
        file_info: vec![FileInfo {
            uid: Some(request.uid.trim().to_owned()),
            search_ai_track: Some(1),
            channel_id: Some(request.options.channel),
            handle: Some(handle),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn find_handle(list: &FileInfoList) -> Option<u32> {
    list.handle.or_else(|| {
        list.file_info
            .iter()
            .chain(list.file.iter())
            .find_map(find_handle_in_entry)
    })
}

fn find_handle_in_entry(entry: &FileInfo) -> Option<u32> {
    entry.handle.or_else(|| {
        entry
            .file
            .iter()
            .find_map(find_handle_in_entry)
            .or_else(|| {
                entry
                    .file_list
                    .as_ref()
                    .or(entry.file_list_upper.as_ref())
                    .and_then(|nested| {
                        nested
                            .file
                            .iter()
                            .chain(nested.file_info.iter())
                            .find_map(find_handle_in_entry)
                    })
            })
    })
}

fn collect_entries(list: &FileInfoList, entries: &mut Vec<RecordingEntry>) {
    for entry in list.file_info.iter().chain(list.file.iter()) {
        collect_entry(entry, entries);
    }
}

fn collect_entry(value: &FileInfo, entries: &mut Vec<RecordingEntry>) {
    if let Some(entry) = RecordingEntry::from_file_info(value) {
        entries.push(entry);
    }
    for child in &value.file {
        collect_entry(child, entries);
    }
    for list in [value.file_list.as_ref(), value.file_list_upper.as_ref()]
        .into_iter()
        .flatten()
    {
        for child in list.file.iter().chain(list.file_info.iter()) {
            collect_entry(child, entries);
        }
    }
}

fn completion_marker(list: &FileInfoList) -> Option<bool> {
    let mut state = None;
    merge_completion_marker(&mut state, list.b_finished);
    merge_completion_marker(&mut state, list.finished);
    for entry in list.file_info.iter().chain(list.file.iter()) {
        merge_entry_completion_marker(&mut state, entry);
    }
    state
}

fn merge_entry_completion_marker(state: &mut Option<bool>, entry: &FileInfo) {
    merge_completion_marker(state, entry.b_finished);
    merge_completion_marker(state, entry.finished);
    for child in &entry.file {
        merge_entry_completion_marker(state, child);
    }
    for list in [entry.file_list.as_ref(), entry.file_list_upper.as_ref()]
        .into_iter()
        .flatten()
    {
        merge_completion_marker(state, list.b_finished);
        merge_completion_marker(state, list.finished);
        for child in list.file.iter().chain(list.file_info.iter()) {
            merge_entry_completion_marker(state, child);
        }
    }
}

fn merge_completion_marker(state: &mut Option<bool>, marker: Option<u8>) {
    if let Some(marker) = marker {
        let finished = marker == 1;
        if finished || state.is_none() {
            *state = Some(finished);
        }
    }
}

fn valid_datetime(value: FileDateTime) -> bool {
    value.year >= 2000
        && (1..=12).contains(&value.month)
        && (1..=days_in_month(value.year, value.month)).contains(&value.day)
        && value.hour <= 23
        && value.minute <= 59
        && value.second <= 59
}

fn days_in_month(year: u16, month: u8) -> u8 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if year.rem_euclid(400) == 0
            || (year.rem_euclid(4) == 0 && year.rem_euclid(100) != 0) =>
        {
            29
        }
        2 => 28,
        _ => 31,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bc::xml::FileResultList;
    use std::{
        cell::RefCell,
        collections::VecDeque,
        future::{pending, ready},
        rc::Rc,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    fn timestamp(hour: u8) -> FileDateTime {
        FileDateTime {
            year: 2026,
            month: 1,
            day: 2,
            hour,
            minute: 0,
            second: 0,
        }
    }

    fn options() -> RecordingSearchOptions {
        RecordingSearchOptions {
            channel: 0,
            start: timestamp(0),
            end: FileDateTime {
                hour: 23,
                minute: 59,
                second: 59,
                ..timestamp(0)
            },
            stream: RecordingStreamKind::Sub,
            record_types: "md, people".to_owned(),
            max_pages: 4,
            max_entries: 200,
        }
    }

    fn request() -> SearchRequest {
        SearchRequest {
            uid: "  FIXTUREUID  ".to_owned(),
            options: options(),
        }
    }

    fn open_reply() -> FileInfoCommandReply {
        FileInfoCommandReply::Xml(FileInfoList {
            version: Some("1.1".to_owned()),
            file_info: vec![FileInfo {
                handle: Some(17),
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    fn page_with_marker(
        first: usize,
        count: usize,
        completion_marker: Option<u8>,
    ) -> FileInfoCommandReply {
        FileInfoCommandReply::Xml(FileInfoList {
            version: Some("1.1".to_owned()),
            file_info: (first..first + count)
                .map(|index| FileInfo {
                    id: Some(format!("/fixture/recording-{index}.mp4")),
                    name: Some(format!("recording-{index}.mp4")),
                    start_time: Some(timestamp((index % 24) as u8)),
                    end_time: Some(FileDateTime {
                        minute: 1,
                        ..timestamp((index % 24) as u8)
                    }),
                    ..Default::default()
                })
                .collect(),
            b_finished: completion_marker,
            ..Default::default()
        })
    }

    fn page(first: usize, count: usize, finished: bool) -> FileInfoCommandReply {
        page_with_marker(first, count, finished.then_some(1))
    }

    async fn run_mock(
        request: SearchRequest,
        replies: Vec<Result<FileInfoCommandReply>>,
    ) -> (Result<RecordingSearchResult>, Vec<u32>) {
        let replies = Rc::new(RefCell::new(VecDeque::from(replies)));
        let calls = Rc::new(RefCell::new(Vec::new()));
        let result = execute_search(request, {
            let replies = replies.clone();
            let calls = calls.clone();
            move |msg_id, _payload| {
                calls.borrow_mut().push(msg_id);
                ready(
                    replies
                        .borrow_mut()
                        .pop_front()
                        .expect("mock reply for every command"),
                )
            }
        })
        .await;
        let calls = calls.borrow().clone();
        (result, calls)
    }

    #[test]
    fn query_validation_rejects_bad_dates_and_limits() {
        assert!(RecordingSearchOptions::default().validate().is_ok());

        let mut value = options();
        value.end.day = 31;
        assert!(value.validate().is_err());

        let mut value = options();
        value.max_pages = HARD_RECORDING_MAX_PAGES + 1;
        assert!(value.validate().is_err());

        let mut value = options();
        value.max_entries = 0;
        assert!(value.validate().is_err());

        let mut value = options();
        value.channel = 32;
        assert!(value.validate().is_err());
    }

    #[test]
    fn open_request_contains_channel_date_and_uid() {
        let request = request();
        let open = build_open_request(&request);
        let info = &open.file_info[0];
        assert_eq!(info.uid.as_deref(), Some("FIXTUREUID"));
        assert_eq!(info.channel_id, Some(0));
        assert_eq!(info.stream_type.as_deref(), Some("subStream"));
        assert_eq!(info.start_time, Some(timestamp(0)));

        let page = build_page_request(&request, 17);
        assert_eq!(page.file_info[0].uid.as_deref(), Some("FIXTUREUID"));
    }

    #[tokio::test]
    async fn configured_uid_is_trimmed_and_avoids_discovery() {
        let discovery_calls = AtomicUsize::new(0);
        let uid = resolve_recording_uid(
            Some("  CONFIGUREDUID  "),
            9,
            Duration::from_millis(1),
            |_: u8| {
                discovery_calls.fetch_add(1, Ordering::SeqCst);
                ready(Ok("DISCOVEREDUID".to_owned()))
            },
        )
        .await
        .unwrap();

        assert_eq!(uid, "CONFIGUREDUID");
        assert_eq!(discovery_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn uid_discovery_uses_query_channel_and_trims_reply() {
        let resolved_channel = AtomicUsize::new(usize::MAX);
        let uid = resolve_recording_uid(None, 17, Duration::from_secs(1), |channel| {
            resolved_channel.store(usize::from(channel), Ordering::SeqCst);
            ready(Ok("  DISCOVEREDUID  ".to_owned()))
        })
        .await
        .unwrap();

        assert_eq!(uid, "DISCOVEREDUID");
        assert_eq!(resolved_channel.load(Ordering::SeqCst), 17);
    }

    #[tokio::test]
    async fn uid_discovery_is_bounded_and_preserves_camera_errors() {
        let timed_out = resolve_recording_uid(None, 3, Duration::from_millis(1), |_| {
            pending::<Result<String>>()
        })
        .await;
        assert!(matches!(timed_out, Err(Error::TimeoutDisconnected)));

        let camera_error = resolve_recording_uid(None, 3, Duration::from_secs(1), |_| {
            ready(Err(Error::CameraServiceUnavailable {
                id: MSG_ID_UID,
                code: 500,
            }))
        })
        .await;
        assert!(matches!(
            camera_error,
            Err(Error::CameraServiceUnavailable {
                id: MSG_ID_UID,
                code: 500
            })
        ));
    }

    #[tokio::test]
    async fn uid_discovery_completes_before_cursor_lock_is_taken() {
        let lock = tokio::sync::Mutex::new(());
        let discovery_started = Rc::new(tokio::sync::Notify::new());
        let release_discovery = Rc::new(tokio::sync::Notify::new());
        let replies = Rc::new(RefCell::new(VecDeque::from(vec![
            Ok(open_reply()),
            Ok(FileInfoCommandReply::Empty),
            Ok(FileInfoCommandReply::Empty),
        ])));

        let search = search_recordings_with(
            None,
            options(),
            &lock,
            {
                let discovery_started = discovery_started.clone();
                let release_discovery = release_discovery.clone();
                move |channel| async move {
                    assert_eq!(channel, 0);
                    discovery_started.notify_one();
                    release_discovery.notified().await;
                    Ok("DISCOVEREDUID".to_owned())
                }
            },
            {
                let replies = replies.clone();
                move |_msg_id, _payload| {
                    ready(
                        replies
                            .borrow_mut()
                            .pop_front()
                            .expect("mock reply for every command"),
                    )
                }
            },
        );
        let lock_probe = async {
            discovery_started.notified().await;
            let guard = tokio::time::timeout(Duration::from_millis(100), lock.lock())
                .await
                .expect("UID discovery must not hold the recording cursor lock");
            drop(guard);
            release_discovery.notify_one();
        };

        let (result, ()) = tokio::join!(search, lock_probe);
        let result = result.unwrap();
        assert!(result.entries.is_empty());
        assert_eq!(result.end, RecordingSearchEnd::ShortPage);
        assert!(replies.borrow().is_empty());
    }

    #[test]
    fn top_level_cursor_and_nested_completion_markers_are_supported() {
        let open = FileInfoList {
            handle: Some(23),
            ..Default::default()
        };
        assert_eq!(find_handle(&open), Some(23));

        let page = FileInfoList {
            file_info: vec![FileInfo {
                file_list: Some(FileResultList {
                    b_finished: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(completion_marker(&page), Some(true));
    }

    #[test]
    fn generic_success_get_is_empty_and_any_success_close_body_is_accepted() {
        let generic = BcXml::try_parse(
            include_bytes!("../bc/samples/file_info_list_generic_empty.xml").as_slice(),
        )
        .unwrap();
        assert!(matches!(
            classify_file_info_reply(
                MSG_ID_FILE_INFO_LIST_GET,
                200,
                Some(BcPayloads::BcXml(generic))
            ),
            Ok(FileInfoCommandReply::Empty)
        ));
        assert!(matches!(
            classify_file_info_reply(MSG_ID_FILE_INFO_LIST_GET, 200, None),
            Ok(FileInfoCommandReply::Empty)
        ));
        assert!(matches!(
            classify_file_info_reply(
                MSG_ID_FILE_INFO_LIST_CLOSE,
                200,
                Some(BcPayloads::Binary(vec![1, 2, 3]))
            ),
            Ok(FileInfoCommandReply::Empty)
        ));
    }

    #[tokio::test]
    async fn recording_lock_serializes_cursor_operations() {
        let lock = tokio::sync::Mutex::new(());
        let active = AtomicUsize::new(0);
        let overlapped = AtomicBool::new(false);

        let first = with_recording_lock(&lock, async {
            if active.fetch_add(1, Ordering::SeqCst) != 0 {
                overlapped.store(true, Ordering::SeqCst);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            active.fetch_sub(1, Ordering::SeqCst);
        });
        let second = with_recording_lock(&lock, async {
            if active.fetch_add(1, Ordering::SeqCst) != 0 {
                overlapped.store(true, Ordering::SeqCst);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            active.fetch_sub(1, Ordering::SeqCst);
        });

        tokio::join!(first, second);
        assert!(!overlapped.load(Ordering::SeqCst));
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn recording_type_prefers_type_then_record_type_then_alarm_type() {
        let entry = RecordingEntry::from_file_info(&FileInfo {
            type_: Some("sched".to_owned()),
            record_type: Some("people".to_owned()),
            alarm_type: Some("vehicle".to_owned()),
            id: Some("/fixture/preference.mp4".to_owned()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(entry.record_type.as_deref(), Some("sched"));

        let entry = RecordingEntry::from_file_info(&FileInfo {
            record_type: Some("people".to_owned()),
            alarm_type: Some("vehicle".to_owned()),
            id: Some("/fixture/fallback.mp4".to_owned()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(entry.record_type.as_deref(), Some("people"));
    }

    #[tokio::test]
    async fn empty_page_stops_and_closes_cursor() {
        let (result, calls) = run_mock(
            request(),
            vec![
                Ok(open_reply()),
                Ok(FileInfoCommandReply::Xml(FileInfoList::default())),
                Ok(FileInfoCommandReply::Empty),
            ],
        )
        .await;
        let result = result.unwrap();
        assert!(result.entries.is_empty());
        assert_eq!(result.pages, 1);
        assert_eq!(result.end, RecordingSearchEnd::ShortPage);
        assert_eq!(
            calls,
            vec![
                MSG_ID_FILE_INFO_LIST_OPEN,
                MSG_ID_FILE_INFO_LIST_GET,
                MSG_ID_FILE_INFO_LIST_CLOSE
            ]
        );
    }

    #[tokio::test]
    async fn duplicate_full_page_stops_as_stalled() {
        let (result, calls) = run_mock(
            request(),
            vec![
                Ok(open_reply()),
                Ok(page(0, TYPICAL_PAGE_SIZE, false)),
                Ok(page(0, TYPICAL_PAGE_SIZE, false)),
                Ok(FileInfoCommandReply::Empty),
            ],
        )
        .await;
        let result = result.unwrap();
        assert_eq!(result.entries.len(), TYPICAL_PAGE_SIZE);
        assert_eq!(result.pages, 2);
        assert_eq!(result.end, RecordingSearchEnd::Stalled);
        assert_eq!(calls.last(), Some(&MSG_ID_FILE_INFO_LIST_CLOSE));
    }

    #[tokio::test]
    async fn unfinished_page_continues_until_explicit_end() {
        let (result, calls) = run_mock(
            request(),
            vec![
                Ok(open_reply()),
                Ok(page(0, TYPICAL_PAGE_SIZE, false)),
                Ok(FileInfoCommandReply::End),
                Ok(FileInfoCommandReply::Empty),
            ],
        )
        .await;
        let result = result.unwrap();
        assert_eq!(result.entries.len(), TYPICAL_PAGE_SIZE);
        assert_eq!(result.pages, 2);
        assert_eq!(result.end, RecordingSearchEnd::EndOfResults);
        assert_eq!(calls.last(), Some(&MSG_ID_FILE_INFO_LIST_CLOSE));
    }

    #[tokio::test]
    async fn explicit_unfinished_short_page_continues_until_finished_marker() {
        let (result, calls) = run_mock(
            request(),
            vec![
                Ok(open_reply()),
                Ok(page_with_marker(0, 1, Some(0))),
                Ok(page_with_marker(1, 1, Some(1))),
                Ok(FileInfoCommandReply::Empty),
            ],
        )
        .await;
        let result = result.unwrap();
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.pages, 2);
        assert_eq!(result.end, RecordingSearchEnd::Finished);
        assert_eq!(calls.last(), Some(&MSG_ID_FILE_INFO_LIST_CLOSE));
    }

    #[tokio::test]
    async fn page_ceiling_is_bounded_and_cursor_is_closed() {
        let mut request = request();
        request.options.max_pages = 2;
        let (result, calls) = run_mock(
            request,
            vec![
                Ok(open_reply()),
                Ok(page(0, TYPICAL_PAGE_SIZE, false)),
                Ok(page(TYPICAL_PAGE_SIZE, TYPICAL_PAGE_SIZE, false)),
                Ok(FileInfoCommandReply::Empty),
            ],
        )
        .await;
        let result = result.unwrap();
        assert_eq!(result.entries.len(), TYPICAL_PAGE_SIZE * 2);
        assert_eq!(result.end, RecordingSearchEnd::PageLimit);
        assert_eq!(calls.last(), Some(&MSG_ID_FILE_INFO_LIST_CLOSE));
    }

    #[tokio::test]
    async fn entry_ceiling_is_bounded_and_cursor_is_closed() {
        let mut request = request();
        request.options.max_entries = 3;
        let (result, calls) = run_mock(
            request,
            vec![
                Ok(open_reply()),
                Ok(page(0, TYPICAL_PAGE_SIZE, false)),
                Ok(FileInfoCommandReply::Empty),
            ],
        )
        .await;
        let result = result.unwrap();
        assert_eq!(result.entries.len(), 3);
        assert_eq!(result.end, RecordingSearchEnd::EntryLimit);
        assert_eq!(calls.last(), Some(&MSG_ID_FILE_INFO_LIST_CLOSE));
    }

    #[tokio::test]
    async fn get_error_still_closes_cursor() {
        let (result, calls) = run_mock(
            request(),
            vec![
                Ok(open_reply()),
                Err(Error::Other("fixture GET error")),
                Ok(FileInfoCommandReply::Empty),
            ],
        )
        .await;
        assert!(matches!(result, Err(Error::Other("fixture GET error"))));
        assert_eq!(calls.last(), Some(&MSG_ID_FILE_INFO_LIST_CLOSE));
    }

    #[tokio::test]
    async fn close_error_after_success_is_reported() {
        let (result, calls) = run_mock(
            request(),
            vec![
                Ok(open_reply()),
                Ok(page(0, 1, true)),
                Err(Error::Other("fixture CLOSE error")),
            ],
        )
        .await;
        let error = result.unwrap_err();
        assert!(matches!(&error, Error::RecordingCloseFailed { .. }));
        assert!(error.to_string().contains("fixture CLOSE error"));
        assert!(std::error::Error::source(&error).is_some());
        assert_eq!(calls.last(), Some(&MSG_ID_FILE_INFO_LIST_CLOSE));
    }

    #[tokio::test]
    async fn search_and_close_errors_are_both_preserved() {
        let (result, calls) = run_mock(
            request(),
            vec![
                Ok(open_reply()),
                Err(Error::Other("fixture GET error")),
                Err(Error::Other("fixture CLOSE error")),
            ],
        )
        .await;
        let error = result.unwrap_err();
        assert!(matches!(
            &error,
            Error::RecordingSearchAndCloseFailed { .. }
        ));
        let display = error.to_string();
        assert!(display.contains("fixture GET error"));
        assert!(display.contains("fixture CLOSE error"));
        assert!(std::error::Error::source(&error)
            .unwrap()
            .to_string()
            .contains("fixture GET error"));
        assert_eq!(calls.last(), Some(&MSG_ID_FILE_INFO_LIST_CLOSE));
    }
}
