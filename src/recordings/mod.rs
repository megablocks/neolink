use anyhow::{bail, Context, Result};
use neolink_core::{
    bc::xml::FileDateTime,
    bc_protocol::{RecordingSearchOptions, RecordingSearchResult, RecordingStreamKind},
};
use serde_json::json;

use crate::common::NeoReactor;

mod cmdline;
use cmdline::CmdStream;
pub(crate) use cmdline::Opt;

/// Run one bounded, read-only FileInfoList recording metadata search.
pub(crate) async fn main(opt: Opt, reactor: NeoReactor) -> Result<()> {
    let (year, month, day) = parse_date(&opt.date)?;
    let camera = reactor.get(&opt.camera).await?;
    let camera_config = camera.config().await?.borrow().clone();
    let channel = opt.channel.unwrap_or(camera_config.channel_id);
    if channel > 31 {
        bail!("Recording channel must be between 0 and 31");
    }

    let options = RecordingSearchOptions {
        channel,
        start: FileDateTime {
            year,
            month,
            day,
            hour: 0,
            minute: 0,
            second: 0,
        },
        end: FileDateTime {
            year,
            month,
            day,
            hour: 23,
            minute: 59,
            second: 59,
        },
        stream: match opt.stream {
            CmdStream::Main => RecordingStreamKind::Main,
            CmdStream::Sub => RecordingStreamKind::Sub,
        },
        max_pages: opt.max_pages,
        max_entries: opt.max_entries,
        ..Default::default()
    };

    let result = camera
        .run_task(|cam| {
            let options = options.clone();
            Box::pin(async move { Ok(cam.search_recordings(options).await?) })
        })
        .await
        .context("Unable to list recording metadata")?;

    if opt.json {
        print_json(&opt.camera, channel, &opt.date, result)?;
    } else {
        print_summary(&opt.camera, channel, &opt.date, &result);
    }
    Ok(())
}

fn print_summary(camera: &str, channel: u8, date: &str, result: &RecordingSearchResult) {
    println!(
        "recordings camera={camera} channel={channel} date={date} count={} pages={} complete={} end={} earliest={} latest={}",
        result.entries.len(),
        result.pages,
        result.complete(),
        result.end,
        format_time(result.earliest()),
        format_time(result.latest())
    );
}

fn print_json(camera: &str, channel: u8, date: &str, result: RecordingSearchResult) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string(&json_value(camera, channel, date, result))?
    );
    Ok(())
}

fn json_value(
    camera: &str,
    channel: u8,
    date: &str,
    result: RecordingSearchResult,
) -> serde_json::Value {
    json!({
        "camera": camera,
        "channel": channel,
        "date": date,
        "count": result.entries.len(),
        "pages": result.pages,
        "complete": result.complete(),
        "end": result.end,
        "earliest": result.earliest(),
        "latest": result.latest(),
        "entries": result.entries,
    })
}

fn format_time(value: Option<FileDateTime>) -> String {
    match value {
        Some(value) => format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            value.year, value.month, value.day, value.hour, value.minute, value.second
        ),
        None => "none".to_owned(),
    }
}

fn parse_date(input: &str) -> Result<(u16, u8, u8)> {
    let mut parts = input.split('-');
    let year = parts
        .next()
        .context("Date must be YYYY-MM-DD")?
        .parse::<u16>()
        .context("Invalid date year")?;
    let month = parts
        .next()
        .context("Date must be YYYY-MM-DD")?
        .parse::<u8>()
        .context("Invalid date month")?;
    let day = parts
        .next()
        .context("Date must be YYYY-MM-DD")?
        .parse::<u8>()
        .context("Invalid date day")?;
    if parts.next().is_some()
        || year < 2000
        || !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
    {
        bail!("Date must be a valid YYYY-MM-DD value");
    }
    Ok((year, month, day))
}

// Keep the arithmetic form compatible with Neolink's existing Rust 2021 toolchain.
#[allow(clippy::manual_is_multiple_of)]
fn days_in_month(year: u16, month: u8) -> u8 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if year % 400 == 0 || (year % 4 == 0 && year % 100 != 0) => 29,
        2 => 28,
        _ => 31,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use neolink_core::bc_protocol::{RecordingEntry, RecordingSearchEnd};

    #[test]
    fn date_parser_handles_leap_years() {
        assert_eq!(parse_date("2024-02-29").unwrap(), (2024, 2, 29));
        assert!(parse_date("2026-02-29").is_err());
    }

    #[test]
    fn date_parser_rejects_malformed_values() {
        assert!(parse_date("2026-07").is_err());
        assert!(parse_date("2026-13-01").is_err());
        assert!(parse_date("anything").is_err());
    }

    #[test]
    fn json_output_excludes_credentials_uid_and_raw_xml() {
        let result = RecordingSearchResult {
            entries: vec![RecordingEntry {
                id: Some("fixture-id".to_owned()),
                name: Some("fixture-name".to_owned()),
                file_name: Some("/fixture/clip.mp4".to_owned()),
                record_type: Some("md".to_owned()),
                size_bytes: Some(123),
                start: None,
                end: None,
            }],
            pages: 1,
            end: RecordingSearchEnd::Finished,
        };
        let output =
            serde_json::to_string(&json_value("fixture-camera", 0, "2026-01-02", result)).unwrap();

        assert!(!output.contains("password"));
        assert!(!output.contains("username"));
        assert!(!output.contains("uid"));
        assert!(!output.contains("rawXml"));
        assert!(!output.contains("<FileInfoList"));
    }
}
