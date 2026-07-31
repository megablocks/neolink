use anyhow::{bail, Context, Result};
use neolink_core::bc_protocol::{
    FileDateTime, RecordingSearchOptions, RecordingSearchResult, RecordingStreamKind,
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

    let options = build_search_options(
        channel,
        (year, month, day),
        &opt.from,
        &opt.until,
        opt.stream,
        opt.max_pages,
        opt.max_entries,
    )?;

    let result = camera
        .run_task(|cam| {
            let options = options.clone();
            Box::pin(async move { Ok(cam.search_recordings(options).await?) })
        })
        .await
        .context("Unable to list recording metadata")?;

    if opt.json {
        print_json(
            &opt.camera,
            channel,
            &opt.date,
            &opt.from,
            &opt.until,
            result,
        )?;
    } else {
        print_summary(
            &opt.camera,
            channel,
            &opt.date,
            &opt.from,
            &opt.until,
            &result,
        );
    }
    Ok(())
}

fn print_summary(
    camera: &str,
    channel: u8,
    date: &str,
    from: &str,
    until: &str,
    result: &RecordingSearchResult,
) {
    println!(
        "recordings camera={camera} channel={channel} date={date} from={from} until={until} count={} pages={} complete={} end={} earliest={} latest={}",
        result.entries.len(),
        result.pages,
        result.complete(),
        result.end,
        format_time(result.earliest()),
        format_time(result.latest())
    );
}

fn print_json(
    camera: &str,
    channel: u8,
    date: &str,
    from: &str,
    until: &str,
    result: RecordingSearchResult,
) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string(&json_value(camera, channel, date, from, until, result))?
    );
    Ok(())
}

fn json_value(
    camera: &str,
    channel: u8,
    date: &str,
    from: &str,
    until: &str,
    result: RecordingSearchResult,
) -> serde_json::Value {
    json!({
        "camera": camera,
        "channel": channel,
        "date": date,
        "from": from,
        "until": until,
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

fn parse_time(input: &str) -> Result<(u8, u8, u8)> {
    let bytes = input.as_bytes();
    if bytes.len() != 8
        || bytes[2] != b':'
        || bytes[5] != b':'
        || ![0, 1, 3, 4, 6, 7]
            .into_iter()
            .all(|index| bytes[index].is_ascii_digit())
    {
        bail!("Time must be an exact HH:MM:SS value");
    }
    let value = |tens: usize| (bytes[tens] - b'0') * 10 + (bytes[tens + 1] - b'0');
    let (hour, minute, second) = (value(0), value(3), value(6));
    if hour > 23 || minute > 59 || second > 59 {
        bail!("Time must be an exact HH:MM:SS value");
    }
    Ok((hour, minute, second))
}

fn parse_window(
    year: u16,
    month: u8,
    day: u8,
    from: &str,
    until: &str,
) -> Result<(FileDateTime, FileDateTime)> {
    let (start_hour, start_minute, start_second) = parse_time(from)?;
    let (end_hour, end_minute, end_second) = parse_time(until)?;
    let start = FileDateTime {
        year,
        month,
        day,
        hour: start_hour,
        minute: start_minute,
        second: start_second,
    };
    let end = FileDateTime {
        year,
        month,
        day,
        hour: end_hour,
        minute: end_minute,
        second: end_second,
    };
    if start > end {
        bail!("--from must not be after --until");
    }
    Ok((start, end))
}

fn build_search_options(
    channel: u8,
    date: (u16, u8, u8),
    from: &str,
    until: &str,
    stream: CmdStream,
    max_pages: usize,
    max_entries: usize,
) -> Result<RecordingSearchOptions> {
    let (start, end) = parse_window(date.0, date.1, date.2, from, until)?;
    Ok(RecordingSearchOptions {
        channel,
        start,
        end,
        stream: match stream {
            CmdStream::Main => RecordingStreamKind::Main,
            CmdStream::Sub => RecordingStreamKind::Sub,
        },
        max_pages,
        max_entries,
        ..Default::default()
    })
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
    use clap::Parser;
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
    fn time_parser_accepts_exact_day_bounds_and_rejects_non_exact_values() {
        assert_eq!(parse_time("00:00:00").unwrap(), (0, 0, 0));
        assert_eq!(parse_time("23:59:59").unwrap(), (23, 59, 59));
        for value in [
            "0:00:00", "00:0:00", "00:00:0", "24:00:00", "23:60:00", "23:59:60", "00-00-00",
            "abcdefgh", "éééé",
        ] {
            assert!(
                parse_time(value).is_err(),
                "accepted invalid time {value:?}"
            );
        }
    }

    #[test]
    fn window_parser_binds_both_times_to_the_same_date_and_orders_them() {
        let (start, end) = parse_window(2024, 2, 29, "01:02:03", "04:05:06").unwrap();
        assert_eq!(
            start,
            FileDateTime {
                year: 2024,
                month: 2,
                day: 29,
                hour: 1,
                minute: 2,
                second: 3,
            }
        );
        assert_eq!(
            end,
            FileDateTime {
                year: 2024,
                month: 2,
                day: 29,
                hour: 4,
                minute: 5,
                second: 6,
            }
        );
        assert!(parse_window(2026, 7, 31, "12:00:01", "12:00:00").is_err());
    }

    #[test]
    fn cli_defaults_to_full_day_and_accepts_exact_window_flags() {
        let defaults =
            Opt::try_parse_from(["recordings", "fixture-camera", "--date", "2026-07-31"]).unwrap();
        assert_eq!(defaults.from, "00:00:00");
        assert_eq!(defaults.until, "23:59:59");

        let bounded = Opt::try_parse_from([
            "recordings",
            "fixture-camera",
            "--date",
            "2026-07-31",
            "--from",
            "01:02:03",
            "--until",
            "04:05:06",
        ])
        .unwrap();
        assert_eq!(bounded.from, "01:02:03");
        assert_eq!(bounded.until, "04:05:06");
    }

    #[test]
    fn protocol_options_receive_the_exact_same_day_window_and_bounds() {
        let options = build_search_options(
            7,
            (2024, 2, 29),
            "01:02:03",
            "04:05:06",
            CmdStream::Main,
            128,
            4_096,
        )
        .unwrap();
        assert_eq!(options.channel, 7);
        assert_eq!(options.start.year, 2024);
        assert_eq!(options.start.month, 2);
        assert_eq!(options.start.day, 29);
        assert_eq!(options.start.hour, 1);
        assert_eq!(options.start.minute, 2);
        assert_eq!(options.start.second, 3);
        assert_eq!(options.end.year, 2024);
        assert_eq!(options.end.month, 2);
        assert_eq!(options.end.day, 29);
        assert_eq!(options.end.hour, 4);
        assert_eq!(options.end.minute, 5);
        assert_eq!(options.end.second, 6);
        assert_eq!(options.stream, RecordingStreamKind::Main);
        assert_eq!(options.max_pages, 128);
        assert_eq!(options.max_entries, 4_096);
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
        let output = serde_json::to_string(&json_value(
            "fixture-camera",
            0,
            "2026-01-02",
            "01:02:03",
            "04:05:06",
            result,
        ))
        .unwrap();

        assert!(!output.contains("password"));
        assert!(!output.contains("username"));
        assert!(!output.contains("uid"));
        assert!(!output.contains("rawXml"));
        assert!(!output.contains("<FileInfoList"));
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["from"], "01:02:03");
        assert_eq!(parsed["until"], "04:05:06");
    }
}
