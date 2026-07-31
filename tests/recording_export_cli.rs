#![cfg(feature = "gstreamer")]

use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn export_error_keeps_stdout_empty_and_private_entry_off_stderr() {
    let sentinel = "RecM01_20260730_010203_PRIVATE_SENTINEL.mp4";
    let mut child = Command::new(env!("CARGO_BIN_EXE_neolink"))
        .args([
            "--config",
            "/definitely/not/a/neolink/config.toml",
            "recording-export",
            "fixture-camera",
        ])
        .env("RUST_LOG", "trace")
        .env("GST_DEBUG", "9")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn recording-export fixture");
    write!(
        child.stdin.take().expect("stdin pipe"),
        "{{\"id\":\"/private/{sentinel}\"}}"
    )
    .expect("write RecordingEntry fixture");
    let output = child.wait_with_output().expect("wait for fixture process");

    assert!(!output.status.success());
    assert!(
        output.stdout.is_empty(),
        "binary stdout must remain uncontaminated"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains(sentinel));
    assert!(!stderr.contains("/private/"));
    assert!(!stderr.to_ascii_lowercase().contains("password"));
    assert!(!stderr.to_ascii_lowercase().contains("uid"));
    assert_eq!(stderr, "unable to read Neolink configuration\n");
}

#[test]
fn export_connection_setup_never_discloses_config_secrets() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let config_path = std::env::temp_dir().join(format!(
        "neolink-recording-export-redaction-{}-{nonce}.toml",
        std::process::id()
    ));
    let uid = "PRIVATE_UID_SENTINEL_927";
    let username = "PRIVATE_USER_SENTINEL_462";
    let password = "PRIVATE_PASSWORD_SENTINEL_815";
    fs::write(
        &config_path,
        format!(
            r#"[[cameras]]
name = "fixture-camera"
address = "definitely not a valid camera address"
uid = "{uid}"
username = "{username}"
password = "{password}"
"#
        ),
    )
    .expect("write redaction fixture config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_neolink"))
        .args([
            "--config",
            config_path.to_str().expect("UTF-8 temp path"),
            "recording-export",
            "fixture-camera",
        ])
        .env("RUST_LOG", "trace")
        .env("GST_DEBUG", "9")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn redaction fixture");
    write!(
        child.stdin.take().expect("stdin pipe"),
        "{{\"id\":\"/private/RecM01_20260730_010203_ENTRY_SENTINEL.mp4\"}}"
    )
    .expect("write RecordingEntry fixture");
    let output = child
        .wait_with_output()
        .expect("wait for redaction fixture");
    fs::remove_file(&config_path).expect("remove redaction fixture config");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    for secret in [uid, username, password, "ENTRY_SENTINEL", "/private/"] {
        assert!(!stderr.contains(secret), "stderr leaked private sentinel");
    }
    assert_eq!(stderr, "camera address is invalid\n");
}
