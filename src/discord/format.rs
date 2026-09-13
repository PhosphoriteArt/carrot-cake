use lazy_static::lazy_static;
use regex::Regex;

use crate::discord::StreamInfo;

pub const DEFAULT_ONLINE_FORMAT: &str = "{ping+, }**{user_name}** is streaming! :tada:";
pub const DEFAULT_OFFLINE_FORMAT: &str = "{ping+, }**{user_name}** streamed :projector:";

lazy_static! {
    static ref FORMAT_SEGMENT_RE: Regex =
        Regex::new(r"\{([a-z_]+)(?:\+([^}]+))?\}").expect("BUG! Regex was invalid");
}

fn extract(id: &str, msg: &StreamInfo) -> Option<String> {
    match id {
        "game_name" => Some(msg.game_name.to_string()),
        "viewer_count" => Some(msg.viewer_count.to_string()),
        "started_at" => msg.started_at.as_ref().map(|f| f.to_rfc2822()),
        "user_name" => Some(msg.user_name.to_string()),
        "user_login" => Some(msg.user_login.to_string()),
        "user_icon" => msg.user_icon.as_ref().map(|x| x.to_string()),
        "game_id" => Some(msg.game_id.to_string()),
        "stream_id" => Some(msg.stream_id.to_string()),
        "stream_title" => Some(msg.stream_title.to_string()),
        "stream_thumbnail" => Some(msg.stream_thumbnail.to_string()),
        "video_id" => msg.video_id.as_ref().map(|v| v.to_string()),
        "ping" => msg.ping.as_ref().map(|p| format!("<@{}>", p)),
        "offline" => Some(msg.offline.to_string()),
        _ => None,
    }
}

pub fn run_format(fmt: &str, msg: &StreamInfo) -> String {
    let mut ptr = 0usize;
    let mut out = fmt.to_string();

    while let Some(mat) = FORMAT_SEGMENT_RE.captures_at(&out, ptr) {
        ptr = mat.get_match().end();
        let Some(ext) = mat.get(1) else {
            continue;
        };
        let r = mat.get_match().start()..mat.get_match().end();
        let Some(into) = extract(ext.as_str(), msg) else {
            ptr = mat.get_match().start();
            out.replace_range(r, "");
            continue;
        };
        let opt = mat.get(2);
        let repl = into + opt.map(|opt| opt.as_str()).unwrap_or_default();
        ptr = mat.get_match().start() + repl.len();
        out.replace_range(r, &repl);
    }

    out
}

#[cfg(test)]
mod test {
    use chrono::Utc;

    use crate::discord::{
        StreamInfo,
        format::{DEFAULT_ONLINE_FORMAT, run_format},
    };

    #[test]
    fn test_format() {
        let fmt = DEFAULT_ONLINE_FORMAT;
        let now = Utc::now();
        let mut msg = StreamInfo {
            game_name: "GAME_NAME".to_string(),
            viewer_count: 100,
            started_at: Some(now.clone().into()),
            user_name: "USER_NAME".to_string(),
            user_login: "USER_LOGIN".to_string(),
            user_icon: Some("USER_ICON".to_string()),
            game_id: "GAME_ID".to_string().into(),
            stream_id: "STREAM_ID".to_string().into(),
            stream_title: "STREAM_TITLE".to_string(),
            stream_thumbnail: "STREAM_THUMBNAIL".to_string(),
            video_id: Some("VIDEO_ID".to_string().into()),
            format_online: "FORMAT_ONLINE".to_string().into(),
            format_offline: "FORMAT_OFFLINE".to_string().into(),
            ping: Some("PING".to_string()),
            offline: false,
        };

        let result = run_format(fmt, &msg);
        assert_eq!("<@PING>, **USER_NAME** is streaming! :tada:", result);
        msg.ping = None;
        let result = run_format(fmt, &msg);
        assert_eq!("**USER_NAME** is streaming! :tada:", result);
    }
}
