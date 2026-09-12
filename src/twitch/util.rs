use std::env;

pub fn twitch_ws_url() -> String {
    env::var("TWITCH_INIT")
        .ok()
        .unwrap_or_else(|| twitch_api::TWITCH_EVENTSUB_WEBSOCKET_URL.to_string())
}
