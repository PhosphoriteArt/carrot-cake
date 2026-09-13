use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serenity::all::{ChannelId, GuildId};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::str::FromStr;
use std::{env, fs};

lazy_static! {
    pub static ref CONFIG: Config = yaml_serde::from_slice(
        &fs::read(env::var("CONFIG_FILE").unwrap_or("./config.yaml".to_string()))
            .expect("failed to read file")
    )
    .expect("failed to parse config");

    // Guild -> (Twitch Login -> (Channel -> Ping))
    pub static ref BY_GUILD_ID: HashMap<GuildId, HashMap<String, HashMap<ChannelId, &'static NotifyConfig>>> =
        derive_by_guild_id();
}

// Computes a reverse mapping form the config so individual guilds can easily look up
// what they need in order to deliver notifications for the right streamers to the right channels
fn derive_by_guild_id()
-> HashMap<GuildId, HashMap<String, HashMap<ChannelId, &'static NotifyConfig>>> {
    let mut map: HashMap<GuildId, HashMap<String, HashMap<ChannelId, &'static NotifyConfig>>> =
        HashMap::new();

    for stream in &CONFIG.streams {
        for notify in &stream.notify {
            let gid =
                GuildId::from_str(&notify.guild_id).expect("config incorrect, failed to parse");
            let cid =
                ChannelId::from_str(&notify.channel_id).expect("config incorrect, failed to parse");
            match map.entry(gid) {
                Entry::Occupied(mut ent) => {
                    let inner = ent.get_mut();
                    match inner.entry(stream.streamer_login.clone()) {
                        Entry::Occupied(mut ent) => {
                            ent.get_mut().insert(cid, notify);
                        }
                        Entry::Vacant(ent) => {
                            ent.insert(HashMap::new()).insert(cid, notify);
                        }
                    }
                }
                Entry::Vacant(ent) => {
                    ent.insert(HashMap::new()).insert(
                        stream.streamer_login.clone(),
                        HashMap::from_iter([(cid, notify)]),
                    );
                }
            }
        }
    }

    map
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub streams: Vec<StreamConfig>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub streamer_login: String,
    pub notify: Vec<NotifyConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct NotifyConfig {
    pub guild_id: String,
    pub channel_id: String,
    pub ping: Option<String>,
    pub format_online: Option<String>,
    pub format_offline: Option<String>,
}
