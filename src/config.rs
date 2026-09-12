use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serenity::all::{ChannelId, GuildId};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::{env, fs};

lazy_static! {
    pub static ref CONFIG: Config = serde_yaml::from_slice(
        &fs::read(env::var("CONFIG_FILE").unwrap_or("./config.yaml".to_string()))
            .expect("failed to read file")
    )
    .expect("failed to parse config");
    pub static ref BY_GUILD_ID: HashMap<GuildId, HashMap<String, HashSet<ChannelId>>> =
        derive_by_guild_id();
}

fn derive_by_guild_id() -> HashMap<GuildId, HashMap<String, HashSet<ChannelId>>> {
    let mut map: HashMap<GuildId, HashMap<String, HashSet<ChannelId>>> = HashMap::new();

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
                            ent.get_mut().insert(cid);
                        }
                        Entry::Vacant(ent) => {
                            ent.insert(HashSet::new()).insert(cid);
                        }
                    }
                }
                Entry::Vacant(ent) => {
                    ent.insert(HashMap::new())
                        .insert(stream.streamer_login.clone(), HashSet::from_iter([cid]));
                }
            }
        }
    }

    map
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    streams: Vec<StreamConfig>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    streamer_login: String,
    notify: Vec<NotifyConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct NotifyConfig {
    guild_id: String,
    channel_id: String,
}
