//! Manages updating / pinging in discord.
//!
//! We're a bit sneaky here; we don't want to use a database to remember
//! where and how we've written messages, but we don't want to accidentally
//! ping multiple times about a stream. So... we use Discord as our database >:)
//! I hide the stream ID that the notification was for as a query parameter in
//! the game thumbnail URL; then before writing, we search for a message to edit,
//! matching on that stream ID to figure out if this is a new stream or not.
use std::{
    collections::{HashMap, HashSet},
    env,
    ops::Deref,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, bail};
use chrono::{TimeDelta, Utc};
use dashmap::{DashMap, DashSet, Entry};
use serenity::{
    Client,
    all::{
        ChannelId, CreateMessage, CurrentUser, EditMessage, GatewayIntents, GetMessages, GuildId,
        GuildInfo, GuildPagination, Http,
    },
    futures::future::join_all,
};
use tokio::{fs, select};
use twitch_api::{
    helix::{search::Channel, streams::Stream, videos::Video},
    types::{StreamId, UserId},
};

use crate::{
    config::{BY_GUILD_ID, CONFIG, NotifyConfig},
    discord::messaging::{StreamInfo, StreamNotifMessage},
    twitch::client::Notification,
    util::{
        SyncEvent,
        metrics::{
            CHANNEL_SYNC_MESSAGES, CHANNEL_SYNCS, CHANNEL_WRITES, EXTERNAL_CALLS, GUILDS, UPDATES,
            increment, record,
        },
    },
};

pub(in crate::discord) mod format;
pub(in crate::discord) mod messaging;

pub struct InnerConnection {
    client: Arc<Http>,
    close: SyncEvent,
    guilds: Mutex<Vec<GuildInfo>>,
    user: CurrentUser,
    bcids: HashMap<UserId, Channel>,

    closed_periodic_resync: SyncEvent,
    closed_discord_client: SyncEvent,

    // Keep track locally of the messages we're updating.
    // We'll resync this cache from the 50 latest messages
    // in a given channel on startup and when there's a cache miss.
    message_cache: Arc<DashMap<(ChannelId, StreamId), StreamNotifMessage>>,
    cache_ready_for_channel: Arc<DashSet<ChannelId>>,
    initial_sync_complete: SyncEvent,
    initial_reconciliation_finished: SyncEvent,
}

#[derive(Clone)]
pub struct DiscordConnection {
    inner: Arc<InnerConnection>,
}

impl Deref for DiscordConnection {
    type Target = InnerConnection;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

fn cache_path() -> PathBuf {
    env::var("DISCORD_CACHE")
        .ok()
        .map(PathBuf::from)
        .unwrap_or(env::temp_dir().join("carrot-cake-discord-cache.json"))
}

impl DiscordConnection {
    pub async fn new(token: String, bcids: HashMap<UserId, Channel>) -> anyhow::Result<Self> {
        let mut discord_client = Client::builder(&token, GatewayIntents::from_bits_retain(84992))
            .await
            .context("Err creating client")?;

        increment!(EXTERNAL_CALLS; "service": "discord", "endpoint": "get_current_user");
        let user = discord_client.http.get_current_user().await?;

        let cache = DashMap::new();
        match std::fs::OpenOptions::new().read(true).open(cache_path()) {
            Ok(file) => {
                let contents: Result<Vec<((ChannelId, StreamId), StreamNotifMessage)>, _> =
                    serde_json::from_reader(file);
                match contents {
                    Ok(data) => {
                        for (key, value) in data {
                            cache.insert(key, value);
                        }
                        log::info!("Read back DISCORD_CACHE");
                    }
                    Err(e) => log::warn!("Couldn't read DISCORD_CACHE: {e}"),
                }
            }
            Err(e) => log::warn!("Couldn't open DISCORD_CACHE: {e}"),
        }

        let client = Self {
            inner: Arc::new(InnerConnection {
                client: discord_client.http.clone(),
                close: SyncEvent::new(),
                guilds: Mutex::new(vec![]),
                user,
                bcids,
                closed_periodic_resync: SyncEvent::new(),
                closed_discord_client: SyncEvent::new(),
                message_cache: Arc::new(cache),
                cache_ready_for_channel: Arc::new(DashSet::new()),
                initial_reconciliation_finished: SyncEvent::new(),
                initial_sync_complete: SyncEvent::new(),
            }),
        };

        client.inner.clone().periodically_resync_guilds().await;

        let shard_manager = discord_client.shard_manager.clone();
        let await_closed = client.close.clone();
        let discord_closed = client.closed_discord_client.clone();
        let client_clone = client.clone();
        
        tokio::spawn(async move {
            await_closed.wait().await;
            shard_manager.shutdown_all().await;
            // best effort
            let _ = client_clone.dump_cache(cache_path()).await;
            discord_closed.signal().await;
        });

        tokio::spawn(async move {
            if let Err(e) = discord_client.start().await {
                log::error!("Discord error: {e}")
            }
        });

        Ok(client)
    }

    pub async fn close(&self) {
        self.close.signal().await;
        self.closed_periodic_resync.wait().await;
        self.closed_discord_client.wait().await;
    }

    pub async fn update_stream(&self, notif: Notification) -> anyhow::Result<()> {
        self.initial_reconciliation_finished.wait().await;
        self.inner.update_stream(notif, false).await?;
        Ok(())
    }
}

impl InnerConnection {
    // If we join a new guild, we should know about it.
    async fn periodically_resync_guilds(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.resync_guilds().await {
                    log::error!("Error resyncing guids: {e}")
                }
                select! {
                    _ = self.close.wait() => {
                        self.closed_periodic_resync.signal().await;
                        return;
                    }
                    _ = tokio::time::sleep(Duration::from_mins(4)) => {
                        continue;
                    }
                }
            }
        });
    }

    #[tracing::instrument(skip(self))]
    async fn resync_guilds(&self) -> anyhow::Result<()> {
        log::debug!("Refreshing guilds");

        let mut guilds: Vec<GuildInfo> = Vec::new();
        loop {
            increment!(EXTERNAL_CALLS; "service": "discord", "endpoint": "get_guilds");
            let next = self
                .client
                .get_guilds(
                    guilds.last().map(|g| GuildPagination::After(g.id)),
                    Some(100),
                )
                .await?;

            let incomplete = next.len() < 100;
            guilds.extend(next);
            if incomplete {
                break;
            }
        }

        let mut channels = HashSet::new();
        for cfg in &CONFIG.streams {
            for notif in &cfg.notify {
                if let Ok(gid) = notif
                    .guild_id
                    .parse::<u64>()
                    .inspect_err(|e| log::warn!("GIDFAIL {e}"))
                    && let Ok(cid) = notif
                        .channel_id
                        .parse::<u64>()
                        .inspect_err(|e| log::warn!("CIDFAIL {e}"))
                    && let gid = GuildId::from(gid)
                    && guilds.iter().any(|g| g.id == gid)
                {
                    channels.insert(ChannelId::from(cid));
                }
            }
        }

        join_all(
            channels
                .into_iter()
                .map(|cid| async move { self.sync_channel_messages(cid).await }),
        )
        .await;

        self.initial_sync_complete.signal().await;

        log::info!("Refreshed guilds: {} guilds", guilds.len());
        record!(GUILDS, guilds.len().try_into().unwrap_or_default());
        *self.guilds.lock().unwrap() = guilds;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn update_stream(&self, notif: Notification, edit_only: bool) -> anyhow::Result<()> {
        let guilds = self.guilds.lock().unwrap().clone();
        let login = &notif.stream().user_login;
        for guild in guilds {
            let Some(cfg) = BY_GUILD_ID.get(&guild.id) else {
                continue;
            };
            let Some(channels) = cfg.get(notif.stream().user_login.as_str()) else {
                continue;
            };
            log::info!(
                "Updating guild {} @ {:?} for {}...",
                guild.id,
                channels,
                login
            );

            self.update_stream_for_guild(
                guild,
                notif.clone(),
                channels.iter().map(|(a, b)| (a, *b)).collect::<Vec<_>>(),
                edit_only,
            )
            .await;
        }

        Ok(())
    }

    async fn update_stream_for_guild(
        &self,
        guild: GuildInfo,
        notif: Notification,
        channels: impl IntoIterator<Item = (&ChannelId, &'static NotifyConfig)>,
        edit_only: bool,
    ) {
        let gid = guild.id;
        let gname = guild.name;
        for (channel, cfg) in channels.into_iter() {
            match self
                .update_stream_for_channel(channel, cfg, &notif, edit_only)
                .await
            {
                Ok(edit_type) => {
                    increment!(UPDATES; "guild": gid.to_string(), "guild_name": gname.clone(), "channel_id": channel.to_string(), "edit_type": edit_type);
                }
                Err(e) => {
                    increment!(UPDATES; "guild": gid.to_string(), "guild_name": gname.clone(), "channel_id": channel.to_string(), "edit_type": "error");
                    log::warn!("Error updating guild {}: {:?}...", gid, e)
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn reconcile(
        &self,
        twitch_items: Vec<(Stream, Option<Video>)>,
    ) -> anyhow::Result<()> {
        self.initial_sync_complete.wait().await;

        let mut seen = HashSet::new();
        for (stream, video) in twitch_items {
            seen.insert(stream.id.clone());
            let update = Notification::Update(stream, video);
            if let Err(e) = self.update_stream(update, true).await {
                log::warn!("Error reconciling: {e}");
            }
        }

        let mut to_clear = HashSet::new();
        for ((cid, stream_id), message) in self
            .message_cache
            .iter()
            .map(|f| (f.key().clone(), f.value().clone()))
            .collect::<Vec<_>>()
        {
            if !seen.contains(&stream_id) {
                let mut with_offline = message.info.clone();
                with_offline.offline = true;
                if let Err(e) = self.set_offline(&message, &with_offline).await {
                    log::warn!("Error setting offline message: {e}");
                }
            }

            if message.info.offline
                && let ts = message.touched_timestamp().to_utc()
                && Utc::now().signed_duration_since(ts) > TimeDelta::new(3600i64, 0).unwrap()
            {
                to_clear.insert((cid, stream_id));
            }
        }

        for key in to_clear {
            self.message_cache.remove(&key);
        }

        if let Err(e) = self.dump_cache(cache_path()).await {
            log::error!("Failed to write discord cache: {e}")
        }

        self.initial_reconciliation_finished.signal().await;
        Ok(())
    }

    async fn dump_cache(&self, path: PathBuf) -> anyhow::Result<()> {
        let tmp = path.with_added_extension(".swp");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(false)
            .write(true)
            .open(&tmp)?;

        let content = self
            .message_cache
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect::<Vec<_>>();
        serde_json::to_writer(file, &content)?;
        fs::rename(tmp, path).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn sync_channel_messages(
        &self,
        channel: ChannelId,
    ) -> anyhow::Result<Vec<StreamNotifMessage>> {
        match self.get_channel_messages(channel).await {
            Ok(mut messages) => {
                increment!(CHANNEL_SYNCS; "channel_id": channel.to_string(), "success": "true");
                messages.sort_by_key(|m| m.timestamp);
                let ret = messages.clone();
                for message in messages.into_iter().rev() {
                    let key = (message.channel_id, message.info.stream_id.clone());
                    match self.message_cache.entry(key.clone()) {
                        Entry::Occupied(mut ent) => {
                            if message.touched_timestamp() > ent.get().touched_timestamp() {
                                increment!(CHANNEL_SYNC_MESSAGES; "channel_id": channel.to_string(), "cache": "update");
                                ent.insert(message);
                            } else {
                                increment!(CHANNEL_SYNC_MESSAGES; "channel_id": channel.to_string(), "cache": "hit");
                            }
                        }
                        Entry::Vacant(ent) => {
                            increment!(CHANNEL_SYNC_MESSAGES; "channel_id": channel.to_string(), "cache": "miss");
                            ent.insert(message);
                        }
                    };
                }
                self.cache_ready_for_channel.insert(channel);
                Ok(ret)
            }
            Err(e) => {
                increment!(CHANNEL_SYNCS; "channel_id": channel.to_string(), "success": "false");
                bail!("Channel sync error: {e}");
            }
        }
    }

    async fn get_channel_messages(
        &self,
        channel: ChannelId,
    ) -> anyhow::Result<Vec<StreamNotifMessage>> {
        increment!(EXTERNAL_CALLS; "service": "discord", "endpoint": "get_channel_messages");
        Ok(channel
            .messages(self.client.deref(), GetMessages::new().limit(50))
            .await?
            .into_iter()
            .filter_map(|f| {
                if f.author.id != self.user.id {
                    None
                } else {
                    f.try_into().ok()
                }
            })
            .collect())
    }

    #[tracing::instrument(skip(self))]
    async fn set_offline(
        &self,
        message: &StreamNotifMessage,
        info: &StreamInfo,
    ) -> anyhow::Result<()> {
        if message.info.offline {
            increment!(CHANNEL_WRITES; "channel_id": message.channel_id.to_string(), "action": "offline", "result": "skipped");
            return Ok(());
        }

        let msg = EditMessage::new()
            .content(info.headline_vod())
            .add_embed(info.stream_embed())
            .components(info.vod_components().unwrap_or_default());

        increment!(EXTERNAL_CALLS; "service": "discord", "endpoint": "edit_message");
        let msg = match message
            .channel_id
            .edit_message(self.client.deref(), message.message_id, msg)
            .await
        {
            Ok(msg) => msg,
            Err(e) => {
                increment!(CHANNEL_WRITES; "channel_id": message.channel_id.to_string(), "action": "offline", "result": "failure");
                return Err(e.into());
            }
        };

        if let Ok(msg) = StreamNotifMessage::try_from(msg) {
            self.message_cache
                .insert((msg.channel_id, info.stream_id.clone()), msg);
        }

        increment!(CHANNEL_WRITES; "channel_id": message.channel_id.to_string(), "action": "offline", "result": "edited");

        Ok(())
    }

    fn info_from_notif(&self, notif: &Notification, cfg: &'static NotifyConfig) -> StreamInfo {
        StreamInfo::from_stream(
            notif.stream(),
            self.bcids
                .get(&notif.stream().user_id)
                .map(|c| c.thumbnail_url.as_str()),
            notif.video().as_ref().map(|v| &v.id),
            matches!(notif, Notification::Offline(..)),
            cfg,
        )
    }

    #[tracing::instrument(skip(self))]
    async fn update_existing_message(
        &self,
        message: &StreamNotifMessage,
        cfg: &'static NotifyConfig,
        notif: &Notification,
    ) -> anyhow::Result<()> {
        let mut new_info = self.info_from_notif(notif, cfg);
        new_info.video_id = new_info
            .video_id
            .or_else(|| message.info.video_id.as_ref().cloned());

        match notif {
            Notification::Online(..) | Notification::Update(..) => {
                increment!(EXTERNAL_CALLS; "service": "discord", "endpoint": "edit_message");
                let msg = match message
                    .channel_id
                    .edit_message(
                        self.client.deref(),
                        message.message_id,
                        EditMessage::new()
                            .content(new_info.headline_streaming())
                            .add_embed(new_info.stream_embed())
                            .components(new_info.streaming_components()),
                    )
                    .await
                {
                    Ok(msg) => msg,
                    Err(e) => {
                        increment!(CHANNEL_WRITES; "channel_id": message.channel_id.to_string(), "action": "update_online", "result": "error");
                        return Err(e.into());
                    }
                };
                increment!(CHANNEL_WRITES; "channel_id": message.channel_id.to_string(), "action": "update_online", "result": "edited");
                if let Ok(msg) = StreamNotifMessage::try_from(msg) {
                    self.message_cache
                        .insert((msg.channel_id, new_info.stream_id.clone()), msg);
                }
            }
            Notification::Offline(..) => {
                self.set_offline(message, &new_info).await?;
            }
        };

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn try_edit_cached(
        &self,
        channel: &ChannelId,
        cfg: &'static NotifyConfig,
        notif: &Notification,
    ) -> anyhow::Result<bool> {
        if let Some(cached) = self
            .message_cache
            .get(&(*channel, notif.stream().id.clone()))
            .map(|opt| opt.clone())
        {
            if let Err(e) = self.update_existing_message(&cached, cfg, notif).await {
                log::warn!(
                    "Failed to edit message on first try, backing off and trying again: {e}"
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
            } else {
                return Ok(true);
            }
        }
        let last_info = self
            .sync_channel_messages(*channel)
            .await?
            .into_iter()
            .rev()
            .find(|v| v.info.stream_id == notif.stream().id);

        if let Some(last_info) = last_info {
            self.update_existing_message(&last_info, cfg, notif).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[tracing::instrument(skip(self))]
    async fn update_stream_for_channel(
        &self,
        channel: &ChannelId,
        cfg: &'static NotifyConfig,
        notif: &Notification,
        edit_only: bool,
    ) -> anyhow::Result<&'static str> {
        if self.try_edit_cached(channel, cfg, notif).await? {
            return Ok("edit");
        }
        if edit_only {
            return Ok("none");
        }

        if !self.cache_ready_for_channel.contains(channel) {
            bail!("Cache not ready for {channel}; was there a failure earlier?");
        }

        let new_info = self.info_from_notif(notif, cfg);

        match notif {
            Notification::Online(..) | Notification::Update(..) => {
                increment!(EXTERNAL_CALLS; "service": "discord", "endpoint": "send_message");
                let msg = match channel
                    .send_message(
                        self.client.deref(),
                        CreateMessage::new()
                            .content(new_info.headline_streaming())
                            .add_embed(new_info.stream_embed())
                            .components(new_info.streaming_components()),
                    )
                    .await
                {
                    Ok(msg) => msg,
                    Err(e) => {
                        increment!(CHANNEL_WRITES; "channel_id": channel.to_string(), "action": "ping_online", "result": "error");

                        return Err(e.into());
                    }
                };

                if let Ok(msg) = StreamNotifMessage::try_from(msg) {
                    self.message_cache
                        .insert((*channel, new_info.stream_id.clone()), msg);
                }

                increment!(CHANNEL_WRITES; "channel_id": channel.to_string(), "action": "ping_online", "result": "created");

                Ok("new")
            }
            _ => Ok("none"),
        }
    }
}
