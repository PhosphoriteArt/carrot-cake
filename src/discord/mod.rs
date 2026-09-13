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
    ops::Deref,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, FixedOffset, Utc};
use dashmap::{DashMap, DashSet, Entry};
use serenity::{
    Client,
    all::{
        ChannelId, Color, CreateActionRow, CreateButton, CreateEmbed, CreateEmbedAuthor,
        CreateEmbedFooter, CreateMessage, CurrentUser, EditMessage, GatewayIntents, GetMessages,
        GuildId, GuildInfo, GuildPagination, Http, Message, MessageId, Timestamp,
    },
    futures::future::join_all,
};
use tokio::select;
use twitch_api::{
    helix::{search::Channel, streams::Stream, videos::Video},
    types::{CategoryId, StreamId, UserId, VideoId},
};
use url_builder::URLBuilder;

use crate::{
    config::{BY_GUILD_ID, CONFIG},
    twitch::client::Notification,
    util::{
        SyncEvent,
        metrics::{
            CHANNEL_SYNC_MESSAGES, CHANNEL_SYNCS, CHANNEL_WRITES, GUILDS, UPDATES, increment,
            record,
        },
    },
};

// Stores everything we need to make our notifications happen.
// Everything here should be re-derivable from the message itself!!
#[derive(Debug, Clone)]
struct StreamInfo {
    game_name: String,
    viewer_count: usize,
    started_at: Option<DateTime<FixedOffset>>,
    user_name: String,
    user_login: String,
    user_icon: Option<String>,
    game_id: CategoryId,
    stream_id: StreamId,
    stream_title: String,
    stream_thumbnail: String,
    video_id: Option<VideoId>,
    ping: Option<String>,
    offline: bool,
}

impl StreamInfo {
    fn into_pairs_with_context(self) -> (Vec<(&'static str, String)>, String, Option<String>) {
        (
            [
                Some(("game_name", self.game_name)),
                Some(("viewer_count", self.viewer_count.to_string())),
                Some(("user_name", self.user_name)),
                Some(("user_login", self.user_login)),
                Some(("game_id", self.game_id.to_string())),
                Some(("stream_id", self.stream_id.to_string())),
                Some(("stream_title", self.stream_title)),
                self.started_at.map(|s| ("started_at", s.to_rfc3339())),
                self.video_id.map(|v| ("video_id", v.to_string())),
                self.ping.map(|p| ("ping", p)),
                self.offline.then(|| ("offline", "1".to_string())),
            ]
            .into_iter()
            .flatten()
            .collect(),
            self.stream_thumbnail,
            self.user_icon,
        )
    }
    fn from_stream(
        stream: &Stream,
        user_icon: Option<&str>,
        video_id: Option<&VideoId>,
        ping: Option<&str>,
        offline: bool,
    ) -> Self {
        StreamInfo {
            game_name: stream.game_name.clone(),
            viewer_count: stream.viewer_count,
            started_at: DateTime::parse_from_rfc3339(stream.started_at.as_str()).ok(),
            user_name: stream.user_name.to_string(),
            user_login: stream.user_login.to_string(),
            game_id: stream.game_id.clone(),
            stream_id: stream.id.clone(),
            stream_title: stream.title.clone(),
            stream_thumbnail: stream.thumbnail_url.clone(),
            ping: ping.map(|s| s.to_string()),
            user_icon: user_icon.map(|ui| ui.to_string()),
            video_id: video_id.cloned(),
            offline,
        }
    }
    fn from_pairs_with_context(
        pairs: impl IntoIterator<Item = (String, String)>,
        stream_thumbnail: String,
        user_icon: Option<String>,
    ) -> Option<Self> {
        let mut game_name: Option<String> = None;
        let mut viewer_count: Option<usize> = None;
        let mut user_name: Option<String> = None;
        let mut user_login: Option<String> = None;
        let mut game_id: Option<CategoryId> = None;
        let mut stream_id: Option<StreamId> = None;
        let mut stream_title: Option<String> = None;

        let mut started_at: Option<DateTime<FixedOffset>> = None;
        let mut video_id: Option<VideoId> = None;
        let mut ping: Option<String> = None;
        let mut offline: bool = false;

        for (key, value) in pairs {
            match key.as_str() {
                "offline" => {
                    offline = true;
                }
                "game_name" => {
                    game_name = Some(value.to_string());
                }
                "viewer_count" => {
                    viewer_count = value.parse().ok();
                }
                "user_name" => {
                    user_name = Some(value.to_string());
                }
                "user_login" => {
                    user_login = Some(value.to_string());
                }
                "game_id" => {
                    game_id = Some(value.to_string().into());
                }
                "stream_id" => {
                    stream_id = Some(value.to_string().into());
                }
                "stream_title" => {
                    stream_title = Some(value.to_string());
                }
                "started_at" => {
                    started_at = DateTime::parse_from_rfc3339(&value).ok();
                }
                "video_id" => {
                    video_id = Some(value.to_string().into());
                }
                "ping" => {
                    ping = Some(value.to_string());
                }
                _ => {}
            };
        }

        Some(Self {
            started_at,
            video_id,
            ping,
            game_name: game_name?,
            viewer_count: viewer_count?,
            user_name: user_name?,
            user_login: user_login?,
            user_icon,
            game_id: game_id?,
            stream_id: stream_id?,
            stream_title: stream_title?,
            stream_thumbnail,
            offline,
        })
    }

    fn headline_vod(&self) -> String {
        format!(
            "{}**{}** streamed :projector:",
            if let Some(ping) = &self.ping {
                format!("<@{}>, ", ping)
            } else {
                "".to_owned()
            },
            self.user_name
        )
    }

    fn headline_streaming(&self) -> String {
        format!(
            "{}**{}** is streaming! :tada: ",
            if let Some(ping) = &self.ping {
                format!("<@{}>, ", ping)
            } else {
                "".to_owned()
            },
            self.user_name
        )
    }

    fn streaming_components(&self) -> Vec<CreateActionRow> {
        vec![CreateActionRow::Buttons(
            [Some(self.stream_button()), self.vod_button()]
                .into_iter()
                .flatten()
                .collect(),
        )]
    }

    fn vod_components(&self) -> Option<Vec<CreateActionRow>> {
        self.vod_button()
            .map(|b| vec![CreateActionRow::Buttons(vec![b])])
    }

    fn vod_button(&self) -> Option<CreateButton> {
        self.video_id.as_ref().map(|v| {
            CreateButton::new_link(format!("https://twitch.tv/videos/{}", v))
                .label("Watch the VOD!")
        })
    }

    fn stream_button(&self) -> CreateButton {
        CreateButton::new_link(format!("https://twitch.tv/{}", self.user_login))
            .label("Watch the stream!")
    }

    fn stream_embed(&self) -> CreateEmbed {
        let fields: Vec<_> = [
            (if !self.game_name.is_empty() {
                Some(("**Game name**", self.game_name.to_string(), true))
            } else {
                None
            }),
            Some(("**Viewers**", self.viewer_count.to_string(), true)),
            (if self.offline
                && let Some(dt) = self.started_at.as_ref()
                && let Ok(duration) = Utc::now().signed_duration_since(dt).to_std()
            {
                Some((
                    "**Duration**",
                    humantime::format_duration(
                        duration - Duration::new(0, duration.subsec_nanos()),
                    )
                    .to_string(),
                    true,
                ))
            } else {
                None
            }),
        ]
        .into_iter()
        .flatten()
        .collect();

        let author = {
            let mut cea = CreateEmbedAuthor::new(self.user_name.to_string());
            if let Some(user_icon) = &self.user_icon {
                cea = cea.icon_url(user_icon.to_string())
            };
            cea
        };

        let mut thumb_url = URLBuilder::new();

        thumb_url
            .set_protocol("https")
            .set_host("static-cdn.jtvnw.net")
            .add_route("ttv-boxart")
            .add_route(&format!("{}.jpg", self.game_id));

        for (key, value) in self.clone().into_pairs_with_context().0 {
            thumb_url.add_param(&urlencoding::encode(key), &urlencoding::encode(&value));
        }

        let url = thumb_url.build();

        let mut embed = CreateEmbed::new()
            .color(Color::from_rgb(240, 161, 163))
            .title(if self.stream_title.trim().is_empty() {
                "<untitled>".to_string()
            } else {
                self.stream_title.to_string()
            })
            .fields(fields)
            .author(author)
            .thumbnail(url)
            .image(
                self.stream_thumbnail
                    .replace("{width}", "1080")
                    .replace("{height}", "720"),
            )
            .url(format!("https://twitch.tv/{}", self.user_login));

        if self.offline {
            embed = embed
                .footer(CreateEmbedFooter::new("Last online"))
                .timestamp(Timestamp::now())
        }

        embed
    }
}

#[derive(Debug, Clone)]
struct StreamNotifMessage {
    channel_id: ChannelId,
    message_id: MessageId,
    timestamp: Timestamp,
    edited_timestamp: Option<Timestamp>,

    info: StreamInfo,
}

impl StreamNotifMessage {
    fn touched_timestamp(&self) -> &Timestamp {
        self.edited_timestamp.as_ref().unwrap_or(&self.timestamp)
    }
}

impl TryFrom<Message> for StreamNotifMessage {
    type Error = anyhow::Error;

    fn try_from(msg: Message) -> Result<Self, Self::Error> {
        let stream_info = msg
            .embeds
            .into_iter()
            .find_map(|e| {
                let thumb = e.thumbnail?;
                let image = e.image?;
                let user_icon = e.author.and_then(|au| au.icon_url);
                let url = reqwest::Url::parse(&thumb.url).ok()?;

                StreamInfo::from_pairs_with_context(
                    url.query_pairs()
                        .into_iter()
                        .map(|(a, b)| (a.into_owned(), b.into_owned())),
                    image.url,
                    user_icon,
                )
            })
            .ok_or_else(|| anyhow!("No stream info found in message {}", &msg.id))?;

        Ok(StreamNotifMessage {
            message_id: msg.id,
            channel_id: msg.channel_id,
            timestamp: msg.timestamp,
            edited_timestamp: msg.edited_timestamp,
            info: stream_info,
        })
    }
}

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

impl DiscordConnection {
    pub async fn new(token: String, bcids: HashMap<UserId, Channel>) -> anyhow::Result<Self> {
        let mut discord_client = Client::builder(&token, GatewayIntents::from_bits_retain(84992))
            .await
            .context("Err creating client")?;

        let user = discord_client.http.get_current_user().await?;

        let client = Self {
            inner: Arc::new(InnerConnection {
                client: discord_client.http.clone(),
                close: SyncEvent::new(),
                guilds: Mutex::new(vec![]),
                user,
                bcids,
                closed_periodic_resync: SyncEvent::new(),
                closed_discord_client: SyncEvent::new(),
                message_cache: Arc::new(DashMap::new()),
                cache_ready_for_channel: Arc::new(DashSet::new()),
                initial_reconciliation_finished: SyncEvent::new(),
                initial_sync_complete: SyncEvent::new(),
            }),
        };

        client.inner.clone().periodically_resync_guilds().await;

        let shard_manager = discord_client.shard_manager.clone();
        let await_closed = client.close.clone();
        let discord_closed = client.closed_discord_client.clone();

        tokio::spawn(async move {
            await_closed.wait().await;
            shard_manager.shutdown_all().await;
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

            self.update_stream_for_guild(guild, notif.clone(), channels, edit_only)
                .await;
        }

        Ok(())
    }

    async fn update_stream_for_guild(
        &self,
        guild: GuildInfo,
        notif: Notification,
        channels: impl IntoIterator<Item = (&ChannelId, &Option<String>)>,
        edit_only: bool,
    ) {
        let gid = guild.id;
        let gname = guild.name;
        for (channel, ping) in channels.into_iter() {
            match self
                .update_stream_for_channel(channel, ping.as_deref(), &notif, edit_only)
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

        for (stream_id, message) in self
            .message_cache
            .iter()
            .map(|f| (f.key().1.clone(), f.value().clone()))
            .collect::<Vec<_>>()
        {
            if !seen.contains(&stream_id) {
                let mut with_offline = message.info.clone();
                with_offline.offline = true;
                if let Err(e) = self.set_offline(&message, &with_offline).await {
                    log::warn!("Error setting offline message: {e}");
                }
            }
        }

        self.initial_reconciliation_finished.signal().await;
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

    fn info_from_notif(&self, notif: &Notification, ping: Option<&str>) -> StreamInfo {
        StreamInfo::from_stream(
            notif.stream(),
            self.bcids
                .get(&notif.stream().user_id)
                .map(|c| c.thumbnail_url.as_str()),
            notif.video().as_ref().map(|v| &v.id),
            ping,
            matches!(notif, Notification::Offline(..)),
        )
    }

    #[tracing::instrument(skip(self))]
    async fn update_existing_message(
        &self,
        message: &StreamNotifMessage,
        ping: Option<&str>,
        notif: &Notification,
    ) -> anyhow::Result<()> {
        let new_info = self.info_from_notif(notif, ping);

        match notif {
            Notification::Online(..) | Notification::Update(..) => {
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
        ping: Option<&str>,
        notif: &Notification,
    ) -> anyhow::Result<bool> {
        if let Some(cached) = self
            .message_cache
            .get(&(*channel, notif.stream().id.clone()))
            .map(|opt| opt.clone())
        {
            if let Err(e) = self.update_existing_message(&cached, ping, notif).await {
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
            self.update_existing_message(&last_info, ping, notif)
                .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[tracing::instrument(skip(self))]
    async fn update_stream_for_channel(
        &self,
        channel: &ChannelId,
        ping: Option<&str>,
        notif: &Notification,
        edit_only: bool,
    ) -> anyhow::Result<&'static str> {
        if self.try_edit_cached(channel, ping, notif).await? {
            return Ok("edit");
        }
        if edit_only {
            return Ok("none");
        }

        if !self.cache_ready_for_channel.contains(channel) {
            bail!("Cache not ready for {channel}; was there a failure earlier?");
        }

        let new_info = self.info_from_notif(notif, ping);

        match notif {
            Notification::Online(..) | Notification::Update(..) => {
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
