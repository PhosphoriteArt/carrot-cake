use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use serenity::{
    Client,
    all::{
        ChannelId, Color, CreateActionRow, CreateButton, CreateEmbed, CreateEmbedAuthor,
        CreateEmbedFooter, CreateMessage, CurrentUser, EditMessage, GatewayIntents, GetMessages,
        GuildInfo, GuildPagination, Http, Message, MessageId, Timestamp,
    },
};
use tokio::select;
use twitch_api::{
    helix::{search::Channel, streams::Stream, videos::Video},
    types::{StreamId, UserId},
};

use crate::{
    config::BY_GUILD_ID,
    twitch::client::Notification,
    util::{
        SyncEvent,
        metrics::{GUILDS, UPDATES, increment, record},
    },
};

pub struct InnerConnection {
    client: Arc<Http>,
    close: SyncEvent,
    guilds: Mutex<Vec<GuildInfo>>,
    user: CurrentUser,
    bcids: HashMap<UserId, Channel>,

    closed_periodic_resync: SyncEvent,
    closed_discord_client: SyncEvent,
}

#[derive(Clone)]
pub struct DiscordConnection {
    inner: Arc<InnerConnection>,
}

impl Deref for DiscordConnection {
    type Target = InnerConnection;

    fn deref(&self) -> &Self::Target {
        return self.inner.deref();
    }
}

impl DiscordConnection {
    pub async fn new(token: String, bcids: HashMap<UserId, Channel>) -> anyhow::Result<Self> {
        let mut discord_client = Client::builder(
            &token,
            GatewayIntents::from_bits_retain(84992).union(GatewayIntents::MESSAGE_CONTENT),
        )
        .await
        .context("Err creating client")?;

        let user = discord_client.http.get_current_user().await?;

        let client = Self {
            inner: Arc::new(InnerConnection {
                client: discord_client.http.clone(),
                close: SyncEvent::new(),
                guilds: Mutex::new(vec![]),
                user,
                bcids: bcids,
                closed_periodic_resync: SyncEvent::new(),
                closed_discord_client: SyncEvent::new(),
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
        self.inner.update_stream(notif).await
    }
}

impl InnerConnection {
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

    async fn resync_guilds(&self) -> anyhow::Result<()> {
        log::debug!("Refreshing guilds");

        let mut guilds: Vec<GuildInfo> = Vec::new();
        loop {
            let next = self
                .client
                .get_guilds(
                    guilds.last().map(|g| GuildPagination::After(g.id.clone())),
                    Some(100),
                )
                .await?;

            let incomplete = next.len() < 100;
            guilds.extend(next);
            if incomplete {
                break;
            }
        }

        log::info!("Refreshed guilds: {} guilds", guilds.len());
        record!(GUILDS, guilds.len().try_into().unwrap_or_default());
        *self.guilds.lock().unwrap() = guilds;

        Ok(())
    }

    async fn update_stream(&self, notif: Notification) -> anyhow::Result<()> {
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

            self.update_stream_for_guild(guild, notif.clone(), channels)
                .await;
        }

        Ok(())
    }

    async fn update_stream_for_guild(
        &self,
        guild: GuildInfo,
        notif: Notification,
        channels: impl IntoIterator<Item = (&ChannelId, &Option<String>)>,
    ) {
        let gid = guild.id;
        let gname = guild.name;
        for (channel, ping) in channels.into_iter() {
            match self
                .update_stream_for_channel(channel, ping.as_deref(), &notif)
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

    async fn get_last_message_for(
        &self,
        channel: &ChannelId,
        notif: &Notification,
    ) -> anyhow::Result<Option<(MessageId, StreamId)>> {
        Ok(channel
            .messages(self.client.deref(), GetMessages::new().limit(50))
            .await?
            .into_iter()
            .filter_map(|f| {
                if f.author.id != self.user.id {
                    None
                } else {
                    if let Some(id) = get_stream_id(&f)
                        && id == notif.stream().id
                    {
                        Some((f.id, id))
                    } else {
                        None
                    }
                }
            })
            .next())
    }

    async fn update_stream_for_channel(
        &self,
        channel: &ChannelId,
        ping: Option<&str>,
        notif: &Notification,
    ) -> anyhow::Result<&'static str> {
        let last_info = self.get_last_message_for(channel, notif).await?;

        match notif {
            Notification::Online(stream, video) | Notification::Update(stream, video) => {
                if let Some((message, stream_id)) = last_info
                    && stream.id == stream_id
                {
                    channel
                        .edit_message(
                            self.client.deref(),
                            message,
                            EditMessage::new()
                                .content(headline_streaming(ping, &stream))
                                .add_embed(stream_embed(
                                    &stream,
                                    false,
                                    self.bcids.get(&stream.user_id),
                                ))
                                .components(streaming_components(&stream, video.as_ref())),
                        )
                        .await?;
                    Ok("edit")
                } else {
                    channel
                        .send_message(
                            self.client.deref(),
                            CreateMessage::new()
                                .content(headline_streaming(ping, &stream))
                                .add_embed(stream_embed(
                                    &stream,
                                    false,
                                    self.bcids.get(&stream.user_id),
                                ))
                                .components(streaming_components(&stream, video.as_ref())),
                        )
                        .await?;
                    Ok("new")
                }
            }
            Notification::Offline(stream, video) => {
                if let Some((message, stream_id)) = last_info
                    && stream.id == stream_id
                {
                    channel
                        .edit_message(self.client.deref(), message, {
                            let mut msg = EditMessage::new()
                                .content(headline_vod(ping, &stream))
                                .add_embed(stream_embed(
                                    &stream,
                                    true,
                                    self.bcids.get(&stream.user_id),
                                ));
                            if let Some(video) = video.as_ref() {
                                msg = msg.components(vod_components(video))
                            }
                            msg
                        })
                        .await?;
                    Ok("edit")
                } else {
                    Ok("none")
                }
            }
        }
    }
}
fn headline_vod(ping: Option<&str>, stream: &Stream) -> String {
    format!(
        "{}**{}** streamed :projector:",
        if let Some(ping) = ping {
            ping.to_owned() + ", "
        } else {
            "".to_owned()
        },
        stream.user_name
    )
}

fn headline_streaming(ping: Option<&str>, stream: &Stream) -> String {
    format!(
        "{}**{}** is streaming! :tada: ",
        if let Some(ping) = ping {
            ping.to_owned() + ", "
        } else {
            "".to_owned()
        },
        stream.user_name
    )
}

fn streaming_components(stream: &Stream, video: Option<&Video>) -> Vec<CreateActionRow> {
    vec![CreateActionRow::Buttons(
        [Some(stream_button(stream)), video.map(|v| vod_button(v))]
            .into_iter()
            .flatten()
            .collect(),
    )]
}

fn vod_components(video: &Video) -> Vec<CreateActionRow> {
    vec![CreateActionRow::Buttons(vec![vod_button(video)])]
}

fn vod_button(video: &Video) -> CreateButton {
    CreateButton::new_link(video.url.clone()).label("Watch the VOD!")
}

fn stream_button(stream: &Stream) -> CreateButton {
    CreateButton::new_link(format!("https://twitch.tv/{}", stream.user_login))
        .label("Watch the stream!")
}

fn stream_embed(stream: &Stream, offline: bool, chan: Option<&Channel>) -> CreateEmbed {
    let fields: Vec<_> = [
        (if !stream.game_name.is_empty() {
            Some(("**Game name**", stream.game_name.clone(), true))
        } else {
            None
        }),
        Some(("**Viewers**", stream.viewer_count.to_string(), true)),
        (if offline
            && let Ok(dt) = DateTime::parse_from_rfc3339(&stream.started_at.to_string())
            && let Ok(duration) = dt.signed_duration_since(Utc::now()).to_std()
        {
            Some((
                "**Duration**",
                humantime::format_duration(duration - Duration::new(0, duration.subsec_nanos()))
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
        let mut cea = CreateEmbedAuthor::new(stream.user_name.to_string());
        if let Some(chan) = chan {
            cea = cea.icon_url(chan.thumbnail_url.to_string())
        };
        cea
    };

    let mut embed = CreateEmbed::new()
        .color(Color::from_rgb(240, 161, 163))
        .title(if stream.title.trim().is_empty() {
            "<untitled>".to_string()
        } else {
            stream.title.clone()
        })
        .fields(fields)
        .author(author)
        .thumbnail(format!(
            "https://static-cdn.jtvnw.net/ttv-boxart/{}.jpg?_cc_id={}",
            stream.game_id, stream.id
        ))
        .image(
            stream
                .thumbnail_url
                .replace("{width}", "1080")
                .replace("{height}", "720"),
        )
        .url(format!("https://twitch.tv/{}", stream.user_login));

    if offline {
        embed = embed
            .footer(CreateEmbedFooter::new("Last online"))
            .timestamp(Timestamp::now())
    }

    embed
}

fn get_stream_id(msg: &Message) -> Option<StreamId> {
    msg.embeds.iter().find_map(|e| {
        let Some(thumb) = &e.thumbnail else {
            return None;
        };
        let Ok(url) = reqwest::Url::parse(&thumb.url) else {
            return None;
        };
        let Some(id) = url
            .query_pairs()
            .find_map(|(k, v)| if k == "_cc_id" { Some(v) } else { None })
        else {
            return None;
        };

        StreamId::try_from(id.to_string()).ok()
    })
}
