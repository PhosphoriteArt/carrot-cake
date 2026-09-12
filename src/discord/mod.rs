use std::{
    ops::Deref,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use serenity::{
    Client,
    all::{
        ActionRowComponent, ButtonKind, ChannelId, Color, ComponentType, CreateActionRow,
        CreateButton, CreateEmbed, CreateMessage, CurrentUser, EditMessage, GatewayIntents,
        GetMessages, GuildInfo, GuildPagination, Http, Message, MessageId,
    },
};
use tokio::select;
use twitch_api::{helix::streams::Stream, types::StreamId};

use crate::{
    config::BY_GUILD_ID,
    twitch::client::Notification,
    util::{
        SyncEvent,
        metrics::{GUILDS, UPDATES, increment},
    },
};

pub struct InnerConnection {
    client: Arc<Http>,
    close: SyncEvent,
    guilds: Mutex<Vec<GuildInfo>>,
    user: CurrentUser,
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
    pub async fn new(token: String) -> anyhow::Result<Self> {
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
            }),
        };

        client.inner.clone().periodically_resync_guilds().await;

        tokio::spawn(async move {
            if let Err(e) = discord_client.start().await {
                log::error!("Discord error: {e}")
            }
        });

        Ok(client)
    }

    pub async fn close(&self) {
        self.close.signal().await;
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
        GUILDS.record(guilds.len().try_into().unwrap_or_default(), &[]);
        *self.guilds.lock().unwrap() = guilds;

        Ok(())
    }

    async fn update_stream(&self, notif: Notification) -> anyhow::Result<()> {
        let guilds = self.guilds.lock().unwrap().clone();
        for guild in guilds {
            let Some(cfg) = BY_GUILD_ID.get(&guild.id) else {
                continue;
            };
            let Some(channels) = cfg.get(notif.stream().id.as_str()) else {
                continue;
            };
            log::info!("Updating guild {} @ {:?}...", guild.id, channels);

            self.update_stream_for_guild(guild, notif.clone(), channels)
                .await;
        }

        Ok(())
    }

    async fn update_stream_for_guild(
        &self,
        guild: GuildInfo,
        notif: Notification,
        channels: impl IntoIterator<Item = &ChannelId>,
    ) {
        let gid = guild.id;
        let gname = guild.name;
        for channel in channels.into_iter() {
            match self.update_stream_for_channel(channel, &notif).await {
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
        notif: &Notification,
    ) -> anyhow::Result<&'static str> {
        let last_info = self.get_last_message_for(channel, notif).await?;

        match notif {
            Notification::Online(stream) | Notification::Update(stream) => {
                if let Some((message, stream_id)) = last_info
                    && stream.id == stream_id
                {
                    channel
                        .edit_message(
                            self.client.deref(),
                            message,
                            EditMessage::new()
                                .content(headline_streaming(&stream))
                                .add_embed(stream_embed(&stream))
                                .components(streaming_components(&stream)),
                        )
                        .await?;
                    Ok("edit")
                } else {
                    channel
                        .send_message(
                            self.client.deref(),
                            CreateMessage::new()
                                .content(headline_streaming(&stream))
                                .add_embed(stream_embed(&stream))
                                .components(streaming_components(&stream)),
                        )
                        .await?;
                    Ok("new")
                }
            }
            Notification::Offline(stream) => {
                if let Some((message, stream_id)) = last_info
                    && stream.id == stream_id
                {
                    channel
                        .edit_message(
                            self.client.deref(),
                            message,
                            EditMessage::new()
                                .content(headline_vod(&stream))
                                .add_embed(stream_embed(&stream))
                                .components(vod_components(&stream)),
                        )
                        .await?;
                    Ok("edit")
                } else {
                    Ok("none")
                }
            }
        }
    }
}
fn headline_vod(stream: &Stream) -> String {
    format!("**{}** streamed", stream.user_name)
}

fn headline_streaming(stream: &Stream) -> String {
    format!("**{}** is streaming!", stream.user_name)
}

fn streaming_components(stream: &Stream) -> Vec<CreateActionRow> {
    vec![CreateActionRow::Buttons(vec![
        stream_button(stream),
        vod_button(stream),
    ])]
}

fn vod_components(stream: &Stream) -> Vec<CreateActionRow> {
    vec![CreateActionRow::Buttons(vec![vod_button(stream)])]
}

fn vod_button(stream: &Stream) -> CreateButton {
    CreateButton::new_link(format!("https://twitch.tv/videos/{}", stream.id))
        .label("Watch the VOD!")
}

fn stream_button(stream: &Stream) -> CreateButton {
    CreateButton::new_link(format!("https://twitch.tv/{}", stream.user_login))
        .label("Watch the stream!")
}

fn stream_embed(stream: &Stream) -> CreateEmbed {
    CreateEmbed::new()
        .color(Color::from_rgb(255, 0, 0))
        .title(if stream.title.trim().is_empty() {
            "<untitled>".to_string()
        } else {
            stream.title.clone()
        })
        .description(if stream.game_name.is_empty() {
            "<no game>".to_string()
        } else {
            stream.game_name.clone()
        })
        .url(format!("https://twitch.tv/{}", stream.user_login))
}

fn get_stream_id(msg: &Message) -> Option<StreamId> {
    msg.components
        .iter()
        .flat_map(|c| {
            if matches!(c.kind, ComponentType::Button) {
                c.components.iter()
            } else {
                [].iter()
            }
        })
        .find_map(|c| {
            let ActionRowComponent::Button(b) = c else {
                return None;
            };
            let ButtonKind::Link { url } = &b.data else {
                return None;
            };
            if !url.starts_with("https://twitch.tv/videos/") {
                return None;
            };
            let id = &url["https://twitch.tv/videos/".len()..];

            StreamId::try_from(id).ok()
        })
}
