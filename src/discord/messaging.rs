use std::time::Duration;

use anyhow::anyhow;
use chrono::{DateTime, FixedOffset, Utc};
use serenity::all::{
    ChannelId, Color, CreateActionRow, CreateButton, CreateEmbed, CreateEmbedAuthor,
    CreateEmbedFooter, Message, MessageId, Timestamp,
};
use twitch_api::{
    helix::streams::Stream,
    types::{CategoryId, StreamId, VideoId},
};
use url_builder::URLBuilder;

use crate::{
    config::NotifyConfig,
    discord::format::{DEFAULT_OFFLINE_FORMAT, DEFAULT_ONLINE_FORMAT, run_format},
};

#[derive(Debug, Clone)]
pub(super) struct StreamNotifMessage {
    pub(super) channel_id: ChannelId,
    pub(super) message_id: MessageId,
    pub(super) timestamp: Timestamp,
    pub(super) edited_timestamp: Option<Timestamp>,

    pub(super) info: StreamInfo,
}

impl StreamNotifMessage {
    pub(super) fn touched_timestamp(&self) -> &Timestamp {
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

// Stores everything we need to make our notifications happen.
// Everything here should be re-derivable from the message itself!!
#[derive(Debug, Clone)]
pub(super) struct StreamInfo {
    pub(super) game_name: String,
    pub(super) viewer_count: usize,
    pub(super) started_at: Option<DateTime<FixedOffset>>,
    pub(super) user_name: String,
    pub(super) user_login: String,
    pub(super) user_icon: Option<String>,
    pub(super) game_id: CategoryId,
    pub(super) stream_id: StreamId,
    pub(super) stream_title: String,
    pub(super) stream_thumbnail: String,
    pub(super) video_id: Option<VideoId>,
    pub(super) format_online: Option<String>,
    pub(super) format_offline: Option<String>,
    pub(super) ping: Option<String>,
    pub(super) offline: bool,
}

impl StreamInfo {
    pub(super) fn into_pairs_with_context(
        self,
    ) -> (Vec<(&'static str, String)>, String, Option<String>) {
        (
            [
                Some(("_ccgn", self.game_name)),
                Some(("_ccvc", self.viewer_count.to_string())),
                Some(("_ccun", self.user_name)),
                Some(("_ccul", self.user_login)),
                Some(("_ccg", self.game_id.to_string())),
                Some(("_ccs", self.stream_id.to_string())),
                Some(("_ccst", self.stream_title)),
                self.format_online.map(|f| ("_ccf", f.to_string())),
                self.format_offline.map(|f| ("_ccfo", f.to_string())),
                self.started_at.map(|s| ("_ccsa", s.to_rfc3339())),
                self.video_id.map(|v| ("_ccv", v.to_string())),
                self.ping.map(|p| ("_ccat", p)),
                self.offline.then(|| ("_cco", "1".to_string())),
            ]
            .into_iter()
            .flatten()
            .collect(),
            self.stream_thumbnail,
            self.user_icon,
        )
    }
    pub(super) fn from_stream(
        stream: &Stream,
        user_icon: Option<&str>,
        video_id: Option<&VideoId>,
        offline: bool,
        cfg: &'static NotifyConfig,
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
            ping: cfg.ping.as_ref().map(|s| s.to_string()),
            user_icon: user_icon.map(|ui| ui.to_string()),
            video_id: video_id.cloned(),
            offline,
            format_online: cfg.format_online.as_ref().map(|f| f.to_string()),
            format_offline: cfg.format_offline.as_ref().map(|f| f.to_string()),
        }
    }
    pub(super) fn from_pairs_with_context(
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
        let mut format_online: Option<String> = None;
        let mut format_offline: Option<String> = None;

        let mut started_at: Option<DateTime<FixedOffset>> = None;
        let mut video_id: Option<VideoId> = None;
        let mut ping: Option<String> = None;
        let mut offline: bool = false;

        for (key, value) in pairs {
            match key.as_str() {
                "offline" | "_cco" => {
                    offline = true;
                }
                "game_name" | "_ccgn" => {
                    game_name = Some(value.to_string());
                }
                "viewer_count" | "_ccvc" => {
                    viewer_count = value.parse().ok();
                }
                "user_name" | "_ccun" => {
                    user_name = Some(value.to_string());
                }
                "user_login" | "_ccul" => {
                    user_login = Some(value.to_string());
                }
                "game_id" | "_ccg" => {
                    game_id = Some(value.to_string().into());
                }
                "stream_id" | "_ccs" => {
                    stream_id = Some(value.to_string().into());
                }
                "stream_title" | "_ccst" => {
                    stream_title = Some(value.to_string());
                }
                "started_at" | "_ccsa" => {
                    started_at = DateTime::parse_from_rfc3339(&value).ok();
                }
                "video_id" | "_ccv" => {
                    video_id = Some(value.to_string().into());
                }
                "ping" | "_ccat" => {
                    ping = Some(value.to_string());
                }
                "_ccf" => format_online = Some(value.to_string()),
                "_ccfo" => format_offline = Some(value.to_string()),
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
            format_online: format_online,
            format_offline: format_offline,
        })
    }

    pub(super) fn headline_vod(&self) -> String {
        run_format(
            self.format_offline
                .as_deref()
                .unwrap_or(DEFAULT_OFFLINE_FORMAT),
            self,
        )
    }

    pub(super) fn headline_streaming(&self) -> String {
        run_format(
            self.format_online
                .as_deref()
                .unwrap_or(DEFAULT_ONLINE_FORMAT),
            self,
        )
    }

    pub(super) fn streaming_components(&self) -> Vec<CreateActionRow> {
        vec![CreateActionRow::Buttons(
            [Some(self.stream_button()), self.vod_button()]
                .into_iter()
                .flatten()
                .collect(),
        )]
    }

    pub(super) fn vod_components(&self) -> Option<Vec<CreateActionRow>> {
        self.vod_button()
            .map(|b| vec![CreateActionRow::Buttons(vec![b])])
    }

    pub(super) fn vod_button(&self) -> Option<CreateButton> {
        self.video_id.as_ref().map(|v| {
            CreateButton::new_link(format!("https://twitch.tv/videos/{}", v))
                .label("Watch the VOD!")
        })
    }

    pub(super) fn stream_button(&self) -> CreateButton {
        CreateButton::new_link(format!("https://twitch.tv/{}", self.user_login))
            .label("Watch the stream!")
    }

    pub(super) fn stream_embed(&self) -> CreateEmbed {
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
