//! Initialization & setup for Twitch integration

use std::{
    collections::HashMap,
    fmt::Debug,
    ops::Deref,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use serenity::futures::future::join_all;
use tokio::{fs, sync::broadcast};
use twitch_api::{
    helix::{search::SearchChannelsRequest, streams::Stream, videos::Video},
    twitch_oauth2::{AppAccessToken, ClientId, ClientSecret},
    types::ConduitId,
};

use anyhow::{anyhow, bail};

use crate::{
    twitch::{
        client::{InnerOnlineClient, Notification},
        websocket::WebsocketRunner,
    },
    util::{
        SyncEvent,
        metrics::{self, EXTERNAL_CALLS, increment},
    },
};

pub(crate) mod client;
pub(in crate::twitch) mod util;
pub(in crate::twitch) mod websocket;

#[derive(Clone)]
pub struct OnlineClient {
    inner: Arc<InnerOnlineClient>,
}

impl Deref for OnlineClient {
    type Target = InnerOnlineClient;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

fn conduit_path() -> PathBuf {
    std::env::temp_dir().join("carrot-cake-conduit.id")
}

async fn get_saved_conduit() -> Option<ConduitId> {
    fs::read(conduit_path())
        .await
        .ok()
        .and_then(|contents| String::from_utf8(contents).ok())
        .and_then(|contents| ConduitId::from_str(&contents).ok())
}
async fn save_conduit(cid: &ConduitId) -> anyhow::Result<()> {
    fs::write(conduit_path(), cid.as_str()).await?;
    Ok(())
}
async fn delete_conduit() -> anyhow::Result<()> {
    fs::remove_file(conduit_path()).await?;
    Ok(())
}

impl OnlineClient {
    #[allow(clippy::type_complexity)]
    #[tracing::instrument(skip(client_secret), name = "OnlineClient::new")]
    pub async fn new<S: ToString>(
        client_id: impl Into<ClientId> + Clone + Debug,
        client_secret: impl Into<ClientSecret> + Clone + Debug,
        streamers: impl Iterator<Item = S> + Debug,
    ) -> anyhow::Result<(
        Self,
        broadcast::Receiver<Notification>,
        broadcast::Receiver<Vec<(Stream, Option<Video>)>>,
    )> {
        let twitch_client = twitch_api::helix::HelixClient::with_client(metrics::client());

        increment!(EXTERNAL_CALLS; "service": "twitch", "endpoint": "get_app_access_token");
        let token: AppAccessToken =
            twitch_api::twitch_oauth2::AppAccessToken::get_app_access_token(
                &twitch_client,
                client_id.clone().into(),
                client_secret.clone().into(),
                vec![],
            )
            .await?;

        if let Some(old_conduit) = get_saved_conduit().await {
            increment!(EXTERNAL_CALLS; "service": "twitch", "endpoint": "delete_conduit");

            if let Err(e) = twitch_client.delete_conduit(old_conduit, &token).await {
                match e {
                    twitch_api::helix::ClientRequestError::HelixRequestDeleteError(
                        twitch_api::helix::HelixRequestDeleteError::Error {
                            status: http::StatusCode::NOT_FOUND,
                            ..
                        },
                    ) => {}
                    e => bail!(e),
                }
            }
        }
        increment!(EXTERNAL_CALLS; "service": "twitch", "endpoint": "create_conduit");
        let conduit = twitch_client.create_conduit(1, &token).await?;
        if let Err(e) = save_conduit(&conduit.id).await {
            log::warn!("Couldn't save conduit ID to tempdir: {e}");
        };

        // find the channels for the given usernames; events
        // have to be subscribed by channel ID.
        let streamers = join_all(streamers.map(|username| {
            let twitch_client = twitch_client.clone();
            let token = token.clone();
            async move {
                increment!(EXTERNAL_CALLS; "service": "twitch", "endpoint": "search_channels");
                twitch_client
                    .req_get(
                        SearchChannelsRequest::query(username.to_string())
                            .first(1)
                            .live_only(false),
                        &token,
                    )
                    .await
                    .map(|res| {
                        res.first()
                            .ok_or_else(|| anyhow!("failed to get twitch streamer"))
                    })
            }
        }))
        .await;

        let mut streamer_map = HashMap::new();
        for streamer in streamers {
            let streamer = streamer??;
            log::info!("mapped {} => {}", streamer.broadcaster_login, streamer.id);

            streamer_map.insert(streamer.id.clone(), streamer);
        }

        let (cast, cast_recv) = broadcast::channel(16);
        let (reconcile, reconcile_recv) = broadcast::channel(16);

        let client = OnlineClient {
            inner: Arc::new(InnerOnlineClient {
                client: twitch_client,
                curr_token: Mutex::new(token),
                curr_client_id: Mutex::new(None),
                broadcaster_ids: streamer_map,
                conduit_id: conduit.id,
                cast,
                reconcile,
                close: SyncEvent::new(),
                ws_closed: SyncEvent::new(),
                live_sync_closed: SyncEvent::new(),
                full_sync_closed: SyncEvent::new(),
                prune_closed: SyncEvent::new(),
                seen: Arc::new(DashMap::new()),
                state: Arc::new(DashMap::new()),
            }),
        };

        // Kickstart all our background tasks
        client.run_websocket().await?;
        client.inner.clone().start_prune().await;
        client.inner.clone().start_live_refresh_sync().await;
        client.inner.clone().start_full_refresh_sync().await;

        log::info!("Websocket started");

        Ok((client, cast_recv, reconcile_recv))
    }

    // Indicate that we're closing out and wait for our background tasks to finish.
    pub async fn close(&self) {
        self.inner.close.signal().await;
        self.inner.ws_closed.wait().await;
        self.live_sync_closed.wait().await;
        self.full_sync_closed.wait().await;
        self.prune_closed.wait().await;
        let token = {
            let Ok(token) = self.curr_token.lock() else {
                return;
            };
            token.clone()
        };

        if let Err(e) = self
            .client
            .delete_conduit(self.conduit_id.clone(), &token.clone())
            .await
        {
            log::error!(
                "Could not clean up conduit {} at shutdown: {e}",
                self.conduit_id.as_str()
            )
        } else {
            let _ = delete_conduit().await;
            log::info!("Cleaned up conduit {} ", self.conduit_id.as_str())
        }
    }

    #[tracing::instrument(skip(self))]
    async fn run_websocket(&self) -> anyhow::Result<()> {
        let innerarc = self.inner.clone();
        let close = self.close.clone();
        let closed = self.ws_closed.clone();

        tokio::spawn(async move {
            WebsocketRunner::new(close, innerarc).run().await;
            closed.signal().await;
        });

        Ok(())
    }
}
