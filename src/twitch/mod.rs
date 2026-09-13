//! Initialization & setup for Twitch integration

use std::{
    collections::HashMap,
    fmt::Debug,
    ops::Deref,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use serenity::futures::future::join_all;
use tokio::sync::broadcast;
use twitch_api::{
    helix::search::SearchChannelsRequest,
    twitch_oauth2::{AppAccessToken, ClientId, ClientSecret},
};

use anyhow::anyhow;

use crate::{
    twitch::{
        client::{InnerOnlineClient, TwitchMessage},
        websocket::WebsocketRunner,
    },
    util::{
        SyncEvent,
        metrics::{self},
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

impl OnlineClient {
    #[tracing::instrument(name = "OnlineClient::new")]
    pub async fn new<S: ToString>(
        client_id: impl Into<ClientId> + Clone + Debug,
        client_secret: impl Into<ClientSecret> + Clone + Debug,
        streamers: impl Iterator<Item = S> + Debug,
    ) -> anyhow::Result<Self> {
        let twitch_client = twitch_api::helix::HelixClient::with_client(metrics::client());
        let token: AppAccessToken =
            twitch_api::twitch_oauth2::AppAccessToken::get_app_access_token(
                &twitch_client,
                client_id.clone().into(),
                client_secret.clone().into(),
                vec![],
            )
            .await?;

        // Twitch has a limit of how many conduits that can be defined
        // at one time; since we are the only user of our own conduits,
        // find and delete any leftovers from e.g. a previous crash
        match twitch_client.get_conduits(&token).await {
            Ok(conduits) => {
                let futs = conduits.into_iter().map(|cond| {
                    let c = twitch_client.clone();
                    let tok = token.clone();
                    tokio::spawn(
                        async move { (c.delete_conduit(cond.id.clone(), &tok).await, cond.id) },
                    )
                });
                let results = join_all(futs).await;
                for result in results {
                    match result {
                        Ok((inner, id)) => match inner {
                            Ok(_) => {
                                log::info!("deleted conduit {id}")
                            }
                            Err(err) => log::warn!("err deleting conduit {id}: {err}"),
                        },
                        Err(err) => log::warn!("err with conduit task: {err}"),
                    }
                }
            }
            Err(e) => {
                log::warn!("Error getting conduits: {e}")
            }
        }
        // ...then recreate a single-sharded conduit for webhook delivery
        let conduit = twitch_client.create_conduit(1, &token).await?;

        // find the channels for the given usernames; events
        // have to be subscribed by channel ID.
        let streamers = join_all(streamers.map(|username| {
            let twitch_client = twitch_client.clone();
            let token = token.clone();
            async move {
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

        let client = OnlineClient {
            inner: Arc::new(InnerOnlineClient {
                client: twitch_client,
                curr_token: Mutex::new(token),
                curr_client_id: Mutex::new(None),
                broadcaster_ids: streamer_map,
                conduit_id: conduit.id,
                cast: broadcast::channel(16).0,
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

        Ok(client)
    }

    // returns a listener for the events we're pushing out
    pub fn handle(&self) -> broadcast::Receiver<TwitchMessage> {
        self.inner.cast.subscribe()
    }

    // Indicate that we're closing out and wait for our background tasks to finish.
    pub async fn close(&self) {
        self.inner.close.signal().await;
        self.inner.ws_closed.wait().await;
        self.live_sync_closed.wait().await;
        self.full_sync_closed.wait().await;
        self.prune_closed.wait().await;
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
