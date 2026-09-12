use std::{
    collections::HashMap,
    fmt::Debug,
    ops::Deref,
    sync::{Arc, Mutex},
};

use serenity::futures::future::join_all;
use tokio::sync::broadcast;
use twitch_api::{
    helix::search::SearchChannelsRequest,
    twitch_oauth2::{AppAccessToken, ClientId, ClientSecret},
};

use anyhow::anyhow;

use crate::{
    twitch::{
        client::{InnerOnlineClient, Notification},
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
    #[tracing::instrument]
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
        let conduit = twitch_client.create_conduit(1, &token).await?;

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

            streamer_map.insert(streamer.id, streamer.broadcaster_login.into());
        }

        let client = OnlineClient {
            inner: Arc::new(InnerOnlineClient {
                client: twitch_client,
                curr_token: Mutex::new(token),
                curr_client_id: Mutex::new(None),
                broadcaster_ids: streamer_map,
                conduit_id: conduit.id,
                cast: broadcast::channel(4).0,
                close: SyncEvent::new(),
                closed: SyncEvent::new(),
                seen: Mutex::new(HashMap::new()),
                state: Mutex::new(HashMap::new()),
            }),
        };

        client.run_websocket().await?;
        client.inner.clone().start_prune().await;
        client.inner.clone().start_live_refresh_sync().await;
        client.inner.clone().start_full_refresh_sync().await;

        log::info!("Websocket started");

        Ok(client)
    }

    pub fn handle(&self) -> broadcast::Receiver<Notification> {
        self.inner.cast.subscribe()
    }

    pub async fn close(&self) {
        self.inner.close.signal().await;
        self.inner.closed.wait().await;
    }

    #[tracing::instrument(skip(self))]
    async fn run_websocket(&self) -> anyhow::Result<()> {
        let innerarc = self.inner.clone();
        let close = self.close.clone();
        let closed = self.closed.clone();

        tokio::spawn(async move {
            WebsocketRunner::new(close, innerarc).run().await;
            closed.signal().await;
        });

        Ok(())
    }
}
