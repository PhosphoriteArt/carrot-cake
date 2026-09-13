use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{self, Duration, Instant},
};

use anyhow::bail;
use dashmap::{DashMap, Entry};
use serenity::futures::StreamExt;
use tokio::sync::broadcast;
use twitch_api::{
    HelixClient,
    helix::{
        search::Channel,
        streams::Stream,
        videos::{self, Video},
    },
    twitch_oauth2::{AppAccessToken, TwitchToken},
    types::{ConduitId, UserId},
};

use crate::util::{
    SyncEvent,
    metrics::{EXTERNAL_CALLS, TOKEN_REFRESH, TOKEN_TTL, TracedHttpClient, increment, record},
};

#[derive(Clone, Debug)]
pub enum Notification {
    Online(Stream, Option<Video>),
    Update(Stream, Option<Video>),
    Offline(Stream, Option<Video>),
}

impl Notification {
    pub fn stream(&self) -> &Stream {
        match self {
            Notification::Online(stream, _)
            | Notification::Update(stream, _)
            | Notification::Offline(stream, _) => stream,
        }
    }
    pub fn video(&self) -> Option<&Video> {
        match self {
            Notification::Online(_, video)
            | Notification::Update(_, video)
            | Notification::Offline(_, video) => video.as_ref(),
        }
    }
}

pub struct InnerOnlineClient {
    pub client: HelixClient<'static, TracedHttpClient>,
    pub broadcaster_ids: HashMap<UserId, Channel>,
    pub conduit_id: ConduitId,

    pub curr_token: Mutex<AppAccessToken>,
    pub curr_client_id: Mutex<Option<String>>,

    pub cast: broadcast::Sender<Notification>,
    pub reconcile: broadcast::Sender<Vec<(Stream, Option<Video>)>>,

    pub close: SyncEvent,
    pub ws_closed: SyncEvent,
    pub live_sync_closed: SyncEvent,
    pub full_sync_closed: SyncEvent,
    pub prune_closed: SyncEvent,

    // Twitch doesn't guarantee exactly-once events,
    // and asks to deduplicate events by event ID, and ignore events
    // older than 10min. We prune this map in the background every 10min.
    pub seen: Arc<DashMap<String, time::Instant>>,

    pub state: Arc<DashMap<UserId, (Stream, Option<Video>)>>,
}

impl InnerOnlineClient {
    pub async fn start_prune(self: Arc<InnerOnlineClient>) {
        tokio::spawn(async move {
            loop {
                self.prune();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_mins(10)) => {
                        continue;
                    }
                    _ = self.close.wait() => {
                        self.prune_closed.signal().await;
                        break;
                    }
                }
            }
        });
    }

    fn prune(&self) {
        let mut to_remove = HashSet::new();
        for ent in self.seen.iter() {
            if Instant::now().saturating_duration_since(*ent.value()) > Duration::from_mins(10) {
                to_remove.insert(ent.key().clone());
            }
        }

        for key in to_remove.into_iter() {
            self.seen.remove(&key);
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn maybe_refresh_token(&self) -> anyhow::Result<()> {
        let mut tok = self.curr_token.lock().unwrap().clone();
        if tok.expires_in().as_secs() >= 600 {
            record!(TOKEN_TTL, tok.expires_in().as_secs());
            log::debug!("No token refresh needed, expires in {:?}", tok.expires_in());
            increment!(TOKEN_REFRESH; "refreshed": "false");
            return Ok(());
        }

        increment!(EXTERNAL_CALLS; "service": "twitch", "endpoint": "refresh_token");
        if let Err(e) = tok.refresh_token(&self.client).await {
            increment!(TOKEN_REFRESH; "refreshed": "error");

            return Err(e.into());
        }

        let mut repl_tok = self.curr_token.lock().unwrap();
        *repl_tok = tok;

        log::info!("Auth token refreshed");

        increment!(TOKEN_REFRESH; "refreshed": "true");

        Ok(())
    }

    // We periodically sync just in case we miss an event or if we crashed
    // and need to rehydrate state.
    pub async fn start_full_refresh_sync(self: Arc<InnerOnlineClient>) {
        tokio::spawn(async move {
            loop {
                log::info!("Refreshing full state...");
                if let Err(e) = self.full_sync().await {
                    log::warn!("Error while refreshing full state: {e}");
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_mins(10)) => {
                        continue;
                    }
                    _ = self.close.wait() => {
                        self.full_sync_closed.signal().await;
                        break;
                    }
                }
            }
        });
    }

    // Sync more often for folks who are live so we also capture metadata
    // updates like if they change their title or game
    pub async fn start_live_refresh_sync(self: Arc<InnerOnlineClient>) {
        tokio::spawn(async move {
            loop {
                log::info!("Refreshing live state...");
                if let Err(e) = self.sync_live().await {
                    log::warn!("Error while refreshing live state: {e}");
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_mins(1)) => {
                        continue;
                    }
                    _ = self.close.wait() => {
                        self.live_sync_closed.signal().await;
                        break;
                    }
                }
            }
        });
    }

    pub async fn notify(&self, updated: UserId) -> anyhow::Result<()> {
        log::info!("Saw new event for {updated}, syncing...");
        self.do_sync([updated].into_iter()).await
    }

    pub async fn full_sync(&self) -> anyhow::Result<()> {
        self.do_sync(self.broadcaster_ids.keys().cloned()).await?;
        self.reconcile.send(
            self.state
                .iter()
                .map(|ent| (ent.value().0.clone(), ent.value().1.clone()))
                .collect(),
        )?;

        Ok(())
    }

    pub async fn sync_live(&self) -> anyhow::Result<()> {
        let keys: Vec<_> = self.state.iter().map(|e| e.key().to_string()).collect();

        self.do_sync(keys.into_iter().map(UserId::from)).await
    }

    #[tracing::instrument(skip_all, fields(users = keys.len()))]
    async fn do_sync(
        &self,
        keys: impl ExactSizeIterator<Item = UserId> + Clone,
    ) -> anyhow::Result<()> {
        let observed = {
            let iter = keys.clone().collect();

            let token = { self.curr_token.lock().unwrap().clone() };
            increment!(EXTERNAL_CALLS; "service": "twitch", "endpoint": "get_streams");
            let mut streams = self.client.get_streams_from_ids(&iter, &token);

            let mut observed: HashSet<UserId> = HashSet::new();
            while let Some(next) = streams.next().await {
                let next = next?;
                observed.insert(next.user_id.clone());

                // Find the most recent video to use for the VOD link
                increment!(EXTERNAL_CALLS; "service": "twitch", "endpoint": "get_videos");
                let video = self
                    .client
                    .req_get(
                        {
                            let mut req = videos::GetVideosRequest::default();
                            req.user_id = Some(next.user_id.clone().into());
                            req.type_ = Some(videos::VideoTypeFilter::Archive);
                            req.sort = Some(videos::Sort::Time);
                            req.period = Some(videos::VideoPeriod::Day);
                            req
                        },
                        &token,
                    )
                    .await
                    .inspect_err(|e| {
                        log::warn!(
                            "Could not find vod for stream {} (UID {}): {e}",
                            &next.id,
                            &next.user_login
                        )
                    })
                    .ok()
                    .and_then(|f| f.first());

                {
                    match self.state.entry(next.user_id.clone()) {
                        Entry::Occupied(mut ent) => {
                            let orig = ent.get();
                            if (&orig.0, &orig.1) != (&next, &video) {
                                ent.insert((next.clone(), video.clone()));
                                self.cast.send(Notification::Update(next, video))?;
                            }
                        }
                        Entry::Vacant(ent) => {
                            ent.insert((next.clone(), video.clone()));
                            self.cast.send(Notification::Online(next, video))?;
                        }
                    }
                }
            }

            observed
        };

        for uid in keys {
            if !observed.contains(&uid) {
                match self.state.entry(uid) {
                    Entry::Occupied(ent) => {
                        let v = ent.remove();
                        self.cast.send(Notification::Offline(v.0, v.1))?;
                    }
                    Entry::Vacant(_) => {}
                }
            }
        }

        Ok(())
    }

    pub fn dedupe(&self, id: String) -> anyhow::Result<()> {
        let entry = self.seen.entry(id);
        match entry {
            Entry::Occupied(mut ent) => {
                if Instant::now().saturating_duration_since(*ent.get()) < Duration::from_mins(10) {
                    bail!("ID previously seen");
                } else {
                    ent.insert(Instant::now());
                }
            }
            Entry::Vacant(ent) => {
                ent.insert(Instant::now());
            }
        };

        Ok(())
    }
}
