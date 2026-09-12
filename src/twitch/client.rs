use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::{Arc, Mutex},
    time::{self, Duration, Instant},
};

use anyhow::bail;
use opentelemetry::KeyValue;
use serenity::futures::StreamExt;
use tokio::sync::broadcast;
use twitch_api::{
    HelixClient,
    helix::streams::Stream,
    twitch_oauth2::{AppAccessToken, TwitchToken},
    types::{ConduitId, UserId},
};

use crate::util::{
    SyncEvent,
    metrics::{TOKEN_REFRESH, TOKEN_TTL, TracedHttpClient},
};

#[derive(Clone, Debug)]
pub enum Notification {
    Online(Stream),
    Update(Stream),
    Offline(Stream),
}

pub struct InnerOnlineClient {
    pub client: HelixClient<'static, TracedHttpClient>,
    pub broadcaster_ids: HashMap<UserId, String>,
    pub conduit_id: ConduitId,

    pub curr_token: Mutex<AppAccessToken>,
    pub curr_client_id: Mutex<Option<String>>,

    pub cast: broadcast::Sender<Notification>,

    pub close: SyncEvent,
    pub closed: SyncEvent,

    pub seen: Mutex<HashMap<String, time::Instant>>,

    pub state: Mutex<HashMap<UserId, Stream>>,
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
                      break;
                    }
                }
            }
        });
    }

    fn prune(&self) {
        let mut seen = self.seen.lock().unwrap();
        let mut to_remove = HashSet::new();
        for (key, v) in seen.iter() {
            if Instant::now().saturating_duration_since(*v) > Duration::from_mins(10) {
                to_remove.insert(key.clone());
            }
        }

        for key in to_remove.into_iter() {
            seen.remove(&key);
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn maybe_refresh_token(&self) -> anyhow::Result<()> {
        let mut tok = self.curr_token.lock().unwrap().clone();
        if tok.expires_in().as_secs() >= 600 {
            TOKEN_TTL.record(tok.expires_in().as_secs(), &[]);
            log::debug!("No token refresh needed, expires in {:?}", tok.expires_in());
            TOKEN_REFRESH.add(1, &[KeyValue::new("refreshed", "false")]);
            return Ok(());
        }

        if let Err(e) = tok.refresh_token(&self.client).await {
            TOKEN_REFRESH.add(1, &[KeyValue::new("refreshed", "error")]);

            return Err(e.into());
        }

        let mut repl_tok = self.curr_token.lock().unwrap();
        *repl_tok = tok;

        log::info!("Auth token refreshed");

        TOKEN_REFRESH.add(1, &[KeyValue::new("refreshed", "true")]);

        Ok(())
    }

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
                      break;
                    }
                }
            }
        });
    }

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
        self.do_sync(self.broadcaster_ids.keys().cloned()).await
    }

    pub async fn sync_live(&self) -> anyhow::Result<()> {
        let keys: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .keys()
            .map(|k| k.to_string())
            .collect();

        self.do_sync(keys.into_iter().map(UserId::from)).await
    }

    #[tracing::instrument(skip_all, fields(users = keys.len()))]
    async fn do_sync(
        &self,
        keys: impl Iterator<Item = UserId> + Clone + ExactSizeIterator,
    ) -> anyhow::Result<()> {
        let observed = {
            let iter = keys.clone().collect();

            let token = { self.curr_token.lock().unwrap().clone() };
            let mut streams = self.client.get_streams_from_ids(&iter, &token);

            let mut observed: HashSet<UserId> = HashSet::new();
            while let Some(next) = streams.next().await {
                let next = next?;
                observed.insert(next.user_id.clone());

                {
                    let mut state = self.state.lock().unwrap();
                    match state.entry(next.user_id.clone()) {
                        Entry::Occupied(mut ent) => {
                            if *ent.get() != next {
                                ent.insert(next.clone());
                                self.cast.send(Notification::Update(next))?;
                            }
                        }
                        Entry::Vacant(ent) => {
                            ent.insert(next.clone());
                            self.cast.send(Notification::Online(next))?;
                        }
                    }
                }
            }

            observed
        };

        for uid in keys {
            if !observed.contains(&uid) {
                let mut state = self.state.lock().unwrap();
                match state.entry(uid) {
                    Entry::Occupied(ent) => {
                        let v = ent.remove();
                        self.cast.send(Notification::Offline(v))?;
                    }
                    Entry::Vacant(_) => {}
                }
            }
        }

        Ok(())
    }

    pub fn dedupe(&self, id: String) -> anyhow::Result<()> {
        let mut seen = self.seen.lock().unwrap();
        let entry = seen.entry(id);
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
