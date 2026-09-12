use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::{self, Duration, Instant},
};

use opentelemetry::KeyValue;
use serenity::futures::{StreamExt, future::join_all};
use tokio::{sync::mpsc, time::timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use twitch_api::eventsub::{self, Event, EventsubWebsocketData, Shard, Transport};

use anyhow::{Context, anyhow, bail};

use crate::{
    twitch::{client::InnerOnlineClient, util::twitch_ws_url},
    util::{
        SyncEvent,
        metrics::{TWITCH_MSG_RECEIVED, WS_CONNECT, WS_ERROR, WS_RECEIVED},
    },
};

pub struct WebsocketRunner {
    close: SyncEvent,
    inner: Arc<InnerOnlineClient>,
}

impl WebsocketRunner {
    pub fn new(close: SyncEvent, inner: Arc<InnerOnlineClient>) -> Self {
        Self { close, inner }
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(self) {
        let mut id = 1u16;
        let (reconnect_tx, mut reconnect_rx) = mpsc::channel::<(String, bool)>(1);
        let mut one = Arc::new(WebsocketConnection {
            url: twitch_ws_url(),
            inner: self.inner.clone(),
            reconnect: reconnect_tx.clone(),
            close: self.close.clone(),
            welcome_gate: SyncEvent::new(),
            next_gate: SyncEvent::new(),
            keepalive: AtomicI64::new(0),
            last_keepalive: Mutex::new(Instant::now()),
            is_reconnect: false,
            id,
        });
        id += 1;
        let runner = one.clone();

        let mut ws_closed = SyncEvent::new();
        let ws_inner = ws_closed.clone();
        let mut handle = tokio::spawn(async move {
            runner.run_websocket_inner().await;
            ws_inner.signal().await
        });

        loop {
            log::debug!("Waiting...");
            tokio::select! {
                biased;
                _ = self.close.wait() => {
                  break;
                }
                Some((reconnect_url, is_reconnect)) = reconnect_rx.recv() => {
                    log::info!("Received reconnect for {reconnect_url}");
                    let two = Arc::new(WebsocketConnection {
                        url: reconnect_url,
                        inner: self.inner.clone(),
                        reconnect: reconnect_tx.clone(),
                        close: self.close.clone(),
                        welcome_gate: SyncEvent::new(),
                        next_gate: SyncEvent::new(),
                        keepalive: AtomicI64::new(0),
                        last_keepalive: Mutex::new(Instant::now()),
                        is_reconnect,
                        id
                    });
                    id += 1;
                    let runner = two.clone();
                    ws_closed.signal().await;
                    ws_closed = SyncEvent::new();
                    let ws_inner = ws_closed.clone();
                    let two_handle = tokio::spawn(async move {
                        runner.run_websocket_inner().await;
                        ws_inner.signal().await
                    });
                    log::info!("[Reconnect] Waiting for 2nd welcome...");
                    tokio::select! {
                        biased;
                        _ = self.close.wait() => {
                            log::warn!("closed while waiting for disconnect");
                            break;
                        }
                        _ = two.welcome_gate.wait() => {
                            // OK
                        }
                        _ = ws_closed.wait() => {
                            log::warn!("2nd handle shut down before we observed welcome!");
                            continue;
                        }
                    }

                    log::debug!("[Reconnect] Signalling for 1st to shutdown...");
                    one.next_gate.signal().await;
                    log::debug!("[Reconnect] Waiting for 1st to shutdown...");
                    if let Err(e) = timeout(Duration::from_mins(5), handle).await {
                    log::error!("Failed to await handle? {e:?}")
                    }
                    log::debug!("[Reconnect] Complete");
                    handle = two_handle;
                    one = two;
                }

                _ = ws_closed.wait() => {
                    log::info!("Looks like our current websocket connection closed...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    match reconnect_tx.try_send((twitch_ws_url(), false)) {
                        Ok(_) => {},
                        Err(e) => {
                            log::debug!("Couldn't send closure reconnect: {e:?}")
                        },
                    }
                }

                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    if one.is_likely_dead() {
                      // Closes the current one and acts as if we observed a closure later.
                      log::warn!("Current connection looks dead!");
                      one.next_gate.signal().await;
                    }
                }
            }
        }
    }
}

struct WebsocketConnection {
    url: String,
    inner: Arc<InnerOnlineClient>,
    reconnect: mpsc::Sender<(String, bool)>,
    close: SyncEvent,
    welcome_gate: SyncEvent,
    next_gate: SyncEvent,

    id: u16,
    is_reconnect: bool,

    keepalive: AtomicI64,
    last_keepalive: Mutex<time::Instant>,
}

impl WebsocketConnection {
    fn is_likely_dead(&self) -> bool {
        let last = { *self.last_keepalive.lock().unwrap() };
        let Ok(keepalive) = self.keepalive.load(Ordering::Acquire).try_into() else {
            return false;
        };

        return keepalive > 0
            && Instant::now().saturating_duration_since(last)
                > (Duration::from_secs(keepalive) * 2);
    }

    #[tracing::instrument(skip(self), fields(conn_id = self.id))]
    async fn run_websocket_inner(&self) {
        let observe = |success: bool| {
            WS_CONNECT.add(
                1,
                &[
                    KeyValue::new("success", success.to_string()),
                    KeyValue::new("reconnect", self.is_reconnect.to_string()),
                ],
            )
        };

        log::debug!("[WS] Connecting to {}...", self.url);
        let mut ws_stream = match connect_async(self.url.clone()).await {
            Ok((ws_stream, _)) => ws_stream,
            Err(err) => {
                log::warn!("Error connecting to Twitch websockets: {err:?}");
                WS_ERROR.add(1, &[KeyValue::new("err", "connect")]);
                observe(false);
                return;
            }
        };
        observe(true);
        log::debug!("[WS] Connected to {}", self.url);

        while !self.close.is_set() {
            let Some(next) = (tokio::select! {
                biased;
                frame = ws_stream.next() => {
                    frame
                }
                _ = self.close.wait() => {
                    return
                }
                _ = self.next_gate.wait() => {
                    return
                }
            }) else {
                log::warn!("Unexpectedly disconnected from twitch?");
                break;
            };
            let next = match next {
                Ok(message) => {
                    log::debug!("[WS] Got message");
                    message
                }
                Err(e) => {
                    log::error!("Error while reading websocket: {e:?}");
                    WS_ERROR.add(1, &[KeyValue::new("err", "read_ws")]);
                    break;
                }
            };

            match self.handle_websocket_msg(next).await {
                Ok(stay_connected) => {
                    if !stay_connected {
                        if let Err(e) = ws_stream.close(None).await {
                            log::warn!("Error while closing: {e:?}");
                        };
                        WS_ERROR.add(1, &[KeyValue::new("err", "close_intentional")]);
                        break;
                    }
                }
                Err(e) => {
                    log::warn!("Error handling websocket message: {e}");
                    WS_ERROR.add(1, &[KeyValue::new("err", "close_unknown")]);
                }
            }
        }
    }

    #[tracing::instrument(skip(self, msg), fields(conn_id = self.id, msg_type = msg_type(&msg)))]
    async fn handle_websocket_msg(&self, msg: Message) -> anyhow::Result<bool> {
        WS_RECEIVED.add(1, &[KeyValue::new("msg_type", msg_type(&msg))]);

        match msg {
            Message::Text(evt) => {
                let parsed = Event::parse_websocket(&evt).context("parsing failure")?;

                self.handle_event_data(parsed).await
            }
            Message::Binary(_) => unimplemented!(),
            Message::Ping(_) | Message::Pong(_) => Ok(true),
            Message::Close(close_frame) => {
                bail!(
                    "Websocket closed? {}",
                    close_frame
                        .map(|f| f.reason)
                        .unwrap_or("<no reason provided>".into())
                )
            }
            Message::Frame(_) => unreachable!(),
        }
    }

    #[tracing::instrument(skip(self), fields(conn_id = self.id))]
    async fn handle_event_data(&self, parsed: EventsubWebsocketData<'_>) -> anyhow::Result<bool> {
        log::debug!("[WS:T] Got message");
        TWITCH_MSG_RECEIVED.add(1, &[KeyValue::new("msg_type", twitch_msg_type(&parsed))]);

        match parsed {
            EventsubWebsocketData::Welcome {
                metadata: m,
                payload,
            } => {
                self.inner.dedupe(m.message_id.to_string())?;
                if let Some(keepalive) = payload.session.keepalive_timeout_seconds {
                    self.keepalive.store(keepalive, Ordering::Release);
                }

                if !self.is_reconnect {
                    let sid = {
                        let mut ccid = self.inner.curr_client_id.lock().unwrap();
                        let sid = payload.session.id.into_owned();
                        *ccid = Some(sid.clone());
                        sid
                    };
                    self.welcome_gate.signal().await;
                    let token = { self.inner.curr_token.lock().unwrap().clone() };
                    self.inner
                        .client
                        .update_conduit_shards(
                            self.inner.conduit_id.clone(),
                            &[Shard::new("0", Transport::websocket(sid))],
                            &token,
                        )
                        .await?;

                    let results = join_all(self.inner.broadcaster_ids.keys().cloned().map(|k| {
                        let token = token.clone();
                        async move {
                            let online = self
                                .inner
                                .client
                                .create_eventsub_subscription(
                                    eventsub::stream::StreamOnlineV1::broadcaster_user_id(
                                        k.clone(),
                                    ),
                                    eventsub::Transport::conduit(self.inner.conduit_id.clone()),
                                    &token,
                                )
                                .await;
                            let offline = self
                                .inner
                                .client
                                .create_eventsub_subscription(
                                    eventsub::stream::StreamOfflineV1::broadcaster_user_id(k),
                                    eventsub::Transport::conduit(self.inner.conduit_id.clone()),
                                    &token,
                                )
                                .await;

                            (online, offline)
                        }
                    }))
                    .await;

                    let errors: Vec<_> = results
                        .into_iter()
                        .flat_map(|(a, b)| {
                            [
                                a.map_err(|e| anyhow!(e)).err(),
                                b.map_err(|e| anyhow!(e)).err(),
                            ]
                        })
                        .filter_map(|x| x)
                        .map(|err| format!("- {err}"))
                        .collect();

                    if !errors.is_empty() {
                        bail!("Multiple errors occured:\n{}", errors.join("\n"))
                    }
                } else {
                    self.welcome_gate.signal().await;
                }
                Ok(true)
            }
            EventsubWebsocketData::Keepalive {
                metadata: m,
                payload: _,
            } => {
                *self.last_keepalive.lock().unwrap() = Instant::now();
                self.inner.dedupe(m.message_id.to_string())?;
                self.inner.maybe_refresh_token().await?;
                Ok(true)
            }
            EventsubWebsocketData::Notification {
                metadata: m,
                payload,
            } => {
                self.inner.dedupe(m.message_id.to_string())?;

                match payload {
                    Event::StreamOnlineV1(p) => match p.message {
                        eventsub::Message::Notification(online) => {
                            self.inner.notify(online.broadcaster_user_id).await?;
                            Ok(true)
                        }
                        evt => bail!("Unknown message type {evt:?}"),
                    },
                    Event::StreamOfflineV1(p) => match p.message {
                        eventsub::Message::Notification(offline) => {
                            self.inner.notify(offline.broadcaster_user_id).await?;
                            Ok(true)
                        }
                        evt => bail!("Unknown message type {evt:?}"),
                    },

                    evt => bail!("Unknown event {evt:?}"),
                }
            }
            EventsubWebsocketData::Revocation {
                metadata: m,
                payload,
            } => {
                self.inner.dedupe(m.message_id.to_string())?;
                log::warn!("Got revocation, will reconnect: {payload:?}");
                Ok(false)
            }
            EventsubWebsocketData::Reconnect {
                metadata: m,
                payload,
            } => {
                self.inner.dedupe(m.message_id.to_string())?;

                let url = payload
                    .session
                    .reconnect_url
                    .ok_or_else(|| anyhow!("Reconnect data did not have reconnect address?"))?;

                self.reconnect
                    .send((url.into(), true))
                    .await
                    .context("send reconnect failed")?;

                Ok(true)
            }
            unknown => bail!("Got unknown event type {unknown:?}??"),
        }
    }
}

fn msg_type(message: &Message) -> &'static str {
    match message {
        Message::Text(_) => "Message::Text",
        Message::Binary(_) => "Message::Binary",
        Message::Ping(_) => "Message::Ping",
        Message::Pong(_) => "Message::Pong",
        Message::Close(_) => "Message::Close",
        Message::Frame(_) => "Message::Frame",
    }
}

fn twitch_msg_type(message: &EventsubWebsocketData<'_>) -> &'static str {
    match message {
        EventsubWebsocketData::Welcome {
            metadata: _,
            payload: _,
        } => "EventsubWebsocketData::Welcome",
        EventsubWebsocketData::Keepalive {
            metadata: _,
            payload: _,
        } => "EventsubWebsocketData::Keepalive",
        EventsubWebsocketData::Notification {
            metadata: _,
            payload: _,
        } => "EventsubWebsocketData::Notification",
        EventsubWebsocketData::Revocation {
            metadata: _,
            payload: _,
        } => "EventsubWebsocketData::Revocation",
        EventsubWebsocketData::Reconnect {
            metadata: _,
            payload: _,
        } => "EventsubWebsocketData::Reconnect",
        _ => "unknown",
    }
}
