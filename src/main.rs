use std::env;

use tokio::{
    signal,
    sync::broadcast::error::RecvError::{Closed, Lagged},
};

use crate::{
    config::{BY_GUILD_ID, CONFIG},
    discord::DiscordConnection,
    twitch::OnlineClient,
    util::{SyncEvent, metrics},
};

pub(crate) mod config;
mod discord;
pub(crate) mod twitch;
pub(crate) mod util;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if cfg!(feature = "dev") {
        let _ = dotenv::dotenv();
        let _ = dotenv::from_filename("secrets.env");
    }

    let shutdown = metrics::init().await?;

    // Ensure config valid as early as possible
    let _ = BY_GUILD_ID;

    let twitch_client_id = env::var("CLIENT_ID").expect("Expected a token in the environment");
    let twitch_client_secret =
        env::var("CLIENT_SECRET").expect("Expected a token in the environment");
    let discord_token = env::var("BOT_TOKEN").expect("Expected a token in the environment");

    let (olwatcher, mut recv, mut reconcile) = OnlineClient::new(
        twitch_client_id,
        twitch_client_secret,
        CONFIG.streams.iter().map(|s| &s.streamer_login),
    )
    .await?;

    let discord_client =
        DiscordConnection::new(discord_token, olwatcher.broadcaster_ids.clone()).await?;
    let cpy = discord_client.clone();

    // Shutdown watcher
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("failed to listen to ctrl-c");
        log::warn!("ctrl-c found, shutting down...");
        olwatcher.close().await;
        log::info!("Twitch shutdown successful");
        cpy.close().await;
        log::info!("Discord shutdown successful");
    });
    let discord_client_copy = discord_client.clone();
    let initial_reconciliation = SyncEvent::new();
    let initial_reconciliation_waiter = initial_reconciliation.clone();
    tokio::spawn(async move {
        while let Ok(evt) = reconcile.recv().await {
            if let Err(e) = discord_client_copy.reconcile(evt).await {
                log::error!("Error updating discord: {e}")
            }
            initial_reconciliation.signal().await;
        }
    });

    initial_reconciliation_waiter.wait().await;
    loop {
        let evt = match recv.recv().await {
            Ok(evt) => evt,
            Err(e) => match e {
                Closed => break,
                Lagged(n) => {
                    log::error!("Lagged, dropped {n} events");
                    continue;
                }
            },
        };
        log::info!("Got twitch event: {evt:?}");
        if let Err(e) = discord_client.update_stream(evt).await {
            log::error!("Error updating discord: {e}")
        }
    }

    log::info!("Shutting down metrics");
    shutdown();
    log::info!("Metrics shutdown successful");

    Ok(())
}
