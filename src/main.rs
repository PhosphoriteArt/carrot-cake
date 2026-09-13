use std::env;

use tokio::signal;

use crate::{
    config::{BY_GUILD_ID, CONFIG},
    discord::DiscordConnection,
    twitch::{OnlineClient, client::TwitchMessage},
    util::metrics,
};

pub(crate) mod config;
mod discord;
pub(crate) mod twitch;
pub(crate) mod util;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if cfg!(feature = "dev") {
        let _ = dotenv::dotenv();
        let _ = dotenv::from_filename("secret.env");
    }

    let shutdown = metrics::init().await?;

    // Ensure config valid as early as possible
    let _ = BY_GUILD_ID;

    let twitch_client_id = env::var("CLIENT_ID").expect("Expected a token in the environment");
    let twitch_client_secret =
        env::var("CLIENT_SECRET").expect("Expected a token in the environment");
    let discord_token = env::var("BOT_TOKEN").expect("Expected a token in the environment");

    let olwatcher = OnlineClient::new(
        twitch_client_id,
        twitch_client_secret,
        CONFIG.streams.iter().map(|s| &s.streamer_login),
    )
    .await?;
    let mut recv = olwatcher.handle();

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

    while let Ok(evt) = recv.recv().await {
        log::info!("Got twitch event: {evt:?}");
        let cli = discord_client.clone();
        let _ = tokio::spawn(async move {
            match evt {
                TwitchMessage::Delta(notification) => {
                    if let Err(e) = cli.update_stream(notification).await {
                        log::error!("Error updating discord: {e}")
                    }
                }
                TwitchMessage::Reconcile(items) => {
                    if let Err(e) = cli.reconcile(items).await {
                        log::error!("Error updating discord: {e}")
                    }
                }
            }
        });
    }

    log::info!("Shutting down metrics");
    shutdown();
    log::info!("Metrics shutdown successful");

    Ok(())
}
