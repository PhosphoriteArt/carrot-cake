use std::{env, sync::Arc};

use ansi_term::{
    Color::{Black, Yellow},
    Style,
};
use tokio::signal;

use crate::{discord::DiscordConnection, twitch::OnlineClient, util::metrics};

mod discord;
pub(crate) mod twitch;
pub(crate) mod util;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let shutdown = metrics::init().await?;

    log::error!("Hello Error!");
    log::warn!("Hello Warn!");
    log::info!("Hello Info!");
    log::debug!("Hello Debug!");
    if cfg!(feature = "dev") {
        let _ = dotenv::dotenv();
    }

    let discord_token = env::var("BOT_TOKEN").expect("Expected a token in the environment");

    let discord_client = DiscordConnection::new(discord_token).await?;
    let cpy = discord_client.clone();

    let twitch_client_id = env::var("CLIENT_ID").expect("Expected a token in the environment");
    let twitch_client_secret =
        env::var("CLIENT_SECRET").expect("Expected a token in the environment");

    let olwatcher = Arc::new(
        OnlineClient::new(
            twitch_client_id,
            twitch_client_secret,
            ["phosphoriteart"].into_iter(),
        )
        .await?,
    );

    let mut recv = olwatcher.handle();

    tokio::spawn(async move {
        signal::ctrl_c().await.expect("failed to listen to ctrl-c");
        log::warn!("ctrl-c found, shutting down...");
        shutdown();
        olwatcher.close().await;
        cpy.close().await;
    });

    while let Ok(evt) = recv.recv().await {
        println!(
            "{} {}",
            Style::new().on(Yellow).fg(Black).bold().paint("EVT!!!"),
            Style::new().italic().paint(format!("{evt:?}"))
        );
        if let Err(e) = discord_client.update_stream(evt).await {
            log::error!("Error updating discord: {e}")
        }
    }

    Ok(())
}
