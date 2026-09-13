# Carrot Cake! 🥕

<img src="docs/logo.png" width="200" alt="the icon - it's a cake with the coloration of a carrot and a green tuft in the back. a small pink dollop of cream is on top.">

Carrot Cake is a lil' twitch "now streaming" notifier for Discord!

I made it because I thought it would be a fun project after Penny Snapcube
mentioned on her stream that her current bot was broken and she wished
there was a better one out there; (that's where the name comes from too,
re: her fursona, Babble, is a bunny)

## How to Use

Carrot Cake is meant to be self-hosted; that said:
* it supports multiple channels and servers; you can host it yourself for your friends!
* you can run it as a lil app on your own computer, you don't have to get fancy with it.

**You will need**:
* A Twitch "confidential" application setup (a `Client ID` and `Client Secret`). Don't have one? [Instructions here](docs/HOW_TO_TWITCH.md)
* A Discord bot token. Don't have one? [Instructions here](docs/HOW_TO_DISCORD.md#bot-token)
* The Server ID and Channel ID where you want notifications sent. [Instructions here](docs/HOW_TO_DISCORD.md#find-various-ids)
* (Optional) The role ID you want the bot to ping. [Instructions here](docs/HOW_TO_DISCORD.md#find-various-ids)

### Running it as an app on your computer
* Download the most recent binary from [releases](https://github.com/PhosphoriteArt/carrot-cake/releases) ([or build it yourself from source](#building-from-source))
* Place the binary in a folder of your choice
* Create a new text file called `secrets.env` and put the following in it, replacing the placeholders:

```env
CLIENT_ID="twitch client ID here"
CLIENT_SECRET="twitch client secret here"

BOT_TOKEN="discord bot token here"
```

* [Download `config.yaml.example`](https://raw.githubusercontent.com/PhosphoriteArt/carrot-cake/refs/heads/main/config.yaml.example) and place it next to `secrets.env`. Rename it to `config.yaml` and edit it to your liking:

```yaml
streams:
  - streamer_login: your_twitch_username
    notify:
      - guild_id: "your_server_id"
        channel_id: "your_channel_id"
        ping: "&your_group_id"
```

* Run the binary and you should be good to go!

### Running using Docker

Create `secrets.env` and `config.yaml` as described above, then:

```bash
docker run \
  --env-file /path/to/secrets.env \
  -v "/path/to/config.yaml:/app/config.yaml:ro" \
  phosphoriteart/carrot-cake:latest
```

### Running using Docker Compose

Setup similarly to the above; here's a template you can use!

```yaml
services:
  carrot-cake:
    image: phosphoriteart/carrot-cake:latest
    env_file:
      - /path/to/secrets.env
    stop_signal: SIGINT
    volumes:
      - /path/to/config.yaml:/app/config.yaml:ro
```

### Telemetry

Carrot Cake is pretty thoroughly instrumented with OpenTelemetry. When enabled with `OTEL_ENABLED=true`, it will try to send it to an otel collector running locally on port 4318, the default.

You can look at the [`docker-compose.yaml` file in the repo](https://github.com/PhosphoriteArt/carrot-cake/blob/main/docker-compose.yaml) if you want to see what that might look like!

### Caches

If your bot is colocated in a channel that has a lot of chatter, it may lose
track of what it's posted if it restarts. If this is the case, you can set

```env
DISCORD_CACHE="path/to/cache.json"
```

in your `secrets.env` that will persist message it's sent for a given
stream ID across restarts.

When unset, it defaults to a file called carrot-cake-cache.json in your system's
temporary directory.

We also cache the Twitch Conduit ID in this folder so that we can clean it up if we crash.

## Building from Source

Download and install [rust](https://rust-lang.org/tools/install/); once that's done it should be a very straightforward

```bash
cargo build
```

and you're off to the races. `cargo run` will work out of the box with the setup described above.
