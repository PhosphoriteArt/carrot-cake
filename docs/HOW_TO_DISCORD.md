# Some quick setup how-to's for Discord

## Bot Token

- You'll need to set yourself up on the [Discord Developer Portal](https://discord.com/developers/home) and create a new app
- Under "Installation":
  - Make sure "Installation Contexts" is set only to "Guild Install"
  - Under "Default Install Settings":
    - Scopes should include `bot`
    - Permissions should include `Embed Links`, `Read Message History`, `Send Messages`, and `View Channel`
- Under "Bot":
  - Set a username of your choice
  - Click "Reset Token"
  - **This token is your bot token. Use it in your `secrets.env`.

## Find Various IDs

- Discord will tell you the IDs of various things if you enable Developer Mode. 
  - Open the app on your computer and go to Settings > Developer and enable Developer Mode.
- **Server ID**
  - You'll find this by opening your server, clicking its name, then going to Copy Server Info > Copy Server ID
- **Channel ID**
  - You'll find this by right-clicking a channel, and clicking Copy Channel ID
- **Role ID**
  - You can find this one of a couple of ways:
    - Server Settings > Roles: click the `...` button and then Copy Role ID.
    - Find a user with the role; in their details, find the role and right-click it. Click Copy Role ID.
  - **When you're filling out the `ping` field in `config.yml`, a role must be prefixed with `&`
