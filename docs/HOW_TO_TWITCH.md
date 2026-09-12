# Some quick setup how-to's for Twitch

## Set up a twitch app

- First, sign into the [Twitch Developer Console](https://dev.twitch.tv/console/apps)
- Click "Register Your Application"
  - Name it whatever you like, users will never see this
  - For "OAuth Redirect URLs," just enter `http://localhost` – we'll also never use this
  - Category doesn't really matter but "Application Integration" makes sense for this
  - Chose Confidential as the client type
  - Then click Create
- Once you're there, note the **Client ID** and generate a new **Client Secret**.
  You'll use these for your `secrets.env` file.
