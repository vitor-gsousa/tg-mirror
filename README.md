# tg-mirror

Tool to mirror messages between Telegram chats.

## Prerequisites

* [Docker](https://www.docker.com/)
* [Docker Compose](https://docs.docker.com/compose/)

## Installation

Clone the repository and enter the project folder:

```bash
git clone https://github.com/vitor-gsousa/tg-mirror.git
cd tg-mirror
```

## Configuration

1. Copy the example file to create your environment file:

   ```bash
   cp .env.example .env
   ```

2. Edit the `.env` file and fill in the variables:

   * `API_ID`: Your Telegram API ID (get it at my.telegram.org).
   * `API_HASH`: Your Telegram API hash.
   * `SESSION_STRING`: The account session string (Pyrogram/Telethon).
   * `DEST_CHAT`: Destination chat ID where messages will be sent.
   * `SOURCE_CHATS`: IDs of source chats to be monitored.
   * `CLEANUP_DAYS`: Number of days to retain processed messages (optional, default: 30).
   * `CLEANUP_TIME`: Time of day to run cleanup in HH:MM format (optional, default: 00:05).
   * `DUP_CODE_REGEX`: Regex to detect alphanumeric codes used for de-duplication (optional, default: `\b[A-Za-z0-9]{6,}\b`).

3. You can configure the cleanup via: **Web interface** (recommended):
   * Log in to the control panel at `http://IP_ADDRESS:8000`
   * Go to the **DB** tab
   * Set **Retention (days)** to your desired value (e.g., 30 days)
   * Click **Save cleanup**

4. Or **Environment file** (`.env`):

   ```env
   CLEANUP_DAYS=30           # Retention period in days
   CLEANUP_TIME=00:05        # Daily run time (HH:MM format, UTC)
   ```

## Database Cleanup

The service automatically removes processed message records older than a configurable retention period. This runs daily at a scheduled time (default 00:05 UTC).

### Notes

* Old records are purged automatically every day at the configured time.
* Set `CLEANUP_DAYS` to `0` to disable automatic cleanup.
* Duplicate prevention still works after cleanup (each message ID is checked before processing).
* The **Clear Database** button allows manual deletion of all history.
* Code de-duplication cache is cleared daily at the configured time.

## Installation and Running

Build and start the service in the background:

```shell
docker compose build # plus `--no-cache` if you want to rebuild from scratch
docker compose up -d
```

## Access the Web interface

In the browser, go to:
`http://IP_ADDRESS:8000`
