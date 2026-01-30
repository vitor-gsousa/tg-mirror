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

## Installation and Running

Build and start the service in the background:

```shell
docker compose build # plus `--no-cache` if you want to rebuild from scratch
docker compose up -d
```

## Access the Web interface

In the browser, go to:
`http://IP_ADDRESS:8000`
