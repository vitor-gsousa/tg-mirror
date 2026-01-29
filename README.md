# tg-mirror

## Configuração

Copiar .env.example para .env
Editar e inserir valores:

* API_ID
* API_HASH
* DEST_CHAT
* SOURCE_CHATS

## Instalação

```shell
docker compose build
```

## Executar o serviço

```shell
docker compose up -d
```

## Primeira execução (login Telegram)

Na primeira vez surge os seguintes inputs:

```yaml
Please enter your phone number:
Please enter the code you received:
```

👉 Introduz:

* número de telefone
* código recebido no Telegram
* password 2FA (se existir)

A sessão fica guardada no volume `/config`.

## Aceder à interface Web

No browser aceder a:
`http://IP_DO_EQUIPAMENTO:8000`
