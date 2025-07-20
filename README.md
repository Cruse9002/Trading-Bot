# Environment Variables Setup

All sensitive configuration (API keys, database URLs, credentials) should be placed in a `.env` file at the project root. This file is loaded automatically by `docker-compose` and all services.

## Example .env file

```
RABBITMQ_HOST=rabbitmq
RABBITMQ_USER=guest
RABBITMQ_PASS=guest
INFLUXDB_URL=http://influxdb:8086
INFLUXDB_TOKEN=your_influxdb_token
INFLUXDB_ORG=your_org
INFLUXDB_BUCKET=market_data
MONGODB_HOST=mongodb
MONGODB_PORT=27017
MONGODB_DB=raw_data_lake
MONGODB_USER=your_mongo_user
MONGODB_PASS=your_mongo_pass
BINANCE_API_KEY=your_binance_api_key
BINANCE_API_SECRET=your_binance_api_secret
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
REDDIT_USER_AGENT=your_reddit_user_agent
NEWSAPI_KEY=your_newsapi_key
TWITTER_BEARER_TOKEN=your_twitter_bearer_token
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
CAPITAL=10000
RISK_PER_TRADE=0.01
```

## Usage
- All services in `docker-compose.yml` use `env_file: .env` to load these variables.
- Each service reads its required variables using `os.environ.get()` in Python.
- **Never commit your `.env` file to version control.**

## Adding New Variables
If you add new services or need new secrets, add them to `.env` and reference them in your code and `docker-compose.yml` as needed. 