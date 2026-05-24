import logging

from flask import Flask, jsonify, request
from dotenv import load_dotenv

load_dotenv()

import config
from consumer import RabbitConsumer
from redis_cache import RedisCache


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


cache = RedisCache()
consumer = RabbitConsumer(cache)

app = Flask(__name__)


@app.get("/health")
def health():
    return jsonify(
        {
            "status": "healthy",
            "service": "early-warning",
            "alerts_generated": cache.get_alerts_generated(),
        }
    )


@app.get("/alerts/recent")
def alerts_recent():
    limit_raw = request.args.get("limit", "10")
    try:
        limit = int(limit_raw)
    except ValueError:
        limit = 10
    return jsonify({"alerts": cache.get_recent_alerts(limit)})


if __name__ == "__main__":
    consumer.start()
    app.run(host="0.0.0.0", port=config.FLASK_PORT, debug=False, use_reloader=False)
