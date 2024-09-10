from flask import Blueprint, jsonify
import tornado.ioloop
import asyncio
from app.services.nsq_services import NSQService

ingestion_blueprint = Blueprint('ingestion', __name__)

nsq_services = NSQService()

@ingestion_blueprint.route('/dataconsume', methods=['POST'])

def ingest_data():
    try:
        # Start consuming data from NSQ
        nsq_services.consume_from_nsq()

        # Start the Tornado IOLoop in a new thread
        tornado.ioloop.IOLoop.instance().start()

        return jsonify({"message": "Data consumption started"}), 200
    except Exception as e:
        print(f"Failed to start data consumption: {e}")
        return jsonify({"error": "Failed to start data consumption"}), 500
