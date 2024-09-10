# import nsq
# import json
# import tornado
# from functools import partial
# import clickhouse_connect
# from datetime import datetime

# class ClickHouseService:
#     def __init__(self):
#         # Initialize the client (synchronously)
#         self.client = clickhouse_connect.get_client(
#             host='localhost', 
#             port=8123, 
#             username='test', 
#             password='password'
#         )

#     def stream_current_data(self):
#         # Get the current timestamp
#         current_time = datetime.now()

#         # Stream only the data with the current timestamp from ClickHouse
#         query = f"""
#         SELECT eventDateTime, hostname, env, client, cpuTotalPercentage, memoryUsedPercentage, filesystemTotalBytes
#         FROM my_table
#         WHERE eventDateTime >= '{current_time.strftime('%Y-%m-%d %H:%M:%S')}'
#         """
        
#         # Fetch results synchronously
#         result = self.client.query(query)
        
#         # Use result.result_rows to access rows
#         for row in result.result_rows:
#             yield row
            
# class NSQService:
#     def __init__(self, nsqd_host='127.0.0.1', nsqd_port=4150):
#         self.writer = nsq.Writer([f'{nsqd_host}:{nsqd_port}'])
#         print(f"NSQ Writer initialized with {nsqd_host}:{nsqd_port}")

#     def publish_to_nsq(self, topic, message):
#         try:
#             # Convert message to JSON
#             # json_message = json.dumps(message, default=str).encode('utf-8')  # Encode JSON message to bytes
#             # print(f"Publishing to topic '{topic}': {json_message}")
#             msg=message
#             # Publish message asynchronously
#             self.writer.pub(topic, msg, self.on_publish)
#         except Exception as e:
#             print(f"Failed to publish message: {e}")

#     def on_publish(self, conn, response):
#         if response == b'OK':
#             print("Message published successfully")
#         else:
#             print(f"Failed to publish message: {response}")

# # Example usage
# if __name__ == "__main__":
#     nsq_service = NSQService()

#     # Define the topic and message to publish
#     topic = "asa"
#     message = {"aa": "aa"}  # Replace with your actual message

#     # Use functools.partial to pass arguments to the publish_to_nsq method
#     publish_callback = partial(nsq_service.publish_to_nsq, topic, message)

#     # Call publish_callback every second
#     tornado.ioloop.PeriodicCallback(publish_callback, 1000).start()

#     # Start the IOLoop to process events
#     nsq.run()

import nsq
import json
import tornado.ioloop
from functools import partial
import clickhouse_connect
from datetime import datetime

class ClickHouseService:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host='localhost', 
            port=8123, 
            username='test', 
            password='password'
        )

    def stream_current_data(self):
        current_time = datetime.now().replace(microsecond=0)

        # Format the timestamp string for ClickHouse
        current_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        query = f"""
        SELECT eventDateTime, hostname, env, client, cpuTotalPercentage, memoryUsedPercentage, filesystemTotalBytes, account_id
        FROM my_table
        WHERE eventDateTime = '{current_time}'
        ORDER BY eventDateTime ASC
        """
        result = self.client.query(query)
        return result.result_rows

class NSQService:
    def __init__(self, nsqd_host='127.0.0.1', nsqd_port=4150):
        self.nsqd_host = nsqd_host
        self.nsqd_port = nsqd_port
        self.writer = nsq.Writer([f'{self.nsqd_host}:{self.nsqd_port}'])

    def publish_to_nsq(self, topic, message):
        try:
            json_message = json.dumps(message, default=str).encode('utf-8')
            print(f"Publishing to topic '{topic}': {json_message}")
            self.writer.pub(topic, json_message, self.on_publish)
        except Exception as e:
            print(f"Failed to publish message: {e}")
            self.reconnect()

    def on_publish(self, conn, response):
        if response == b'OK':
            print("Message published successfully")
        else:
            print(f"Failed to publish message: {response}")
            self.reconnect()

    def reconnect(self):
        print("Reconnecting to NSQ...")
        self.writer = nsq.Writer([f'{self.nsqd_host}:{self.nsqd_port}'])

# Example usage
if __name__ == "__main__":
    clickhouse_service = ClickHouseService()
    nsq_service = NSQService()

    topic = "metrics_data"

    def periodic_publish():
        new_data = clickhouse_service.stream_current_data()
        
        # A set to track published rows (assuming each row is unique)
        published_rows = set()

        for row in new_data:
            # Convert the row into a dictionary with respective field names
            message = {
                "eventDateTime": row[0],
                "hostname": row[1],
                "env": row[2],
                "client": row[3],
                "cpuTotalPercentage": row[4],
                "memoryUsedPercentage": row[5],
                "filesystemTotalBytes": row[6],
                "account_id": row[7]
            }
            if tuple(row) not in published_rows:
                publish_callback = partial(nsq_service.publish_to_nsq, topic, message)
                publish_callback()
                published_rows.add(tuple(row))  # Mark the row as published

    tornado.ioloop.PeriodicCallback(periodic_publish, 1000).start()
    nsq.run()
