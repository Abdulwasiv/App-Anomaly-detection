import json
import clickhouse_connect
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from statsmodels.tsa.statespace.sarimax import SARIMAX
import pandas as pd


class ClickHouseService:
    def __init__(self):
        self.client=clickhouse_connect.get_client(
            host='localhost',
            port='8123',
            username='test',
            password='password'
        )

    # def store_anomaly_data(self, anomaly_data):
    #     query = """
    #     INSERT INTO anomalies_table (eventDateTime, hostname, env, client, cpuTotalPercentage, memoryUsedPercentage, filesystemTotalBytes, anomaly)
    #     VALUES
    #     """
    #     values = ", ".join([f"('{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}', {row[4]}, {row[5]}, {row[6]}, {row[7]})"
    #                         for row in anomaly_data])
    #     query += values

    #     # Execute the query
    #     self.client.command(query)

class FlinkService:
    def __init__(self, nsq_host='127.0.0.1', nsq_port=4150):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.nsq_host = nsq_host
        self.nsq_port = nsq_port

    def consume_and_process(self, clickhouse_service):
        # Set up Kafka consumer (assuming NSQ->Kafka bridge or similar)
        properties = {
            'bootstrap.servers': f'{self.nsq_host}:{self.nsq_port}',
            'group.id': 'flink_consumer'
        }
        consumer = FlinkKafkaConsumer(
            'metrics_data',
            SimpleStringSchema(),
            properties
        )
        
        # Add the source to the execution environment
        data_stream = self.env.add_source(consumer)
        print(data_stream)
        # def process_data(message):
        #     data = json.loads(message)
        #     # Assuming data is a dictionary with the relevant keys
        #     timestamp = pd.to_datetime(data['eventDateTime'])
        #     series_data = pd.Series(data['cpuTotalPercentage'], index=[timestamp])
            
        #     # Apply SARIMA model for anomaly detection
        #     sarima_model = SARIMAX(series_data, order=(1, 1, 1), seasonal_order=(1, 1, 1, 12))
        #     results = sarima_model.fit(disp=False)
        #     predictions = results.predict(start=0, end=len(series_data)-1, dynamic=False)
        #     anomaly_detected = series_data - predictions
            
        #     # Flag anomalies
        #     if anomaly_detected.abs().mean() > 0.1:  # Example threshold
        #         data['anomaly'] = 1
        #     else:
        #         data['anomaly'] = 0
            
        #     return list(data.values())

        # # Process each message
        # processed_data_stream = data_stream.map(process_data)
        
        # # Store the processed data back to ClickHouse
        # processed_data_stream.add_sink(clickhouse_service.store_anomaly_data)

        # # Execute the Flink job
        # self.env.execute("Flink SARIMA Anomaly Detection")

# Example usage
if __name__ == "__main__":
    clickhouse_service = ClickHouseService()
    flink_service = FlinkService()

    # Start Flink to consume and process data
    flink_service.consume_and_process(clickhouse_service)
