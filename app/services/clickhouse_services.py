from clickhouse_driver import Client

class ClickHouseService:
    def __init__(self, host='localhost', port=9000):
        self.client = Client(host=host, port=port)

    def get_data(self, account_id, metric):
        try:
            # Query ClickHouse for data where account_id and metric match
            query = f"""
            SELECT * FROM your_table
            WHERE account_id = '{account_id}' AND metric = '{metric}'
            """
            result = self.client.execute(query)
            return result
        except Exception as e:
            print(f"Failed to query ClickHouse: {e}")
            return None
