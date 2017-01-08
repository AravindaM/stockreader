from datetime import datetime

class Influx:

    CURRENT_DATA = "stock_current_data"
    HISTORICAL_DATA = "stock_historical_data"

    def __init__(self, influx_client):
        self.client = influx_client

    def update_stock_current_data(self, quote, stock_current_data):
        data = {
            "measurement": self.CURRENT_DATA,
            "tags": {},
            "time": datetime.now().isoformat(),
            "fields": stock_current_data
        }
        points = [data]
        self.client.write_points(points)

    def save_stock_historical_data(self, quote, stock_historical_data_array):
        points = []
        for stock_historical_data in stock_historical_data_array:
            date = stock_historical_data['date']
            stock_historical_data.pop('date', None)
            data = {
                "measurement": self.HISTORICAL_DATA,
                "tags": {},
                "time": date,
                "fields": {
                    "symbol": stock_historical_data["symbol"],
                    "open": float(stock_historical_data["open"]),
                    "high": float(stock_historical_data["high"]),
                    "low": float(stock_historical_data["low"]),
                    "close": float(stock_historical_data["close"]),
                    "volume": float(stock_historical_data["volume"]),
                    "adj_close": float(stock_historical_data["adj_close"])
                }
            }
            points = [data]
        # Save the points
        self.client.write_points(points)
