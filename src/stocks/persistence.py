from datetime import datetime
from infrastructure import log

logger = log.get_logger("persistence")


class Persistence:

    STOCKS = "stocks"
    STOCKS_CURRENT_DATA = "stocks_current_data"
    HISTORICAL_DATA_SUFIX = "_historical_data"

    def create_stocks_table_if_not_exists(self):
        connection = self.mariadb.get_connection()
        cursor = connection.cursor()
        create_template = ("CREATE TABLE IF NOT EXISTS " + self.STOCKS + " "
                           "(symbol varchar(128) PRIMARY KEY,"
                           "stock_market varchar(128) NOT NULL,"
                           "name varchar(256))")
        cursor.execute(create_template)
        cursor.close()
        connection.close()

    def create_stocks_current_data_table_if_not_exists(self):
        connection = self.mariadb.get_connection()
        cursor = connection.cursor()
        create_template = ("CREATE TABLE IF NOT EXISTS " + self.STOCKS_CURRENT_DATA + " "
                           "(symbol varchar(128), date DATETIME,"
                           "name varchar(256), stock_exchange varchar(128),"
                           "market_capitalization varchar(16), `change` varchar(16),"
                           "days_range varchar(32), average_daily_volume DECIMAL(12, 2),"
                           "volume DECIMAL(12, 2), days_low DECIMAL(12, 2), days_high DECIMAL(12, 2),"
                           "year_low DECIMAL(12, 2), year_high DECIMAL(12, 2),"
                           "last_trade_price_only DECIMAL(12, 2), PRIMARY KEY (symbol, date))")
        cursor.execute(create_template)
        cursor.close()
        connection.close()

    def __init__(self, mariadb):
        self.mariadb = mariadb
        self.create_stocks_table_if_not_exists()
        self.create_stocks_current_data_table_if_not_exists()

    def add_to_stocks(self, stock):
        try:
            connection = self.mariadb.get_connection()
            cursor = connection.cursor()
            add_template = ("INSERT INTO  " + self.STOCKS + " "
                            "(stock_market, symbol, name) "
                            "VALUES "
                            "('{0}', '{1}', '{2}')")
            query = add_template.format(stock["stockMarket"], stock["symbol"],
                                        stock["name"])
            cursor.execute(query)
        except Exception as e:
            logger.exception(e)
        finally:
            self.mariadb.close(cursor, connection)

    def stock_exists(self, quote):
        try:
            connection = self.mariadb.get_connection()
            cursor = connection.cursor()
            select_template = "SELECT COUNT(*) FROM " + self.STOCKS + " WHERE symbol = '{0}'"
            query = select_template.format(quote)
            cursor.execute(query)
            result = cursor.fetchone()
            rows = result[0]
            exists = (rows > 0)
        except Exception as e:
            logger.exception(e)
        finally:
            self.mariadb.close(cursor, connection)
            return exists

    def get_stock_list(self):
        try:
            connection = self.mariadb.get_connection()
            cursor = connection.cursor()
            select_template = "SELECT * FROM " + self.STOCKS
            cursor.execute(select_template)
            results = cursor.fetchall()
        except Exception as e:
            logger.exception(e)
        finally:
            self.mariadb.close(cursor, connection)
            return results

    def upsert_stock_current_data(self, quote, stock_current_data):
        try:
            connection = self.mariadb.get_connection()
            cursor = connection.cursor()
            upsert_template = ("INSERT INTO " + self.STOCKS_CURRENT_DATA + " "
                            "(symbol, date, name, stock_exchange, market_capitalization, days_range, "
                            "`change`, average_daily_volume, volume, days_low, days_high, year_low, "
                            "year_high, last_trade_price_only) "
                            "VALUES "
                            "('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}) "
                            "ON DUPLICATE KEY UPDATE symbol = '{0}'")
            query = upsert_template.format(
                stock_current_data["symbol"],
                datetime.now(),
                stock_current_data["name"],
                stock_current_data["stock_exchange"],
                stock_current_data["market_capitalization"],
                stock_current_data["days_range"],
                float(stock_current_data["change"]),
                float(stock_current_data["average_daily_volume"]),
                float(stock_current_data["volume"]),
                float(stock_current_data["days_low"]),
                float(stock_current_data["days_high"]),
                float(stock_current_data["year_low"]),
                float(stock_current_data["year_high"]),
                float(stock_current_data["last_trade_price_only"])
            )
            cursor.execute(query)
        except Exception as e:
            logger.exception(e)
        finally:
            self.mariadb.close(cursor, connection)

    def create_stock_historical_data_table_if_not_exists(self, table_name):
        try:
            connection = self.mariadb.get_connection()
            cursor = connection.cursor()
            create_template = ("CREATE TABLE IF NOT EXISTS {0} "
                            "(symbol varchar(128), date DATE, "
                            "volume DECIMAL(12, 2), low DECIMAL(12, 2), "
                            "high DECIMAL(12, 2), open DECIMAL(12, 2), "
                            "close DECIMAL(12, 2), adj_close DECIMAL(12, 2),"
                            "PRIMARY KEY (symbol, date))")
            query = create_template.format(table_name)
            cursor.execute(query)
        except Exception as e:
            logger.exception(e)
        finally:
            self.mariadb.close(cursor, connection)

    def add_stock_historical_data(self, quote, stock_historical_data_array):
        try:
            connection = self.mariadb.get_connection()
            cursor = connection.cursor()
            table_name = quote + self.HISTORICAL_DATA_SUFIX
            self.create_stock_historical_data_table_if_not_exists(table_name)
            add_template = ("INSERT IGNORE INTO " + table_name + " "
                            "(symbol, date, volume, low, high, open, close, adj_close) "
                            "VALUES "
                            "(%s, %s, %s, %s, %s, %s, %s, %s)")
            data = []
            for stock_historical_data in stock_historical_data_array:
                params = (
                    stock_historical_data["symbol"],
                    stock_historical_data["date"],
                    float(stock_historical_data["volume"]),
                    float(stock_historical_data["low"]),
                    float(stock_historical_data["high"]),
                    float(stock_historical_data["open"]),
                    float(stock_historical_data["close"]),
                    float(stock_historical_data["adj_close"])
                )
                data.append(params)

            cursor.executemany(add_template, data)
        except Exception as e:
            logger.exception(e)
            logger.error(add_template)
            logger.error(data[0])
        finally:
            self.mariadb.close(cursor, connection)
