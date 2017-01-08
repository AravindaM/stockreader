import sys
import toml
import threading

from influxdb import InfluxDBClient
from infrastructure import log, time_series
from admin import admin_api
from stocks import job, read, mongo, influx, domain, download, stocks_api

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

from flask import Flask

from apscheduler.schedulers.background import BackgroundScheduler

NYSE = "nyse"
NASDAQ = "nasdaq"

logger = log.get_logger("stockreader")

def get_config():
    # Read config parameters from a TOML file.
    config = None
    config_file_path = sys.argv[1]
    with open(config_file_path) as config_file:
        config = toml.loads(config_file.read())
    return config

def read_stocks_from_exchange_file(config, exchange):
    exchange_file_path_list = config[exchange]
    stocks_from_exchange = read.read_stocks_from_multiple_files(exchange_file_path_list, exchange)
    return stocks_from_exchange

def get_mongo(mongo_config):
    host = mongo_config["host"]
    port = mongo_config["port"]
    dbname = mongo_config["dbname"]
    mongo_client = mongo.Mongo(host, port, dbname)
    return mongo_client

def get_influx(influx_config):
    host = influx_config["host"]
    port = influx_config["port"]
    dbname = influx_config["dbname"]
    influx_client = InfluxDBClient(host, port, dbname)
    influx_client.create_database(dbname)
    influx_client.switch_database(dbname)
    return influx_client

# Initialize
config = get_config()

## Mongo
mongo_config = config["mongo"]
mongo_persistence = get_mongo(mongo_config)

## Influx
influx_config = config["influx"]
influx_client = get_influx(influx_config)
time_series = time_series.TimeSeries(influx_client)
influx_persistence = influx.Influx(influx_client)

read = read.Read()
download = download.Download()
domain = domain.Domain(mongo_persistence, influx_persistence, download)

scheduler = BackgroundScheduler()
job = job.Job(domain, scheduler, time_series)

# add stocks from files.
exchanges = config["exchanges"]
stocks = read_stocks_from_exchange_file(exchanges, NYSE)
stocks.extend(read_stocks_from_exchange_file(exchanges, NASDAQ))
logger.info("stocks %s", len(stocks))
# Download all stocks data asynchronously on startup.
thread = threading.Thread(target=job.add_stocks_list_to_stockreader, args=[stocks])
thread.start()

# Schedule recurrent stock update jobs.
job.schedule_stock_updates()

# Start the flask server
app = Flask(__name__)
admin_blueprint = admin_api.get_admin_blueprint(time_series)
app.register_blueprint(admin_blueprint, url_prefix='/stockreader/admin')
stocks_api = stocks_api.get_stocks_blueprint(domain, job, time_series)
app.register_blueprint(stocks_api, url_prefix='/stockreader/api/stocks')

server = config["server"]
host = server["host"]
port = server["port"]
debug = server["debug"]
if debug:
    app.run(host=host, port=port, debug=debug)
else:
    http_server = HTTPServer(WSGIContainer(app))
    http_server.bind(port)
    http_server.start(0)  # forks one process per cpu.
    IOLoop.current().start()
