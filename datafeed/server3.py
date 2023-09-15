import csv
import datetime
import http.server
import json
import operator
import os.path
import random
import re
import threading
import time
import urllib.parse

# Configuration
REALTIME = True
SIM_LENGTH = datetime.timedelta(days=365 * 5)
MARKET_OPEN = datetime.datetime.now().replace(hour=0, minute=30, second=0)

# Market Parameters
SPD = (2.0, 6.0, 0.1)
PX = (60.0, 150.0, 1)
FREQ = (12, 36, 50)
OVERLAP = 4

# Data Persistence
DATA_FILE = 'test.csv'

# Server Configuration
HOST = '0.0.0.0'
PORT = 8080

# Lock for data access in a multi-threaded environment
data_lock = threading.Lock()

# Define a class to manage order books
class OrderBook:
    def __init__(self):
        self.buy_book = []
        self.sell_book = []

    def add_order(self, order, size, age=10):
        yield order, size, age
        for o, s, a in self.buy_book if order < 0 else self.sell_book:
            if a > 0:
                yield o, s, a - 1

    def clear_order(self, order, size, book, op=operator.ge, notional=0):
        (top_order, top_size, age), tail = book[0], book[1:]
        if op(order, top_order):
            notional += min(size, top_size) * top_order
            sdiff = top_size - size
            if sdiff > 0:
                return notional, list(self.add_order(tail, top_order, sdiff, age))
            elif len(tail) > 0:
                return self.clear_order(order, -sdiff, tail, op, notional)

    def clear_book(self, buy=None, sell=None):
        while buy and sell:
            order, size, _ = buy[0]
            new_book = self.clear_order(order, size, sell)
            if new_book:
                sell = new_book[1]
                buy = buy[1:]
            else:
                break
        return buy, sell

# Define a class to manage the application state
class App:
    def __init__(self):
        self.order_books = {
            'ABC': OrderBook(),
            'DEF': OrderBook(),
        }
        self.realtime_start = datetime.datetime.now()
        self.simulation_start, _, _ = next(self.order_book('ABC'))
        self.read_initial_data()

    def read_initial_data(self):
        with data_lock:
            if not os.path.isfile(DATA_FILE):
                print("No data found, generating...")
                self.generate_csv()

    def generate_csv(self):
        with open(DATA_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            for t, stock, side, order, size in self.generate_test_data():
                if t > MARKET_OPEN + SIM_LENGTH:
                    break
                writer.writerow([t, stock, side, order, size])

    def generate_test_data(self):
        while True:
            t = datetime.datetime.now()
            stock = 'ABC' if random.random() > 0.5 else 'DEF'
            side, d = ('sell', 2) if random.random() > 0.5 else ('buy', -2)
            order = round(random.normalvariate(PX[0] + (SPD[0] / d), SPD[0] / OVERLAP), 2)
            size = int(abs(random.normalvariate(0, 100)))
            yield t, stock, side, order, size

    def order_book(self, stock):
        while True:
            if stock not in self.order_books:
                yield datetime.datetime.now(), [], []  # Return empty order book for unknown stocks
            else:
                with data_lock:
                    yield datetime.datetime.now(), self.order_books[stock].buy_book, self.order_books[stock].sell_book

# Create an instance of the App class
app = App()

# Define routes for the server
class Routes:
    @staticmethod
    def query(params):
        # Query the top of the order book
        top_bid = None
        top_ask = None

        stock = params.get('stock', None)
        if stock:
            with data_lock:
                _, bids, asks = next(app.order_book(stock))
                if bids:
                    top_bid = {'price': bids[0][0], 'size': bids[0][1]}
                if asks:
                    top_ask = {'price': asks[0][0], 'size': asks[0][1]}

        return {
            'stock': stock,
            'timestamp': str(datetime.datetime.now()),
            'top_bid': top_bid,
            'top_ask': top_ask
        }

# Create an instance of the Routes class
routes = Routes()

# Define a threaded HTTP server
class ThreadedHTTPServer(threading.Thread):
    def __init__(self):
        super().__init__()

    def run(self):
        server = http.server.ThreadingHTTPServer((HOST, PORT), RequestHandler)
        print(f'HTTP server started on port {PORT}')
        server.serve_forever()

# Define a request handler
class RequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args, **kwargs):
        pass

    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        query_dict = urllib.parse.parse_qs(parsed_path.query)

        if parsed_path.path == '/query':
            response_data = routes.query(query_dict)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            response_json = json.dumps(response_data)
            self.wfile.write(bytes(response_json, encoding='utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

# Run the threaded HTTP server
http_server = ThreadedHTTPServer()
http_server.daemon = True
http_server.start()

# Keep the main thread alive
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print('Shutting down...')
    http_server.join()
