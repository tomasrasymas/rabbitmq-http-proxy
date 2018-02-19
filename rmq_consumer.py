from pika import adapters
import pika


class RmqConsumer():
    def __init__(self, url, exchange, queue, routing_key='#', queue_type='topic', durable=True):
        self.url = url
        self.exchange = exchange
        self.queue = queue
        self.type = queue_type
        self.routing_key = routing_key
        self.durable = durable
        self.auto_delete = False
        self.channel = None
        self.consumer_tag = None

        self._connection = None
        self._channel = None
        self._closing = False

    def connect(self):
        return adapters.AsyncioConnection(pika.URLParameters(self.url), self.on_connection_open,
                                          self.on_open_error)

    def on_open_error(self, connection, error):
        self._connection.add_timeout(5, self.reconnect)

    def close_connection(self):
        self._connection.close()

    def add_on_connection_close_callback(self):
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, unused_connection):
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        if not self._closing:
            self._connection = self.connect()

    def add_on_channel_close_callback(self):
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self._connection.close()

    def on_channel_open(self, channel):
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def setup_exchange(self, exchange_name):
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.type,
                                       durable=self.durable,
                                       auto_delete=self.auto_delete)

    def on_exchange_declareok(self, unused_frame):
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name):
        self._channel.queue_declare(self.on_queue_declareok, queue_name,
                                    durable=self.durable, auto_delete=self.auto_delete)

    def on_queue_declareok(self, method_frame):
        self._channel.queue_bind(self.on_bindok, self.queue,
                                 self.exchange, self.routing_key)

    def add_on_cancel_callback(self):
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        if self._channel:
            self._channel.close()

    def acknowledge_message(self, delivery_tag):
        self._channel.basic_ack(delivery_tag)

    def nacknowledge_message(self, delivery_tag):
        self._channel.basic_nack(delivery_tag)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        raise NotImplementedError

    def on_cancelok(self, unused_frame):
        self.close_channel()

    def stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(self.on_cancelok, self.consumer_tag)

    def start_consuming(self):
        self.add_on_cancel_callback()
        self.consumer_tag = self._channel.basic_consume(self.on_message,
                                                        self.queue)

    def on_bindok(self, unused_frame):
        self.start_consuming()

    def close_channel(self):
        self._channel.close()

    def open_channel(self):
        self._connection.channel(on_open_callback=self.on_channel_open)

    def run(self):
        self._connection = self.connect()

        try:
            self._connection.ioloop.start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
