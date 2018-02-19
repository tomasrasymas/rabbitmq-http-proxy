# RabbitMQ to restful API proxy

Consumes messages from single RabbitMQ queue and posts message to restful API.

### Usage
```
usage: rabbitmq_to_http_proxy.py [-h] [-a ENDPOINT] -u URL -e EXCHANGE -q
                                 QUEUE -r ROUTING_KEY [-t QUEUE_TYPE] [-v]

optional arguments:
  -h, --help            show this help message and exit
  -a ENDPOINT, --endpoint ENDPOINT
                        Endpoint url to post
  -u URL, --url URL     Url of RabbitMQ server
  -e EXCHANGE, --exchange EXCHANGE
                        Exchange to bind
  -q QUEUE, --queue QUEUE
                        Name of queue to consume
  -r ROUTING_KEY, --routing_key ROUTING_KEY
                        Routing key
  -t QUEUE_TYPE, --queue_type QUEUE_TYPE
                        Queue type
  -v, --verbose         Verbose
```

Command example:
```
python rabbitmq_to_http_proxy.py -u 'amqp://john:john1234@rabbit.john.com:5672/johnvhost?heartbeat=10' -e clients -q prices -r '*.prices' -v -a http://127.0.0.1:5000/api/prices
```
