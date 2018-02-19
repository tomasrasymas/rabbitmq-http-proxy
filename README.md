# RabbitMQ to HTTP restful API proxy

Consumes messages from single RabbitMQ queue and posts message to restful API.

### Usage
```
python rabbitmq_to_http_proxy.py -u 'amqp://username:userpassword@rabbit_address:port/vhost?heartbeat=10' -e clients -q prices -r '*.prices' -v -a http://127.0.0.1:5000/api/prices
```
