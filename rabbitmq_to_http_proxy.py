import json
import requests
from argparse import ArgumentParser
from rmq_consumer import RmqConsumer


def str_to_dict(string):
    try:
        return json.loads(string)
    except:
        return None


class RabbitMqToHttpProxy(RmqConsumer):
    def __init__(self, url, exchange, queue, routing_key='#', queue_type='topic', durable=True, endpoint=None,
                 verbose=False, prefetch_count=1000):
        RmqConsumer.__init__(self, url=url, exchange=exchange, queue=queue,
                             routing_key=routing_key, queue_type=queue_type, durable=durable,
                             prefetch_count=prefetch_count)

        self.endpoint = endpoint
        self.verbose = verbose
        self.headers = {'content-type': 'application/json'}

    def on_message(self, channel, method, properties, body):
        msg = str_to_dict(body)

        if msg:
            if self.verbose:
                print(msg, end='\t' if self.endpoint else '\n')

            if self.endpoint:
                r = requests.post(self.endpoint, data=json.dumps(msg), headers=self.headers)

                if self.verbose:
                    print(r.status_code, r.reason)

                if r.status_code not in (200, 201):
                    self.nacknowledge_message(method.delivery_tag)
                    return

        self.acknowledge_message(method.delivery_tag)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-a', '--endpoint', dest='endpoint', type=str, required=False, help='Endpoint url to post')
    parser.add_argument('-u', '--url', dest='url', type=str, required=True, help='Url of RabbitMQ server')
    parser.add_argument('-e', '--exchange', dest='exchange', type=str, required=True, help='Exchange to bind')
    parser.add_argument('-q', '--queue', dest='queue', type=str, required=True, help='Name of queue to consume')
    parser.add_argument('-p', '--prefetch_count', dest='prefetch_count', type=int, required=False,
                        default=1000, help='Number of prefetch count')
    parser.add_argument('-r', '--routing_key', dest='routing_key', type=str, required=True,
                        help='Routing key')
    parser.add_argument('-t', '--queue_type', dest='queue_type', type=str, required=False, help='Queue type',
                        default='topic')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                        default=False, help='Verbose')

    args = parser.parse_args()

    if args.verbose:
        print('*' * 50)
        for i in vars(args):
            print(str(i) + ' - ' + str(getattr(args, i)))

        print('*' * 50)

    proxy = RabbitMqToHttpProxy(url=args.url, exchange=args.exchange, queue=args.queue,
                                routing_key=args.routing_key, queue_type=args.queue_type,
                                endpoint=args.endpoint, verbose=args.verbose, prefetch_count=args.prefetch_count)
    proxy.run()