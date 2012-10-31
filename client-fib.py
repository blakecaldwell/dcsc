#!/usr/bin/env python
##
## Client that accepts messages encoded as integers
## computes the fibonacci of those numbers and returns
## a string representation
##

import pika
import time,sys,os
import fib_pb2
import logging

if os.getenv('QHOST'):
  QHost = os.getenv('QHOST')
else:
  QHost = "127.0.0.1"

logging.basicConfig(filename="/tmp/client-fib.log", level=logging.INFO)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=QHost))

recvchan = connection.channel()
sendchan = connection.channel()

recvchan.queue_declare(queue='fib_to_compute', durable=True)
recvchan.queue_declare(queue='fib_from_compute', durable=True)

def compute_fib(n):
    a = b = 1
    i = 3
    while ( i <= n):
        c = a + b
        a = b
        b = c	
	i += 1
    return b

def receive_fib(ch, method, properties, body):
    ch.basic_ack(delivery_tag = method.delivery_tag)
    try:
        fiblist = fib_pb2.FibList()
        fiblist.ParseFromString(body)
        for fibInstance in fiblist.fibs:
	    fibInstance.response = str(compute_fib(fibInstance.n))
            print "Sent Fib(",fibInstance.n,") = ", fibInstance.response
            sendchan.basic_publish(exchange='',
                           routing_key='fib_from_compute',
                           body=fiblist.SerializeToString(),
                           properties=pika.BasicProperties(delivery_mode = 2))
    finally:
        pass

while True:
    try:
        import pika
        break
    except ImportError:
        print "Waiting for Pika to become available"
        time.sleep(1)

recvchan.basic_qos(prefetch_count=1)
recvchan.basic_consume(receive_fib, queue='fib_to_compute', no_ack=False)
recvchan.start_consuming()

print "Done.."
sys.exit(0)
