#!/usr/bin/env python
##
## Client that accepts messages encoded as integers
## computes the fibonacci of those numbers and returns
## a string representation
##

import pika
import threading,time,sys
import fib_pb2

QHost = ""

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=QHost))

recvchan = connection.channel()
sendchan = connection.channel()

recvchan.queue_declare(queue='fib_to_compute', durable=True)
recvchan.queue_declare(queue='fib_from_compute', durable=True)

def receive_fib(ch, method, properties, body):
    ch.basic_ack(delivery_tag = method.delivery_tag)
    try:
        fiblist = fib_pb2.FibList()
        fiblist.ParseFromString(body)
        for fibInstance in fiblist.fibs:
            print "Fib(",fibInstance.n,") = ", fibInstance.response
    finally:
        pass

class RecvThread(threading.Thread):
    def run(self):
        recvchan.basic_qos(prefetch_count=1)
        recvchan.basic_consume(receive_fib, queue='fib_from_compute', no_ack=False)
        recvchan.start_consuming()

recvThread = RecvThread()
recvThread.daemon = True
recvThread.start()
while True:
    try:
        import pika
        break
    except ImportError:
        print "Waiting for Pika to become available"
        time.sleep(1)

    print "Sent.."
    sys.exit(0)
