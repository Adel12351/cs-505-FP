import pika
import sys
import json

username = 'student'
password = 'student01'
hostname = '128.163.202.61'
virtualhost = 'patient_feed'
#5672  

credentials = pika.PlainCredentials(username, password)
parameters = pika.ConnectionParameters(hostname,5672,virtualhost,credentials)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

exchange_name = 'patient_data'
channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

result = channel.queue_declare('', exclusive=True)
queue_name = result.method.queue

binding_keys = "#"

if not binding_keys:
    sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
    sys.exit(1)

for binding_key in binding_keys:
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=binding_key)

print(' [*] Waiting for logs. To exit pres CTRL+C')

def callback(ch, method, properties, body):
    #print(" [x] %r:%r" % (method.routing_key, body))
    temp = json.loads(body,encoding='utf-8')
    print("body: {}".format(temp))
    for i in temp:
        # print("i is {}".format(i))
        print("first name is: {}, last name is: {}, mrn is: {}, zip code is: {}, pathient status code is: {}".format(i[0],i[1],i[2],i[3],i[4]))


channel.basic_consume(
    queue=queue_name, on_message_callback=callback,auto_ack=True)

channel.start_consuming()