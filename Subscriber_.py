import pika
import sys
import json
import time
import threading
import schedule
import pyorient

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
check_zip_code={}
check_zip_code2={}


if not binding_keys:
    sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
    sys.exit(1)

for binding_key in binding_keys:
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=binding_key)

print(' [*] Waiting for logs. To exit pres CTRL+C')
counter = 0
def callback(ch, method, properties, body):
    #print(" [x] %r:%r" % (method.routing_key, body))
    global check_zip_code
    temp = json.loads(body,encoding='utf-8')
    #print("body: {}".format(len(temp)))
    for i in temp:
        if i['zip_code'] in check_zip_code:
            check_zip_code[i['zip_code']]= check_zip_code[i['zip_code']] +1
        else:
            check_zip_code[i['zip_code']] = 1
        # print ("/zip code is: " + i['zip_code'])

def counter():
    threading.Timer(15.0,counter).start()
    global check_zip_code2
    global check_zip_code
    alert_state=[]
    print("the zip_code(1) counter is: ", check_zip_code)
    if len(check_zip_code2) > 0:
        for key,value in check_zip_code2.items():
                if check_zip_code2[key] <= 2 * check_zip_code[key]:
                    print("alert")
                    alert_state.append(key)

                    # print ("we have alert state:",alert_state)
                    # print("we have alert state")
                    # print("key 1 is: ", check_zip_code[key])
                    # print("key 2 is: ",check_zip_code2[key])
                #else:
                    # print("         ")
                    # print("         ")
                    # print("not alert state")
                    # print("key 1 is: ", check_zip_code[key])
                    # print("key 2 is: ",check_zip_code2[key])
    # else:
    #     print("=== first 15 seconds ===")
    #     print("first dict: ", check_zip_code)
    #     print("second dict: ", check_zip_code2)
    #     print("=======================")
    dbname = "finall"
    login = "root"
    password = "rootpwd"
        # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("172.31.147.227", 2424)
    session_id = client.connect(login, password)
    client.db_open(dbname, login, password)

    if len(alert_state) > 0:
        for row in alert_state:
            client.command("UPDATE alert_state ADD zip_code=" + row)
    else:
        print("safe state")
        client.command("UPDATE alert_state set zip_code = null")
    client.close()

    check_zip_code2 = check_zip_code.copy()
    check_zip_code.clear()

counter()

# while True:
#     #schedule.run_pending()
#     time.sleep(15)
#     counter()


channel.basic_consume(
    queue=queue_name, on_message_callback=callback,auto_ack=True)

channel.start_consuming()

