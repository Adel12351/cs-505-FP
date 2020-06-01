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
# first dictionary of zip codes
check_zip_code={}
# second dictionary of zip codes to check and trigger the alert state
check_zip_code2={}


if not binding_keys:
    sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
    sys.exit(1)

for binding_key in binding_keys:
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=binding_key)

print(' [*] Waiting for logs. To exit pres CTRL+C')

def callback(ch, method, properties, body):
    #print(" [x] %r:%r" % (method.routing_key, body))
    global check_zip_code
    temp = json.loads(body,encoding='utf-8')
    #print("body: {}".format(temp))

    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("172.31.147.227", 2424)
    session_id = client.connect(login, password)
    # open the database by its name
    client.db_open(dbname, login, password)
    # saving the patient's data in variables
    for i in temp:
        first_name = i['first_name']
        last_name = i['last_name']
        mrn = i['mrn']
        zip_code = i['zip_code']
        print("zip code: ", zip_code)
        patient_status_code = i['patient_status_code']
        patient_data={first_name,last_name,mrn,zip_code}
        print (patient_data)
        if patient_status_code == "0" or patient_status_code == "1" or patient_status_code == "2" or patient_status_code == "4":
            pass
            #client.command("CREATE VERTEX patient SET mrn= '" + mrn +"', first_name = '" + first_name + "', last_name = '"+last_name+"',zip_code = "+ zip_code +",patient_status_code = " + patient_status_code + ", location_code=0" )
            print ("1-the patient is ok!")
        elif patient_status_code == "3":
            nearest_hospital=client.command("SELECT min(distance),zip_to FROM kyzipdistance WHERE zip_from='"+zip_code+"'")
            hospital_id_in_case_3 = client.command("SELECT ID FROM hospitals WHERE ZIP=" + nearest_hospital)
            # client.command("CREATE VERTEX patient SET mrn= '" + mrn + "', first_name = '" + first_name + "', last_name = '" + last_name + "',zip_code = " + zip_code + ",patient_status_code = " + patient_status_code + ", location_code="+hospital_id_in_case_3)
            print("3-nearest hospital for testing is: "+ hospital_id_in_case_3)
        elif patient_status_code == "5":
            nearest_hospital = client.command("SELECT min(distance),zip_to FROM kyzipdistance WHERE zip_from='" + zip_code + "'")
            hospital_id_in_case_5 = client.command("SELECT ID FROM hospitals WHERE ZIP=" + nearest_hospital + " AND available_beds >= 1")
            print ("5- in case 5:", hospital_id_in_case_5)
        elif patient_status_code == "6":
            nearest_hospital = client.command("SELECT min(distance),zip_to FROM kyzipdistance WHERE zip_from='" + zip_code + "'")
            hospital_id_in_case_6 = client.command("SELECT ID FROM hospitals WHERE ZIP=" + nearest_hospital + " AND available_beds >= 1")
            print ("6- closest available Level IV (I > II > III > IV) or better treatment facility",hospital_id_in_case_6)


        if i['zip_code'] in check_zip_code:
            check_zip_code[i['zip_code']]= check_zip_code[i['zip_code']] +1
        else:
            check_zip_code[i['zip_code']] = 1
        # print ("/zip code is: " + i['zip_code'])
     # print("////////////first_name is{}, last_name is :{}, mrn is {}, zip_code is {}, patient_status_code is: {}".format(first_name,last_name,mrn,zip_code,patient_status_code))
    # client.close()


def counter():
    threading.Timer(15.0,counter).start()
    global check_zip_code2
    global check_zip_code
    #list of zip codes that are under alert due to thier growth for RTR1
    alert_state=[]
    print("the zip_code(1) counter is: ", check_zip_code)
    print("the zip_code(2) counter is: ",check_zip_code2)
    if len(check_zip_code2) > 0:
        for key,value in check_zip_code2.items():
                if check_zip_code2[key] <= 2 * check_zip_code[key]:
                    print("alert")
                    # add the zip code to the alert_state list
                    alert_state.append(key)
                    print ("alert state: ",alert_state)

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
    # dbname = "finall"
    # login = "root"
    # password = "rootpwd"
    #     # create client to connect to local orientdb docker container
    # client = pyorient.OrientDB("172.31.147.227", 2424)
    # session_id = client.connect(login, password)
    # client.db_open(dbname, login, password)
    #
    # if len(alert_state) > 0:
    #     for row in alert_state:
    #         client.command("UPDATE alert_state ADD zip_code=" + row)
    #     if len(alert_state) >= 5:
    #         client.command("UPDATE alert_state set alert_statewide = 1")
    #     else:
    #         client.command("UPDATE alert_state set alert_statewide = 0")
    # else:
    #     print("safe state")
    #     client.command("UPDATE alert_state set zip_code = []")
    #     client.command("UPDATE alert_state set alert_statewide = 0")
    #
    # client.close()

    check_zip_code2 = check_zip_code.copy()
    check_zip_code.clear()

# counter()

channel.basic_consume(
    queue=queue_name, on_message_callback=callback,auto_ack=True)

channel.start_consuming()

