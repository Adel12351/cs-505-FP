import pika
import sys
import  json
import time
import threading
import schedule
import pyorient

username = 'student'
password = 'student01'
hostname = '128.163.202.61'
virtualhost = 'patient_feed'
addr = "172.31.145.69"
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
    print("body: {}".format(temp))

    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB(addr, 2424)
    session_id = client.connect(login, password)
    # open the database by its name
    client.db_open(dbname, login, password)
    # # saving the patient's data in variables
    for i in temp:
        first_name = i['first_name']
        last_name = i['last_name']
        mrn = i['mrn']
        zip_code = i['zip_code']
        # print("zip code: ", type(zip_code))
        patient_status_code = i['patient_status_code']
        patient_data={first_name,last_name,mrn,zip_code}
        print ("patient ID is: ",mrn)

        if patient_status_code == "0" or patient_status_code == "1" or patient_status_code == "2" or patient_status_code == "4":
            #print ("patient_status_code = 0, 1, 2 or 4")
            client.command("CREATE VERTEX patient SET mrn= '" + mrn +"', first_name = '" + first_name + "', last_name = '"+last_name+"',zip_code = "+ zip_code +",patient_status_code = " + patient_status_code + ", location_code=0" )

        elif patient_status_code == "3":
            temp_result3 = client.command("SELECT distance ,zip_to FROM kyzipdistance WHERE zip_from = " + zip_code + " and zip_to in (select ZIP from hospitals) order by distance asc")
            if len(temp_result3) >= 1:
                print("length of 33 is: ",len(temp_result3))
                temp_result = temp_result3[0].__getattr__('zip_to')
                print("333333333333333",temp_result)
                hospital_id_in_case_3 = client.command("SELECT ID FROM hospitals WHERE ZIP=" +str( temp_result))
                hospital_id_in_case_3 = hospital_id_in_case_3[0].__getattr__('ID')
                # print(hospital_id_in_case_3)
                client.command("CREATE VERTEX patient SET mrn= '" + mrn + "', first_name = '" + first_name + "', last_name = '" + last_name + "',zip_code = " + zip_code + ",patient_status_code = " + patient_status_code + ", location_code=" +str( hospital_id_in_case_3))
            #print ("3333333333: ", hospital_id_in_case_3)

            # nearest_hospital=client.command("SELECT min(distance),zip_to FROM kyzipdistance WHERE zip_from='"+zip_code+"'")
            # hospital_id_in_case_3 = client.command("SELECT ID FROM hospitals WHERE ZIP=" + nearest_hospital)
            # client.command("CREATE VERTEX patient SET mrn= '" + mrn + "', first_name = '" + first_name + "', last_name = '" + last_name + "',zip_code = " + zip_code + ",patient_status_code = " + patient_status_code + ", location_code="+hospital_id_in_case_3)
            # print("3-nearest hospital for testing is: "+ hospital_id_in_case_3)
            else:
                pass

        elif patient_status_code == "5":
            temp_result5 = client.command("SELECT distance ,zip_to FROM kyzipdistance WHERE zip_from = "+ zip_code +" and zip_to in (select ZIP from hospitals WHERE available_beds >= 1) order by distance asc")
            print("length of 55 is: ",len(temp_result5))
            if len(temp_result5) >= 1:
                
                temp_result = temp_result5[0].__getattr__('zip_to')
                print("5555555555555", temp_result)
                hospital_id_in_case_5 = client.command("SELECT ID FROM hospitals WHERE ZIP="+ str(temp_result))
                hospital_id_in_case_5= hospital_id_in_case_5[0].__getattr__('ID')
            # print(hospital_id_in_case_5)
                client.command("CREATE VERTEX patient SET mrn= '" + mrn + "', first_name = '" + first_name + "', last_name = '" + last_name + "',zip_code = " + zip_code + ",patient_status_code = " + str(patient_status_code) + ", location_code="+ str(hospital_id_in_case_5))
                client.command("UPDATE hospitals SET occupied_beds= eval('occupied_beds + 1') WHERE ID="+ str(hospital_id_in_case_5))
                client.command("UPDATE hospitals SET available_beds= eval('BEDS - occupied_beds') WHERE ID="+ str(hospital_id_in_case_5))
            #print ("55555555555:  ", hospital_id_in_case_5)
            else:
                pass
            # nearest_hospital = client.command("SELECT min(distance),zip_to FROM kyzipdistance WHERE zip_from='" + zip_code + "'")
            # hospital_id_in_case_5 = client.command("SELECT ID FROM hospitals WHERE ZIP=" + nearest_hospital + " AND available_beds >= 1")
            # client.command("CREATE VERTEX patient SET mrn= '" + mrn + "', first_name = '" + first_name + "', last_name = '" + last_name + "',zip_code = " + zip_code + ",patient_status_code = " + patient_status_code + ", location_code="+hospital_id_in_case_5)
            # client.command("UPDATE hospitals SET occupied_beds= eval('occupied_beds + 1') WHERE ID ='" + hospital_id_in_case_5 +"'")
            # print ("5- in case 5:", hospital_id_in_case_5)

        elif patient_status_code == "6":
            temp_result6 = client.command("SELECT distance ,zip_to FROM kyzipdistance WHERE zip_from = " + zip_code + " and zip_to in (select ZIP from hospitals WHERE available_beds >= 1 AND TRAUMA != 'NOT AVAILABLE') order by distance asc")
            print("length of 66 is: ", len(temp_result6))
            if len(temp_result6) >= 1:
                temp_result = temp_result6[0].__getattr__('zip_to')
                print("66666666666",temp_result)
                hospital_id_in_case_6 = client.command("SELECT ID FROM hospitals WHERE ZIP=" + str(temp_result))
                hospital_id_in_case_6 = hospital_id_in_case_6[0].__getattr__('ID')
            # print(hospital_id_in_case_6)
                client.command("CREATE VERTEX patient SET mrn= '" + mrn + "', first_name = '" + first_name + "', last_name = '" + last_name + "',zip_code = " + zip_code + ",patient_status_code = " + str (patient_status_code) + ", location_code=" + str( hospital_id_in_case_6))
                client.command("UPDATE hospitals SET occupied_beds= eval('occupied_beds + 1') WHERE ID="+ str(hospital_id_in_case_6))
                client.command("UPDATE hospitals SET available_beds= eval('BEDS - occupied_beds') WHERE ID="+ str(hospital_id_in_case_6))
            else:
                pass
            #print("666666666:  ", hospital_id_in_case_6)
            
            # nearest_hospital = client.command("SELECT min(distance),zip_to FROM kyzipdistance WHERE zip_from='" + zip_code + "'")
            # hospital_id_in_case_6 = client.command("SELECT ID FROM hospitals WHERE ZIP=" + nearest_hospital + " AND available_beds >= 1")
            # client.command("CREATE VERTEX patient SET mrn= '" + mrn + "', first_name = '" + first_name + "', last_name = '" + last_name + "',zip_code = " + zip_code + ",patient_status_code = " + patient_status_code + ", location_code="+hospital_id_in_case_6)
            # client.command("UPDATE hospitals SET occupied_beds= occupied_beds + 1 WHERE ID ='"+hospital_id_in_case_6 +"'")
            # print ("6- closest available Level IV (I > II > III > IV) or better treatment facility",hospital_id_in_case_6)

        if i['zip_code'] in check_zip_code:
            check_zip_code[i['zip_code']]= check_zip_code[i['zip_code']] +1
        else:
            check_zip_code[i['zip_code']] = 1
        # print ("/zip code is: " + i['zip_code'])
     # print("////////////first_name is{}, last_name is :{}, mrn is {}, zip_code is {}, patient_status_code is: {}".format(first_name,last_name,mrn,zip_code,patient_status_code))
    client.close()


def counter():
    threading.Timer(15.0,counter).start()
    global check_zip_code2
    global check_zip_code
    #list of zip codes that are under alert due to thier growth for RTR1
    alert_state=[]
    if len(check_zip_code2) > 0:
        print("1111111",check_zip_code)
        print("222222",check_zip_code2)
        #for key,value in check_zip_code2.items():
                #if check_zip_code2[key] <= 2 * check_zip_code[key]:
                    # add the zip code to the alert_state list
                    #alert_state.append(key)
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    client = pyorient.OrientDB(addr, 2424)
    session_id = client.connect(login, password)
    client.db_open(dbname, login, password)

    if len(alert_state) > 0:
        client.command("UPDATE alert_state set zip_code = []")
        client.command("UPDATE alert_state set alert_statewide = 0")
        print("alert")
        for row in alert_state:
            client.command("UPDATE alert_state ADD zip_code=" + row)
            # print("we  added {} to zip_code array in orientDB".format(row))
        if len(alert_state) >= 5:
            pass
            client.command("UPDATE alert_state set alert_statewide = 1")
            #print("we set alert_statewaide = 1")
        else:

            client.command("UPDATE alert_state set alert_statewide = 0")
            #print("we set alert_statewaide = 0")
    else:
        print("safe state")
        
        client.command("UPDATE alert_state set zip_code = []")
        client.command("UPDATE alert_state set alert_statewide = 0")

    client.close()

    check_zip_code2 = check_zip_code.copy()
    check_zip_code.clear()

counter()

channel.basic_consume(
    queue=queue_name, on_message_callback=callback,auto_ack=True)

channel.start_consuming()

# SELECT distance ,zip_to FROM kyzipdistance WHERE zip_from = 40353 and zip_to in (select ZIP from hospitals WHERE available_beds >= 1 AND TRAUMA != 'NOT AVAILABLE') order by distance asc

