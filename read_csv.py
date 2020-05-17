# import pandas as pd
import pyorient
# # import pandas as df
from csv import reader

def creating_hospitals_table():
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    # open the database by its name
    client.db_open(dbname, login, password)
    # hospitals_table
    client.command("CREATE class hospitals EXTENDS V")
    client.command("CREATE PROPERTY hospitals.ID INTEGER")
    client.command("CREATE PROPERTY hospitals.NAME STRING")
    client.command("CREATE PROPERTY hospitals.ADDRESS STRING")
    client.command("CREATE PROPERTY hospitals.CITY STRING")
    client.command("CREATE PROPERTY hospitals.STATE STRING")
    client.command("CREATE PROPERTY hospitals.ZIP INTEGER")
    client.command("CREATE PROPERTY hospitals.TYPE STRING")
    client.command("CREATE PROPERTY hospitals.BEDS INTEGER")
    client.command("CREATE PROPERTY hospitals.COUNTY STRING")
    client.command("CREATE PROPERTY hospitals.COUNTYFIPS INTEGER")
    client.command("CREATE PROPERTY hospitals.COUNTRY STRING")
    client.command("CREATE PROPERTY hospitals.LATITUDE FLOAT")
    client.command("CREATE PROPERTY hospitals.LONGITUDE FLOAT")
    client.command("CREATE PROPERTY hospitals.NAICS_CODE INTEGER")
    client.command("CREATE PROPERTY hospitals.WEBSITE STRING")
    client.command("CREATE PROPERTY hospitals.OWNER STRING")
    client.command("CREATE PROPERTY hospitals.TRAUMA STRING")
    client.command("CREATE PROPERTY hospitals.HELIPAD STRING")
    client.command("CREATE PROPERTY hospitals.occupied_beds INTEGER")
    client.command("CREATE PROPERTY hospitals.available_beds INTEGER")
    client.close()

#creating_hospitals_table()



def importing_hospital_data():
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    # open the database by its name
    client.db_open(dbname, login, password)

    with open('hospitals.csv','r') as hospitals_csv:
        csv_reader=reader(hospitals_csv)
        header=next(csv_reader)
        if header != None:
            for row in csv_reader:
                # print("ID is {}, name is {}, address is:{}, CITY is {}, STATE is {}, ZIP is {}, TYPE is {}, BEDS is {}, COUNTY is {}, COUNTYFIPS is {}, COUNTRY is {}, LATITUDE is {}, LONGITUDE is {}, NAICS_CODE is {}, WEBSITE is {}, OWNER is {}, TRAUMA is {}, HELIPAD is {}".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17]))
                # print("")
                client.command("CREATE VERTEX hospitals SET ID="+ str(row[0]))
                row[1] = row[1].replace("'","")
                client.command("UPDATE hospitals SET NAME = '" + str(row[1]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET address= '" + str(row[2]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET CITY= '" + str(row[3]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET STATE= '" + str(row[4]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET ZIP= '" + str(row[5]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET TYPE= '" + str(row[6]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET BEDS= '" + str(row[7]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET COUNTY= '" + str(row[8]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET COUNTYFIPS= '" + str(row[9]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET COUNTRY= '" + str(row[10]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET LATITUDE= '" + str(row[11]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET LONGITUDE= '" + str(row[12]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET NAICS_CODE= '" + str(row[13]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET WEBSITE= '" + str(row[14]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET OWNER= '" + str(row[15]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET TRAUMA= '" + str(row[16]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET HELIPAD= '" + str(row[17]) + "' where ID = " + str(row[0]))
                client.command("UPDATE hospitals SET available_beds= eval('BEDS - occupied_beds')")
                client.command("UPDATE hospitals SET occupied_beds= 0")

    client.close()
#importing_hospital_data()

def creating_kyzipdistance_table():
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    # open the database by its name
    client.db_open(dbname, login, password)

    #kyzipdistance table
    client.command("CREATE CLASS kyzipdistance EXTENDS V")
    client.command("CREATE PROPERTY kyzipdistance.zip_from INTEGER")
    client.command("CREATE PROPERTY kyzipdistance.zip_to INTEGER")
    client.command("CREATE PROPERTY kyzipdistance.distance FLOAT")
    client.close()

#creating_kyzipdistance_table()


def importing_kyzipdistance_data():
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    # open the database by its name
    client.db_open(dbname, login, password)

    with open('kyzipdistance.csv','r') as kyzipdistance_csv:
        csv_reader_kyzipdistance=reader(kyzipdistance_csv)
        header=next(csv_reader_kyzipdistance)
        if header != None:
            for row in csv_reader_kyzipdistance:
                # print("From:{} To:{} is:{}".format(row[0],row[1],row[2]))
                client.command("CREATE VERTEX kyzipdistance SET zip_from="+str(row[0]))
                client.command("UPDATE kyzipdistance SET zip_to= '" +str(row[1])+"'WHERE zip_from="+str(row[0]))
                client.command("UPDATE kyzipdistance SET distance= '"+str(row[2])+"' WHERE zip_from="+str(row[0]))

    client.close()
# importing_kyzipdistance_data()

def creating_patient_table():
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    # open the database by its name
    client.db_open(dbname, login, password)

    # kyzipdistance table
    client.command("CREATE CLASS patient EXTENDS V")
    client.command("CREATE PROPERTY patient.first_name STRING")
    client.command("CREATE PROPERTY patient.last_name STRING")
    client.command("CREATE PROPERTY patient.mrn STRING")
    client.command("CREATE PROPERTY patient.zip_code INTEGER")
    client.command("CREATE PROPERTY patient.patient_status_code INTEGER")
    client.close()

# creating_patient_table()

def creating_alert_state_table():
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("172.31.147.227", 2424)
    session_id = client.connect(login, password)

    # open the database by its name
    client.db_open(dbname, login, password)

    # kyzipdistance table
    client.command("CREATE CLASS alert_state EXTENDS V")
    client.command("CREATE PROPERTY alert_state.zip_code EMBEDDEDLIST STRING")
    client.command("CREATE PROPERTY alert_state.alert_statewide INTEGER")
    client.close()
# creating_alert_state_table()

