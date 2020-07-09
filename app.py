import threading
import pyorient
from flask import Flask
from flask_json import FlaskJSON, as_json
print("testing app")
app=Flask(__name__)
FlaskJSON(app)
import read_csv


addr="172.31.145.69"

@app.route('/api/getteam')
def getteam():
    details={
        "team_name":"Shark",
        "team_members":"[12377243]",
        "app_status_code":"1"
    }
    return details

@app.route('/api/reset')
def reset():
    try:
        dbname = "finall"
        login = "root"
        password = "rootpwd"
        # create client to connect to local orientdb docker container
        client = pyorient.OrientDB(addr, 2424)
        session_id = client.connect(login, password)
        client.db_open(dbname, login, password)
        client.command("DELETE VERTEX hospitals")
        # client.command("DELETE VERTEX kyzipdistance")
        client.command("DELETE VERTEX patient")
        client.command("DELETE VERTEX alert_state")
        read_csv.importing_hospital_data()
        # read_csv.importing_kyzipdistance_data()
        client.command("CREATE VERTEX alert_state set zip_code = [], alert_statewide = 0")
        client.close()
        result ={
            "reset_status_code": "1"
        }
        return result

    except:
        result = {
            "reset_status_code": "0"
        }
        return result

@app.route('/api/zipalertlist')
def zipalertlist():
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB(addr, 2424)
    session_id = client.connect(login, password)
    client.db_open(dbname, login, password)
    ziplist=client.command("SELECT zip_code from alert_state")
    ziplist=ziplist[0].__getattr__('zip_code')
    client.close()
    list={
        "ziplist":ziplist
    }
    return list

@app.route('/api/alertlist')
def alertlist():
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB(addr, 2424)
    session_id = client.connect(login, password)
    client.db_open(dbname, login, password)
    state_status = client.command("SELECT alert_statewide from alert_state ")
    state_status = state_status[0].__getattr__('alert_statewide')
    client.close()

    status={
        "state_status": state_status
    }
    return status


@app.route('/api/testcount')
def testcount():
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB(addr, 2424)
    session_id = client.connect(login, password)
    client.db_open(dbname, login, password)

    positive_test=client.command("SELECT COUNT(patient_status_code) FROM patient WHERE patient_status_code = 2 OR patient_status_code = 5 OR patient_status_code = 6")
    positive_test = positive_test[0].__getattr__('COUNT')
    negative_test= client.command("SELECT COUNT(patient_status_code) FROM patient WHERE patient_status_code = 1 OR patient_status_code = 4 ")
    negative_test = negative_test[0].__getattr__('COUNT')
    client.close()

    status={
        "positive_test":positive_test,
        "negative_test":negative_test
    }
    return status

@app.route('/api/getpatient/<mrn>')
def getpatient(mrn):
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    # client = pyorient.OrientDB("172.31.147.227", 2424)
    client = pyorient.OrientDB(addr, 2424)
    session_id = client.connect(login, password)
    client.db_open(dbname, login, password)
    location_code = client.command("SELECT location_code FROM patient WHERE mrn = '" + mrn + "'")

    #location_code = location_code[0].__getattr__('location_code')
    client.close()
    if len(location_code)  > 0:
        location_code = location_code[0].__getattr__('location_code')
        patientDetail={
            "mrn":mrn,
            "location_code":location_code
        }
        return patientDetail
    else:
        patientDetail = {
            "mrn": "mrn not found",
            "location_code": "location_code"
        }
        return patientDetail

@app.route("/api/gethospital/<id>")
@as_json
def gethospital(id):
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB(addr, 2424)
    session_id = client.connect(login, password)
    # open the database by its name
    client.db_open(dbname, login, password)

    total_beds=client.command("SELECT BEDS from hospitals WHERE ID="+str(id))
    total_beds = total_beds[0].__getattr__('BEDS')
    available_beds=client.command("SELECT available_beds from hospitals WHERE ID="+str(id))
    available_beds=available_beds[0].__getattr__('available_beds')
    zip_code=client.command("SELECT ZIP from hospitals WHERE ID="+str(id))
    zip_code=zip_code[0].__getattr__('ZIP')
    client.close()

    info={
        "total_beds":total_beds,
        "available_beds":available_beds,
        "zipcode":zip_code
    }
    return info

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8088, debug=True,threaded=True)


