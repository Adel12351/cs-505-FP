import threading
import pyorient
from flask import Flask
from flask_json import FlaskJSON, as_json
print("testing app")
app=Flask(__name__)
FlaskJSON(app)

@app.route('/api/getteam')
def getteam():
    details={
        "team_name":"Shark",
        "team_members":"[12377243]",
        "app_status_code":"0"
    }
    return details

@app.route('/api/reset')
def reset():
    result ={
        "reset_status_code": "1"
    }
    return result

@app.route('/api/zipalertlist')
def zipalertlist():
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
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
    status={
        "state_status": "0"
    }
    return status

@app.route('/api/testcount')
def testcount():
    status={
        "positive_test":"0",
        "negative_test":"0"
    }
    return status
@app.route('/api/getpatient/<mrn>')
def getpatient(mrn):
    patientDetail={
        "mrn":mrn,
        "location_code":"12345"
    }
    return patientDetail

@app.route("/api/gethospital/<id>")
@as_json
def gethospital(id):
    dbname = "finall"
    login = "root"
    password = "rootpwd"
    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
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
        "zip_code":zip_code
    }
    return info

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8088, debug=True,threaded=True)


