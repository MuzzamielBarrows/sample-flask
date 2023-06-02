from flask import Flask
from flask import render_template
import base64
import requests
from datetime import datetime, timedelta
from time import sleep

import sqlalchemy as db
from sqlalchemy import exc
import pymysql
from sqlalchemy.orm import sessionmaker

app = Flask(__name__)

end = datetime.now().replace(microsecond=0)
start = end - timedelta(days=1)
def chunk(arr, size):
    return [arr[i:i+size] for i in range(0, len(arr), size)]


def getAllBoxId():
    url = f'https://vidicenter.quividi.com/api/v1/boxes/'
    auth_header = "Basic " + base64.b64encode(
        b"barrowssouthafrica_gibbon:735f0c293ccdb9a04bcb18f3077c6da7c3d37c47").decode("ascii")
    headers = {"Authorization": auth_header}
    boxes_response = requests.get(url, headers=headers).json()

    return boxes_response


def getStoreInformation():
    engine = db.create_engine(
        'mysql+pymysql://doadmin:AVNS_TaeOZGWPqjzs0Bs@mysql-db-do-user-1422871-0.b.db.ondigitalocean.com:25060/defaultdb')

    connection = engine.connect().execution_options(autocommit=False)
    metadata = db.MetaData()
    print('\nconnected')

    # create session objects to manage transactions
    Session = sessionmaker(bind=engine)
    session = Session()

    metadata = db.MetaData()

    # reflect view and print result
    v = db.Table('cv_accessory_ref', metadata,
                 autoload_with=engine, mysql_autoload=True)
    query = v.select()
    result = session.execute(query)

    #result = engine.execute('SELECT * FROM cv_accessory_ref')

    #rows = result.fetchall()

    return result

def getViewersData(box_ids, store_acc_info):
    url = f'https://vidicenter.quividi.com/api/v1/data/?locations=91724,91723,91724,91564,91710,91712,91693,91681,91915,91920,91980,92583,91643,91636,91555,91432,91557,91610,91611,91612,92025,91632,91430,91663,91782,91911,91713,91678,92035,91680,91620,91433,91436,91439,91440,91719,91959,92376,91718,91412,91411,91414,91630,91391,91390,91388,91384,91393,91698,91700,91580,91581,91626,91582,91559,91670,91646,91645,91501,91536,91500,91539,91545,92034,91575,91576,91577,91578,91537,91616,91618,91409,91410,92224,86727,86735,86737,86738,86740,86744,86747,85333,85339,85341,86729,91044,91124,89243,91292,91560,91563,91839,91840,91843,91846,91871,91872,91874,91877,91883,91884,91886,91914,91986,91987,91988,91989,92115,92114,92033,91428,92046,92233,92327,92001,91561,91880,91562,91827,91556,91634,91621,91426,91565&start={start.isoformat()}&end={end.isoformat()}&data_type=viewers&time_resolution=finest'
    print(url)
    auth_header = "Basic " + base64.b64encode(
        b"barrowssouthafrica_gibbon:735f0c293ccdb9a04bcb18f3077c6da7c3d37c47").decode("ascii")
    headers = {"Authorization": auth_header}

    MAX_RETRIES = 3
    WAIT_TIME_MS = 100000
    retries = 0

    while retries <= MAX_RETRIES:
        print('hitting API')
        exportStatusResponse = requests.get(url, headers=headers)

        if exportStatusResponse.ok:
            exportStatus = exportStatusResponse.json()

            if exportStatus["state"] == 'started':
                retries += 1
                waitTimeMs = WAIT_TIME_MS * (2 ** retries)
                sleep(waitTimeMs / 1000)
            else:
                data = []
                for item in exportStatus["data"]:

                    filtered_list = [d for d in box_ids if d.get(
                        'location_id') == item["location_id"]]

                    filtered_acc_info = [
                        d for d in store_acc_info if str(d[0]) == str(filtered_list[0]["id"])]

                    data.append({
                        "id": str(item["location_id"]) + "-" + item["period_start"] + "-" + str(item.get("dwell_time_in_tenths_of_sec")) + "-" + str(item.get("attention_time_in_tenths_of_sec")),
                        "location_id": item["location_id"],
                        "period_start": item["period_start"],
                        "gender": item.get("gender"),
                        "age": item.get("age"),
                        "age_value": item.get("age_value"),
                        "very_unhappy": item.get("very_unhappy"),
                        "unhappy": item.get("unhappy"),
                        "neutral": item.get("neutral"),
                        "happy": item.get("happy"),
                        "very_happy": item.get("very_happy"),
                        "dwell_time_in_tenths_of_sec": item.get("dwell_time_in_tenths_of_sec"),
                        "attention_time_in_tenths_of_sec": item.get("attention_time_in_tenths_of_sec"),
                        # ---ADDED
                        "display_pk": filtered_acc_info[0][1] if len(filtered_acc_info) > 0 else None,
                        "retailer_id": filtered_acc_info[0][2] if len(filtered_acc_info) > 0 else None,
                        "store_id": filtered_acc_info[0][3] if len(filtered_acc_info) > 0 else None,
                        "region": filtered_acc_info[0][4] if len(filtered_acc_info) > 0 else None,
                        "touchpoint_id": filtered_acc_info[0][5] if len(filtered_acc_info) > 0 else None,
                    })

                return data
        else:
            raise Exception(
                f"Failed to fetch export status. HTTP error {exportStatusResponse.status_code}: {exportStatusResponse.text}")


def getOTSData(box_ids, store_acc_info):

    url = f'https://vidicenter.quividi.com/api/v1/data/?locations=91724,91723,91724,91564,91710,91712,91693,91681,91915,91920,91980,92583,91643,91636,91555,91432,91557,91610,91611,91612,92025,91632,91430,91663,91782,91911,91713,91678,92035,91680,91620,91433,91436,91439,91440,91719,91959,92376,91718,91412,91411,91414,91630,91391,91390,91388,91384,91393,91698,91700,91580,91581,91626,91582,91559,91670,91646,91645,91501,91536,91500,91539,91545,92034,91575,91576,91577,91578,91537,91616,91618,91409,91410,92224,86727,86735,86737,86738,86740,86744,86747,85333,85339,85341,86729,91044,91124,89243,91292,91560,91563,91839,91840,91843,91846,91871,91872,91874,91877,91883,91884,91886,91914,91986,91987,91988,91989,92115,92114,92033,91428,92046,92233,92327,92001,91561,91880,91562,91827,91556,91634,91621,91426,91565&start={start.isoformat()}&end={end.isoformat()}&data_type=ots&time_resolution=15m'
    print(url)
    auth_header = "Basic " + base64.b64encode(
        b"barrowssouthafrica_gibbon:735f0c293ccdb9a04bcb18f3077c6da7c3d37c47").decode("ascii")
    headers = {"Authorization": auth_header}

    MAX_RETRIES = 3
    WAIT_TIME_MS = 10000
    retries = 0

    while retries <= MAX_RETRIES:
        print('hitting API')
        exportStatusResponse = requests.get(url, headers=headers)

        if exportStatusResponse.ok:
            exportStatus = exportStatusResponse.json()

            if exportStatus["state"] == 'started':
                retries += 1
                waitTimeMs = WAIT_TIME_MS * (2 ** retries)
                sleep(waitTimeMs / 1000)
            else:
                data = []
                for item in exportStatus["data"]:

                    filtered_list = [d for d in box_ids if d.get(
                        'location_id') == item["location_id"]]

                    filtered_acc_info = [
                        d for d in store_acc_info if str(d[0]) == str(filtered_list[0]["id"])]

                    data.append({
                        "id": str(item["location_id"]) + "-" + item["period_start"],
                        "location_id": item["location_id"],
                        "period_start": item["period_start"],
                        "status": item.get("status"),
                        "ots_count": item.get("ots_count"),
                        "watcher_count": item.get("watcher_count"),
                        "duration": item.get("duration"),
                        # ---ADDED
                        "display_pk": filtered_acc_info[0][1] if len(filtered_acc_info) > 0 else None,
                        "retailer_id": filtered_acc_info[0][2] if len(filtered_acc_info) > 0 else None,
                        "store_id": filtered_acc_info[0][3] if len(filtered_acc_info) > 0 else None,
                        "region": filtered_acc_info[0][4] if len(filtered_acc_info) > 0 else None,
                        "touchpoint_id": filtered_acc_info[0][5] if len(filtered_acc_info) > 0 else None,
                    })

                return data
        else:
            raise Exception(
                f"Failed to fetch export status. HTTP error {exportStatusResponse.status_code}: {exportStatusResponse.text}")

@app.route("/")
def main():
    # /////////////////////////////////////////////////////////////////////////////////////
    # GETTING All BOX ID's
    # /////////////////////////////////////////////////////////////////////////////////////
    print("Getting Box ID's")
    box_ids = getAllBoxId()
    print("Finished Box ID's")
    # /////////////////////////////////////////////////////////////////////////////////////
    # GETTING All STORE ACCESSORY INFORMATION
    # /////////////////////////////////////////////////////////////////////////////////////
    print("Getting Store Meta information")
    store_acc_info = getStoreInformation()
    print("Finished Store Meta information")
    # /////////////////////////////////////////////////////////////////////////////////////
    # GETTING DATA FROM API
    # /////////////////////////////////////////////////////////////////////////////////////
    print("Getting Viewers Data")
    viewers_data = getViewersData(box_ids, store_acc_info)
    print("Finished Viewers Data")

    print("Getting OTS Data")
    ots_data = getOTSData(box_ids, store_acc_info)
    print("Finished OTS Data")
    # /////////////////////////////////////////////////////////////////////////////////////
    # SETTING UP DATABASE CONNECTION
    # /////////////////////////////////////////////////////////////////////////////////////

    print("Inserting Data")
    engine = db.create_engine(
        'mysql+pymysql://doadmin:AVNS_NWrP4l2Rg35GYOrXSMA@quividi-data-do-user-1422871-0.b.db.ondigitalocean.com:25060/defaultdb')

    connection = engine.connect().execution_options(autocommit=False)
    metadata = db.MetaData()
    print('\nconnected')

    # create session objects to manage transactions
    Session = sessionmaker(bind=engine)
    session = Session()

    metadata = db.MetaData()
    viewers_table = db.Table(
        'viewers', metadata, autoload_with=engine, mysql_autoload=True)
    ots_table = db.Table(
        'ots', metadata, autoload_with=engine, mysql_autoload=True)

    # /////////////////////////////////////////////////////////////////////////////////////
    # INSERTING QUIVIDI VIEWERS DATA
    # /////////////////////////////////////////////////////////////////////////////////////

    # viewers_query = db.insert(viewers_table)
    # session.execute(viewers_query, viewers_data)
    # try:
    #    session.execute(viewers_query, viewers_data)
    # except db.exc.IntegrityError as e:
    #    print(e)
    #    if (e.orig.args[0] == 1062):
    #        pass
    #    else:
    #        session.rollback()
    #        raise

    # session.commit()

    viewers_query = db.insert(viewers_table).values(viewers_data).prefix_with('IGNORE')

    try:
        session.execute(viewers_query)
        session.commit()
    except exc.IntegrityError as e:
        # Handle the exception here
        print("Duplicate entry error:", e)
        session.rollback()

    # /////////////////////////////////////////////////////////////////////////////////////
    # INSERTING QUIVIDI OTS DATA
    # /////////////////////////////////////////////////////////////////////////////////////

    ots_query = db.insert(ots_table).values(ots_data).prefix_with('IGNORE')

    try:
        session.execute(ots_query)
        session.commit()
    except exc.IntegrityError as e:
        # Handle the exception here
        print("Duplicate entry error:", e)
        session.rollback()

    print('Completed')

    return {"body": "successful"}
