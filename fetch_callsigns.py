import requests
import time
from bs4 import BeautifulSoup
from sqlalchemy import (
    create_engine,
    text,
    select,
    update,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    bindparam,
)
from sqlalchemy.sql.expression import func
import re

POINTS_URL = "https://vota.arrl.org/callPoints.php"
NOT_FOUND_RE = re.compile("was not found in the points table.$")

engine = create_engine("postgresql+psycopg2://localhost:5433/ussjoin")
metadata_obj = MetaData()

vota_points_table = Table(
    "vota_points",
    metadata_obj,
    Column("callsign", String, primary_key=True),
    Column("points", Integer),
    Column("role", String),
    Column("role_abbrev", String),
    Column("end_date", DateTime),
    Column("result_string", String),
)


def store_callsigns(callsign_structs):
    # print(arr)

    stmt = (
        update(vota_points_table)
        .where(vota_points_table.c.callsign == bindparam("bind_callsign"))
        .values(points=bindparam("points"), result_string=bindparam("result_string"))
    )

    with engine.begin() as conn:
        conn.execute(stmt, callsign_structs)


def fetch_callsign_points(callsign):
    r = requests.post(POINTS_URL, data={"callsign": callsign})
    soup = BeautifulSoup(r.text, "html.parser")
    p = soup.find("p", {"class": "info"})
    if p is None:
        return None

    pp = p.find("p")
    if pp is None:
        return None

    if NOT_FOUND_RE.search(pp.text) is None:
        strongs = pp.find_all("strong")
        if len(strongs) < 2:
            return None
        pointstring = strongs[1].text
        points = int(pointstring.split()[0])
        return {"bind_callsign": callsign, "points": points, "result_string": pp.text}
    else:
        return {"bind_callsign": callsign, "points": 0, "result_string": pp.text}

    return None


def get_callsigns():
    NUMBER_TO_FETCH = 10
    ret_arr = []
    with engine.connect() as conn:
        stmt = (
            select(vota_points_table.c.callsign)
            .where(
                vota_points_table.c.points == None,
                vota_points_table.c.result_string == None,
            )
            .order_by(func.random())
            .limit(NUMBER_TO_FETCH)
        )
        # print(stmt)
        for row in conn.execute(stmt):
            ret_arr.append(row[0])
    return ret_arr


if __name__ == "__main__":
    while True:
        fail_count = 0
        print("Fetching a batch of callsigns.")
        callsigns = get_callsigns()
        callsign_structs = []
        give_up = False
        for callsign in callsigns:
            ret = None
            while ret is None and not give_up:
                ret = fetch_callsign_points(callsign)
                if ret is None:
                    fail_count = fail_count + 1
                    print(
                        f"\tERROR: Problem while fetching {callsign}. Failure {fail_count}."
                    )
                    if fail_count >= 5:
                        print("\tERROR: Giving up on batch.")
                        give_up = True
                    else:
                        time.sleep(5)
                else:
                    callsign_structs.append(ret)
            if give_up:
                break
        if give_up:
            print("\tERROR: Gave up, sleeping for 60 seconds.")
            time.sleep(60)
        else:  # Successfully finished batch
            print(callsign_structs)
            if len(callsign_structs) > 0:
              store_callsigns(callsign_structs)
              print(f"\tSUCCESS: Stored {len(callsign_structs)} callsigns.")
              time.sleep(5)
            else:
              print("\tEND: No more callsigns need to be updated, terminating.")
              exit(0)
