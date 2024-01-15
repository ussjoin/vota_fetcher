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
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import ProgrammingError
import psycopg2
import re

usa_match_re = re.compile("^([0-9]+) \(USA\)$")
state_match_re = re.compile("^([0-9]+) \(([A-Z]{2})\)$")

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

vota_rankings_table = Table(
    "vota_rankings",
    metadata_obj,
    Column("callsign", String, primary_key=True),
    Column("world_rank", Integer, nullable=False),
    Column("usa_rank", Integer),
    Column("state", String),
    Column("state_rank", Integer),
    Column("qso_count", Integer, nullable=False),
    Column("point_count", Integer, nullable=False),
)


def store_callsigns(arr):
    # print(arr)
    with engine.connect() as conn:
        insertion_arr = [{"callsign": i} for i in arr]
        conn.execute(
            text(
                "INSERT INTO vota_points (callsign) VALUES (:callsign) ON CONFLICT DO NOTHING"
            ),
            insertion_arr,
        )
        conn.commit()


def store_rankings(rankings_structs):
    # print(arr)

    stmt = insert(vota_rankings_table).values(
        callsign=bindparam("callsign"),
        world_rank=bindparam("world_rank"),
        usa_rank=bindparam("usa_rank"),
        state=bindparam("state"),
        state_rank=bindparam("state_rank"),
        qso_count=bindparam("qso_count"),
        point_count=bindparam("point_count"),
    )

    update_stmt = stmt.on_conflict_do_update(
        constraint="vota_rankings_pkey",
        set_=dict(
            world_rank=stmt.excluded.world_rank,
            usa_rank=stmt.excluded.usa_rank,
            state=stmt.excluded.state,
            state_rank=stmt.excluded.state_rank,
            qso_count=stmt.excluded.qso_count,
            point_count=stmt.excluded.point_count,
        ),
    )

    with engine.begin() as conn:
        try:
            conn.execute(update_stmt, rankings_structs)
        except ProgrammingError as e:
            # This gets thrown when there are duplicate callsigns *on the same page*
            # ...come on, ARRL. Do better.
            # In the meantime, run them each individually, that'll work.
            with engine.begin() as conn2:
                for r in rankings_structs:
                    conn2.execute(update_stmt, [r])


if __name__ == "__main__":
    LEADERBOARD_URL = "https://vota.arrl.org/leaderboard.php?page="
    COUNTER = 1
    STOP = 722
    err_happened = False
    while COUNTER <= STOP:
        if err_happened:
            print(f"\tERROR: Failed to grab page {COUNTER}, going to sleep for 5min.")
            time.sleep(300)
            err_happened = False

        list_of_callsigns = []
        rankings_list = []

        print(f"Fetching page {COUNTER}")
        r = requests.get(f"{LEADERBOARD_URL}{COUNTER}")
        if r.status_code != 200:
            err_happened = True
            next

        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table", {"class": "leadTable"})
        for row in table.find_all("tr"):
            # Overall Rank | Call Sign | Country Rank | State Rank | QSOs | Points

            a = row.find("a")
            if a:
                callsign = a.text
                list_of_callsigns.append(callsign)

                # This behavior also skips the header row, so we're going to do the other parsing here.
                cells = row.find_all("td")

                rankings_list_obj = {
                    "callsign": callsign,
                    "world_rank": int(cells[0].text),
                    "qso_count": int(cells[4].text),
                    "point_count": int(cells[5].text),
                    "usa_rank": None,
                    "state_rank": None,
                    "state": None,
                }

                m = usa_match_re.match(cells[2].text)
                if m:
                    rankings_list_obj["usa_rank"] = m.group(1)
                    m2 = state_match_re.match(cells[3].text)
                    # print(cells[3].text)
                    # print(m2)
                    if (
                        m2
                    ):  # There are some listings which inexplicably have *stuff* in the columns despite not being US (and being malformed generally) so we have to check
                        rankings_list_obj["state_rank"] = m2.group(1)
                        rankings_list_obj["state"] = m2.group(2)

                rankings_list.append(rankings_list_obj)

            else:
                next

        if not err_happened:
            # print(rankings_list)
            store_callsigns(list_of_callsigns)
            store_rankings(rankings_list)
            print(f"\t Successfully grabbed and stored page {COUNTER}.")
            time.sleep(5)
            COUNTER = COUNTER + 1
