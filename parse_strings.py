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
from datetime import datetime

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


# 2E0OBO counts for 1 point because 2E0OBO has the role ARRL Member (MEM) through 2024-05-31 04:00:00.
# KE5SF counts for 30 points through 2024-01-01 00:00:00.

parse_role_re = re.compile(
    "^([A-Z0-9/]+) counts for ([0-9]+ points?) because [A-Z0-9/]+ has the role ([A-Za-z0-9,/ ]+) \(([A-Z0-9_]+)\) through ([0-9: -]+)\.$"
)

parse_snowflake_re = re.compile(
    "^([A-Z0-9/]+) counts for 30 points through ([0-9: -]+)\.$"
)

if __name__ == "__main__":
    with engine.connect() as conn:
        stmt = select(vota_points_table).where(
            vota_points_table.c.points != 0, vota_points_table.c.role == None
        )
        # print(stmt)
        row_updates = []
        for row in conn.execute(stmt):
            m = parse_role_re.match(row.result_string)
            if m:
                result = {
                    "b_callsign": m.group(1),
                    "points": int(m.group(2).split()[0]),
                    "role": m.group(3),
                    "role_abbrev": m.group(4),
                    "end_date": datetime.strptime(m.group(5), "%Y-%m-%d %H:%M:%S"),
                }
                row_updates.append(result)
            elif row.points == 30:
                m2 = parse_snowflake_re.match(row.result_string)
                if m2:
                    result = {
                        "b_callsign": m2.group(1),
                        "points": 30,
                        "role": "Special 30-Point Snowflake",
                        "role_abbrev": "SNWF",
                        "end_date": datetime.strptime(m2.group(2), "%Y-%m-%d %H:%M:%S"),
                    }
                    row_updates.append(result)
                else:
                    print(f"ERROR: Unable to parse string <<{row.result_string}>>.")
            else:
                print(f"ERROR: Unable to parse string <<{row.result_string}>>.")

    stmt = (
        update(vota_points_table)
        .where(vota_points_table.c.callsign == bindparam("b_callsign"))
        .values(
            points=bindparam("points"),
            role=bindparam("role"),
            role_abbrev=bindparam("role_abbrev"),
            end_date=bindparam("end_date"),
        )
    )

    with engine.begin() as conn:
        conn.execute(stmt, row_updates)
