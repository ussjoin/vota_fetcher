# VOTA Fetcher

## What is this?

A series of queries to fetch the relevant bits of the [ARRL 2023 Volunteers on the Air](https://vota.arrl.org/) databases; primarily, the callsign-to-ARRL-role mappings, but also the leaderboard.

Also, the VOTA points database this produces (not the leaderboard), in the [results/](results/) folder. This copy was collected during time that the following provenance data was displayed on the [Call Sign Points](https://vota.arrl.org/callPoints.php) page: "Point information was last updated: 2023/10/16 21:39:24 UTC"

## Who did this?

Brendan O'Connor, but on the amateur airwaves I'm known as K3QB. Within the ARRL, I'm a [Volunteer Counsel](https://www.arrl.org/vc-description) in the Northwest Division. I'm also the trustee of the [Narwhal Amateur Radio Society (NR7WL)](https://nars.narwhal.be).

## Why?

I wanted to be able to query points while looking at my logs for 2023. Having them stored in a database makes that as easy as a LEFT JOIN. More broadly, I think there are some interesting things to be learned from this data, so I wanted to make the data available.

## Is this code usable by others?

Essentially yes, though the SQLalchemy connection strings at the top of each file should be changed to point to your database, rather than mine. Only one line (an exception handler) in `fetch_list.py` is Postgres-dependent; the rest are quite agnostic.

That said, the code is ugly. It's functional! But please don't take this as an example of the highest art of programming. It's an example of the highest art of "this works sufficiently for my purposes."

## How would I run this?

1. `python3 fetch_list.py` (This grabs the leaderboard, which, among other things, populates your database with the list of all callsigns on the leaderboard.)
2. `python3 fetch_callsigns.py` (This runs a [Call Sign Points](https://vota.arrl.org/callPoints.php) query for each callsign in the database. This is designed to run on many nodes talking to the same database at once, and to use random sampling to minimize duplicated work.)
3. `python3 parse_strings.py` (This takes the results of fetch_calligns.py and runs the parsing of the result string to add role information to the database.)

## What do I get if I run this?

A database table, `vota_points`, with the following columns (in SQLAlchemy expression):

```python
vota_points_table = Table(
  "vota_points",
  metadata_obj,
  Column("callsign", String, primary_key=True), # Callsign
  Column("points", Integer), # Points this callsign was worth in VOTA 2023
  Column("role", String), # Role of this callsign in the ARRL
  Column("role_abbrev", String), # Abbreviation of this callsign's ARRL role
  Column("end_date", DateTime), # When the VOTA Points Database said their role was up for renewal
  Column("result_string", String), # The complete sentence the VOTA Points Database returned for this callsign
)
```

Note that there's one weird case you'll note in `parse_strings.py`: the "Special 30-Point Snowflake." These are people who instead of the "normal" result sentence from the database, which looks like

> K3QB counts for 15 points because K3QB has the role Volunteer Counsel (VC) through 2024-01-01 00:00:00.

Instead have something like

> N7UVH counts for 30 points through 2024-01-01 00:00:00.

...what gives? _I have no idea._ I looked through the QRZ pages for many of the **65** people that have this status in the DB and couldn't find any commonalities or mentions of why they had a special number of points. (In fact, some of them mention "lesser" roles (that is, roles worth fewer points in VOTA) that are "covered" by their higher point total, just as my (K3QB) Volunteer Examiner (5 points) role is "covered" by my Volunteer Counsel (15 points) role. VOTA did not allow people with multiple roles to score as the total of their roles, only as the highest-value single role.) I should just email them and ask them who they are. :-)

## LICENSE

BSD 2-clause, except for the contents of the results folder, which are not copyrightable. *See, e.g.*, [*Feist Publications, Inc. v. Rural Tel. Serv. Co.*, 499 U.S. 340 (1991)](https://supreme.justia.com/cases/federal/us/499/340/).

