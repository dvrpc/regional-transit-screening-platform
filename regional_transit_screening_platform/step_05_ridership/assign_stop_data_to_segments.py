import pandas as pd
from regional_transit_screening_platform import db


def step_01_combine_ridership():
    """
    Merge the raw trolley & bus datasets together
    """
    query = """
    WITH
    bus_data AS (
        SELECT
            stop_id,
            stop_name,
            route,
            direction,
            sequence,
            sign_up,
            mode,
            source,
            weekday_bo,
            weekday_le,
            weekday_bo - weekday_le AS change,
            geom
        FROM
            raw.bus_ridership_spring2019
        WHERE
            mode != 'Trolley'
    ),

    bus_running_sum AS (
        SELECT
            *,
            SUM(weekday_bo) OVER (PARTITION BY route, direction ORDER BY sequence) weekday_tbo,
            SUM(weekday_le) OVER (PARTITION BY route, direction ORDER BY sequence) weekday_tle
        FROM bus_data
        ORDER BY sequence
    ),

    trolley_data AS (
        SELECT
            stop_id,
            stop_name,
            route,
            direction,
            sequence,
            sign_up,
            mode,
            source,
            weekday_bo,
            weekday_le,
            weekday_bo - weekday_le AS change,
            geom
        FROM raw.trolley_ridership_spring2018
        ),

    trolley_running_sum AS (
        SELECT
            *,
            SUM(weekday_bo) OVER (PARTITION BY route, direction ORDER BY sequence) weekday_tbo,
            SUM(weekday_le) OVER (PARTITION BY route, direction ORDER BY sequence) weekday_tle
        FROM trolley_data
        ORDER BY sequence
    ),

    combined_table AS(
        SELECT
            *,
            weekday_tbo - weekday_tle AS weekday_lo
        FROM bus_running_sum

        UNION ALL

        SELECT
            *,
            weekday_tbo - weekday_tle AS weekday_lo
        FROM trolley_running_sum
    )

    SELECT *
    FROM combined_table
    ORDER BY route, direction, sequence
    """

    kwargs = {"geom_type": "Point", "epsg": 4326}

    db.make_geotable_from_query(query, "ridership.surface_transit_loads", **kwargs)


def step_02_assign_loads_to_links():
    """
    Assign loads to model links. Ported over from mega SQL script.

    Notes:
    -----
        - It's unclear if the code within 'query_prep_stoppoints'
          needs to be run every time, or if this was a one-time thing.
          For now, it's omitted under the assumption that this was
          already executed on the database hosted on Daisy.
    """

    query_lineroutes = """
        -- need rank column for line routes to use a number to identify the fromto links in order for each line route
        -- need to create an unnested intermediate table, then can add a new SERIAL identifier which will be in the correct order (call it order)

        CREATE TABLE
        ridership.lineroutes_unnest AS(
            WITH temp_table AS(
                SELECT
                    lrid, tsys, linename, lrname, direction, stopsserved, numvehjour,
                    UNNEST(fromnodeseq) AS fromn,
                    UNNEST(tonodeseq) AS ton
                FROM raw.lineroutes
            )

            SELECT
                lrid, tsys, linename, lrname, direction, stopsserved, numvehjour,
                CONCAT (fromn, ton) AS fromto
            FROM temp_table
        );
        COMMIT;

        ALTER TABLE ridership.lineroutes_unnest
        ADD COLUMN total_order SERIAL;
        COMMIT;

        CREATE TABLE
        ridership.lineroutes_linkseq AS(
            SELECT
                lrid,
                tsys,
                linename,
                lrname,
                direction,
                stoppsserved,
                numvehjour,
                fromto,
                RANK() OVER(
                    PARTITION BY lrid
                    ORDER BY total_order
                ) AS lrseq
            FROM ridership.lineroutes_unnest
        );
        COMMIT;
    """

    query_gtfs = """
        --also need to split out LR GTFSid seq and create rank column too

        CREATE TABLE
        ridership.lineroutes_unnest_gtfs AS(
            SELECT
                lrid, tsys, linename, lrname, direction, stopsserved, numvehjour,
                UNNEST(gtfsidseq) AS gtfs
            FROM lineroutes
        );
        COMMIT;

        ALTER TABLE ridership.lineroutes_unnest_gtfs
        ADD COLUMN total_order SERIAL;
        COMMIT;

        CREATE TABLE
        ridership.lineroutes_gtfs AS(
            SELECT
                lrid,
                tsys,
                linename,
                lrname,
                direction,
                stopsserved,
                numvehjour,
                gtfs,
                RANK() OVER(
                    PARTITION BY lrid
                    ORDER BY total_order
                ) AS gtfsseq
            FROM ridership.lineroutes_unnest_gtfs
        );
        COMMIT;
    """

    query_apportion_percentages_to_route_lines = """

        -- divide ridership across line routes by number of vehicle journeys (evenly to start)

        CREATE TABLE
        ridership.lrid_portions_rider2019 AS(
            WITH temp_table AS(
                SELECT 
                    linename,
                    direction,
                    SUM(numvehjour)::NUMERIC as sum_vehjour
                FROM raw.lineroutes
                GROUP BY linename, direction
            ),

            all_lineroutes AS(
                SELECT 
                    lrid,
                    linename,
                    direction,
                    numvehjour::NUMERIC
                FROM raw.lineroutes
            )

            SELECT
                all_lineroutes.lrid,
                all_lineroutes.linename,
                all_lineroutes.direction,
                ROUND(
                    (all_lineroutes.numvehjour / temp_table.sum_vehjour), 2
                ) AS portion
            FROM
                all_lineroutes

            INNER JOIN temp_table
                    ON temp_table.linename = tblB.linename
                    AND temp_table.direction = tblB.direction

            WHERE
                temp_table.sum_vehjour <> 0

            ORDER BY
                linename, lrid
            );
        COMMIT;

    """

    query_prep_stoppoints = """

        --update concatenated text tonode fields to allow for future joining
        --in each case, one of the values was the same as the from node, so the tonode value was replaced with the remaining value
        --first update for where the fromnode matches the 2nd value in the concatenated tonode
        WITH tblA AS(
            SELECT
                spid,
                gtfsid,
                spname,
                fromonode,
                tonode,
                SPLIT_PART(tonode, ',', 1) as tn1,
                SPLIT_PART(tonode, ',', 2) as tn2
            FROM stoppoints
            WHERE tonode LIKE '%,%'
            ORDER by fromonode DESC
            ),
        tblB AS(
            SELECT *
            FROM tblA
            WHERE fromonode = CAST(tn1 AS numeric)
            OR fromonode = CAST(tn2 AS numeric)
            )
        UPDATE stoppoints
        SET tonode = tn1
        FROM tblA
        WHERE stoppoints.fromonode = CAST(tblA.tn2 AS numeric)

        --then update for where the fromnode matches the 1st value in the concatenated tonode
        WITH tblA AS(
            SELECT
                spid,
                gtfsid,
                spname,
                fromonode,
                tonode,
                SPLIT_PART(tonode, ',', 1) as tn1,
                SPLIT_PART(tonode, ',', 2) as tn2
            FROM stoppoints
            WHERE tonode LIKE '%,%'
            ORDER by fromonode DESC
            ),
        tblB AS(
            SELECT *
            FROM tblA
            WHERE fromonode = CAST(tn1 AS numeric)
            OR fromonode = CAST(tn2 AS numeric)
            )
        UPDATE stoppoints
        SET tonode = tn2
        FROM tblA
        WHERE stoppoints.fromonode = CAST(tblA.tn1 AS numeric)

    """

    query_assign_link_loads = """
        --get stoppoints ready to join to line route links with fromto field
        --first manually updated 7 recrods; tonode field had 2 values. In each case, one was a repeat of the fromnode, so it was removed.
        --then line up stop points with links they are on and the portion of the passenger load they should receive
        CREATE TABLE
        ridership.linkseq_withloads_bus_rider2019 AS(
            WITH tblA AS(
                SELECT spid, gtfsid, linkno, CONCAT(fromonode, CAST(tonode AS numeric)) AS fromto
                FROM raw.stoppoints
                WHERE gtfsid <> 0
            ),
            tblB AS(
                SELECT 
                    l.*,
                    p.portion
                FROM ridership.lineroutes_linkseq l
                INNER JOIN ridership.lrid_portions_rider2019 p
                ON l.lrid = p.lrid
            ),
            tblC AS(
                SELECT
                    l.lrid,
                    l.tsys,
                    l.linename,
                    l.lrname,
                    l.direction,
                    l.stopsserved,
                    l.numvehjour,
                    l.fromto,
                    l.lrseq,
                    l.portion,
                    a.spid, 
                    a.gtfsid,
                    a.linkno
                FROM tblB l
                LEFT JOIN tblA a
                ON a.fromto = l.fromto
                --for buses only (will repeat later for trolleys)
                WHERE l.tsys = 'Bus'
                ORDER BY lrid, lrseq
            ),
            tblD AS(
                SELECT *
                FROM ridership.surface_transit_loads
                WHERE weekday_lo > 0
                )
            SELECT
                c.*,
                d.weekday_lo,
                (d.weekday_lo * c.portion) AS load_portion
            FROM tblC c
            LEFT JOIN tblD d
            ON c.gtfsid = d.stop_id
            AND c.linename = d.route
            WHERE c.lrname LIKE 'sepb%'
            ORDER BY lrid, lrseq
            );
        COMMIT;

        --repeating above for Trolleys
        CREATE TABLE 
        ridership.linkseq_withloads_trl_rider2019 AS(
            WITH tblA AS(
                SELECT spid, gtfsid, linkno, CONCAT(fromonode, CAST(tonode AS numeric)) AS fromto
                FROM raw.stoppoints
                ),
            tblB AS(
                SELECT 
                    l.*,
                    p.portion
                FROM ridership.lineroutes_linkseq l
                INNER JOIN ridership.lrid_portions_rider2019 p
                ON l.lrid = p.lrid
                ),
            tblC AS(
                SELECT
                    l.lrid,
                    l.tsys,
                    l.linename,
                    l.lrname,
                    l.direction,
                    l.stopsserved,
                    l.numvehjour,
                    l.fromto,
                    l.lrseq,
                    l.portion,
                    a.spid, 
                    a.gtfsid,
                    a.linkno
                FROM tblB l
                LEFT JOIN tblA a
                ON a.fromto = l.fromto
                --for trolleys only
                WHERE l.tsys = 'Trl' OR l.tsys = 'LRT'
                ORDER BY lrid, lrseq
                ),
            tblD AS(
                SELECT *
                FROM ridership.surface_transit_loads
                WHERE weekday_lo > 0
                )
            SELECT
                c.*,
                d.weekday_lo,
                (d.weekday_lo*c.portion) AS load_portion
            FROM tblC c
            LEFT JOIN tblD d
            ON c.spid = (d.stop_id + 100000)
            AND c.linename = d.route
            WHERE c.lrname LIKE 'sepb%'
            ORDER BY lrid, lrseq
            );
        COMMIT;

        CREATE TABLE 
        ridership.linkseq_withloads_rider2019 AS(
            SELECT *
            FROM ridership.linkseq_withloads_bus_rider2019
            UNION ALL
            SELECT *
            FROM ridership.linkseq_withloads_trl_rider2019
            );
        COMMIT;
    """

    query_distribute_loads = """

        --Assumption: ridership distributed across line routes by number of vehicle journeys
        --Assumption: if more than one stop is on a link (sometimes up to 6), the load is averaged - it is usually very similar

        --clean up repeats from links that have multiple stops (average loads)
        --requires losing detail on gtfsid, but can always get it from the previous table

        CREATE TABLE
        ridership.linkseq_cleanloads_rider2019 AS(
            WITH tblA AS(
                SELECT lrid, tsys, linename, direction, stopsserved, numvehjour, fromto, lrseq, COUNT(DISTINCT(gtfsid)), sum(load_portion)
                FROM ridership.linkseq_withloads_rider2019
                GROUP BY lrid, tsys, linename, direction, stopsserved, numvehjour, fromto, lrseq
            )
            SELECT 
                lrid,
                tsys, 
                linename,
                direction,
                stopsserved,
                numvehjour,
                fromto,
                lrseq,
                count,
                sum / count AS load_portion_avg
            FROM tblA
            ORDER BY lrid, lrseq
            );
        COMMIT;
    """

    queries = [
        query_lineroutes,
        query_gtfs,
        query_apportion_percentages_to_route_lines,
        # query_prep_stoppoints,
        query_assign_link_loads,
        query_distribute_loads,
    ]

    for idx, q in enumerate(queries):
        print("-" * 80)
        print(f"Query # {idx + 1} \n\n")
        print(q)
        db.execute(q)

    ######### incorporate fill_in_linkloads.py

    query_join_loads_to_geom = """

       ---AFTER PYTHON
        --summarize and join to geometries to view
        --line level results
        CREATE TABLE loaded_links_linelevel_rider2019 AS(
            WITH tblA AS(
                SELECT 
                    no,
                    CONCAT(CAST(fromnodeno AS text), CAST(tonodeno AS text)) AS fromto,
                    r_no,
                    CONCAT(CAST("r_fromno~1" AS text), CAST(r_tonodeno AS text)) AS r_fromto,
                    geom
                FROM "2015base_link"
                ),
            tblB AS(
                SELECT
                    lrid,
                    tsys,
                    linename,
                    direction,
                    stopsserved,
                    numvehjour,
                    fromto,
                    COUNT(fromto) AS times_used,
                    SUM(CAST(load_portion_avg AS numeric)) AS total_load
                FROM loaded_links_rider2019
                WHERE tsys = 'Bus'
                OR tsys = 'Trl'
                OR tsys = 'LRT'
                GROUP BY lrid, tsys, linename, direction, stopsserved, numvehjour, fromto
                ),
            tblC AS(
                SELECT
                    b.*,
                    a.geom,
                    aa.geom AS geom2
                FROM tblB b
                LEFT JOIN tblA a
                ON b.fromto = a.fromto
                LEFT JOIN tblA aa
                ON b.fromto = aa.r_fromto
            )
            SELECT
                lrid,
                tsys,
                linename,
                direction,
                stopsserved,
                numvehjour,
                fromto,
                times_used,
                ROUND(total_load, 0),
                CASE WHEN geom IS NULL THEN geom2
                    ELSE geom
                    END
                    AS geometry
            FROM tblC);
        COMMIT;

        --aggregate further (and loose line level attributes) for segment level totals

        CREATE TABLE loaded_links_segmentlevel_rider2019 AS(
            WITH tblA AS(
                SELECT 
                    no,
                    CONCAT(CAST(fromnodeno AS text), CAST(tonodeno AS text)) AS fromto,
                    r_no,
                    CONCAT(CAST("r_fromno~1" AS text), CAST(r_tonodeno AS text)) AS r_fromto,
                    geom
                FROM "2015base_link"
                ),
            tblB AS(
                SELECT
                    fromto,
                    COUNT(fromto) AS times_used,
                    SUM(CAST(load_portion_avg AS numeric)) AS total_load
                FROM loaded_links_rider2019
                WHERE tsys = 'Bus'
                OR tsys = 'Trl'
                OR tsys = 'LRT'
                GROUP BY fromto
                ),
            tblC AS(
                SELECT
                    b.*,
                    a.geom,
                    aa.geom AS geom2
                FROM tblB b
                LEFT JOIN tblA a
                ON b.fromto = a.fromto
                LEFT JOIN tblA aa
                ON b.fromto = aa.r_fromto
            )
            SELECT
                fromto,
                times_used,
                ROUND(total_load,0),
                CASE WHEN geom IS NULL THEN geom2
                    ELSE geom
                    END
                    AS geometry
            FROM tblC);
        COMMIT;

        ---segment level totals with split from/to to allow for summing directionsal segment level loads
        --added 01/06/20 to help Al with Frankford Ave project mapping
        --updated 07/07/2020
        CREATE TABLE loaded_links_segmentlevel_test_rider2019 AS(
            WITH tblA AS(
                SELECT 
                    no,
                    CAST(fromnodeno AS text),
                    CAST(tonodeno AS text),
                    CONCAT(CAST(fromnodeno AS text), CAST(tonodeno AS text)) AS fromto,
                    r_no,
                    CONCAT(CAST("r_fromno~1" AS text), CAST(r_tonodeno AS text)) AS r_fromto,
                    CAST("r_fromno~1" AS text) AS r_from,
                    CAST(r_tonodeno AS text) AS r_to,
                    geom
                FROM "2015base_link"
                ),
            tblB AS(
                SELECT
                    fromto,
                    COUNT(fromto) AS times_used,
                    SUM(CAST(load_portion_avg AS numeric)) AS total_load
                FROM loaded_links_rider2019
                WHERE tsys = 'Bus'
                OR tsys = 'Trl'
                OR tsys = 'LRT'
                GROUP BY fromto
                ),
            tblC AS(
                SELECT
                    b.*,
                    a.no,
                    a.fromnodeno,
                    a.tonodeno,
                    --a.r_no,
                    --a.r_from,
                    --a.r_to,
                    a.geom,
                    aa.r_no,
                    aa.r_from,
                    aa.r_to,
                    aa.geom AS geom2
                FROM tblB b
                LEFT JOIN tblA a
                ON b.fromto = a.fromto
                LEFT JOIN tblA aa
                ON b.fromto = aa.r_fromto
            )
            SELECT
                fromto,
                CASE WHEN no IS NULL THEN r_no
                ELSE no
                END
                AS linkno,
                CASE WHEN fromnodeno IS NULL THEN r_from
                ELSE fromnodeno
                END
                AS fromnodeno,
                CASE WHEN tonodeno IS NULL THEN r_to
                ELSE tonodeno
                END
                AS tonodeno,	    
                times_used,
                ROUND(total_load,0),
                CASE WHEN geom IS NULL THEN geom2
                    ELSE geom
                    END
                    AS geometry
            FROM tblC);
        COMMIT;


    """


def inner_step_2_fill_in_linkloads():

    loads = db.query_as_list(
        """
        SELECT *
        FROM linkseq_cleanloads_rider2019
        ORDER BY lrid, lrseq
        """
    )

    # convert tupples to lists that can be changed
    loads_list = []
    for row in loads:
        list_row = []
        for item in row:
            list_row.append(item)
        loads_list.append(list_row)

    # if load for first link in sequence is none, change to 0
    counter = 0
    for i in range(0, len(loads_list)):
        if loads_list[i][7] == 1:
            if loads_list[i][9] is None:
                loads_list[i][9] = 0

    # testing
    # test that it got all of them (should equal 0)
    counter = 0
    for i in range(0, len(loads_list)):
        if loads_list[i][7] == 1:
            if loads_list[i][9] is None:
                counter += 1
    if counter > 0:
        print("QA alert: Counter should have been 0!")

    # testing
    # make sure there are not other weird first link values
    firsts = []
    for i in range(0, len(loads_list)):
        if int(loads_list[i][7]) == 1:
            firsts.append(loads_list[i][9])

    # drop values down in list
    holder = 0
    for i in range(0, len(loads_list)):
        if int(loads_list[i][7]) == 1:
            holder = loads_list[i][9]
        else:
            if loads_list[i][9] is None:
                loads_list[i][9] = holder
            else:
                holder = loads_list[i][9]

    df = pd.DataFrame(loads_list)

    df.columns = [
        "lrid",
        "tsys",
        "linename",
        "direction",
        "stopsserved",
        "numvehjour",
        "fromto",
        "lrseq",
        "count",
        "load_portion_avg",
    ]

    db.import_dataframe(df, "loaded_links_rider2019", if_exists="replace", schema="ridership")


if __name__ == "__main__":
    # step_01_combine_ridership()
    step_02_assign_loads_to_links()
