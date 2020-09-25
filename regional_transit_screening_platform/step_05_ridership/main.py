from tqdm import tqdm

from regional_transit_screening_platform import db, match_features_with_osm


def match_septa_ridership_with_osm(
        ridership_table: str = "passloads_segmentlevel_2020_07"):

    # # Filter out ridership segments that don't have volumes
    # q = f"""
    #     SELECT * FROM {ridership_table}
    #     WHERE round IS NOT NULL and round > 0;
    # """
    # filtered_tbl = f"{ridership_table}_filtered"
    # db.make_geotable_from_query(q, filtered_tbl, "LINESTRING", 26918)

    # uid_query = f"""
    #     SELECT uid FROM {filtered_tbl}
    # """

    # uid_list = db.query_as_list(uid_query)

    # for uid in uid_list:
    #     print(uid)

    match_features_with_osm(filtered_tbl)


def match_njt_ridership_with_osm(
        ridership_table: str = "statsbyline_allgeom"):

    # Filter the table to only include NJT features
    filtered_table = f"{ridership_table}_filtered"

    filter_query = f"""
        SELECT * FROM {ridership_table}
        WHERE name LIKE 'njt%%' AND dailyrider > 0
    """
    db.make_geotable_from_query(
        filter_query,
        filtered_table,
        "LINESTRING",
        26918
    )

    match_features_with_osm(filtered_table, compare_angles=False)


def analyze_ridership(
    match_table: str = "osm_matched_passloads_segmentlevel_2020_07_filtered",
    data_table: str = "passloads_segmentlevel_2020_07_filtered"
):

    # Make a table of all OSM features that matched a ridership feature
    query = f"""
        select *
        from  osm_edges
        where osmuuid in (
            select distinct osmuuid::uuid
            from {match_table}
        )
    """
    db.make_geotable_from_query(query, "osm_ridership", "LINESTRING", 26918)

    # Add columns to the OSM edge layer called 'avgspeed' and 'num_obs'
    make_ridership_col = """
        alter table osm_ridership drop column if exists ridership;
        alter table osm_ridership add column ridership float;

        alter table osm_ridership drop column if exists num_obs;
        alter table osm_ridership add column num_obs float;
    """
    db.execute(make_ridership_col)

    # Analyze each ridership feature
    query = f"select distinct osmuuid from {match_table}"
    osmid_list = db.query_as_list(query)
    for osmuuid in tqdm(osmid_list, total=len(osmid_list)):
        osmuuid = osmuuid[0]

        ridership_query = f"""
            select
                sum(round) / count(uid) as ridership,
                count(uid) as num_obs
            from {data_table}
            where uid in (select distinct data_uid
                          from {match_table} m
                          where m.osmuuid = '{osmuuid}')
        """

        result = db.query_as_list(ridership_query)
        ridership, num_obs = result[0]

        update_query = f"""
            UPDATE osm_ridership
            SET ridership = {ridership},
                num_obs = {num_obs}
            WHERE osmuuid = '{osmuuid}';
        """
        db.execute(update_query)

    # Draw a line from the centroid of the ridership feature to OSM
    qaqc = f"""
        select
            m.osmuuid,
            m.data_uid,
            st_makeline(
                ST_LineInterpolatePoint(f.geom, 0.5),
                ST_LineInterpolatePoint(s.geom, 0.5)
            ) as geom
        from {match_table} m
        left join
            osm_ridership s
            on s.osmuuid = m.osmuuid::uuid
        left join
            {data_table} f
            on f.uid = m.data_uid
    """
    db.make_geotable_from_query(qaqc, "osm_ridership_qaqc", "LINESTRING", 26918)

    # Add a length column to the QAQC table
    length_col = """
        alter table osm_ridership_qaqc add column feat_len float;
        update osm_ridership_qaqc set feat_len = st_length(geom);
    """
    db.execute(length_col)


if __name__ == "__main__":
    # match_septa_ridership_with_osm()
    match_njt_ridership_with_osm()
    # analyze_ridership()
