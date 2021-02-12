"""
Business logic: transpose speed data to the OSM network
"""

from tqdm import tqdm

from regional_transit_screening_platform import db, match_features_with_osm


def match_speed_features_with_osm(
    speed_table: str = "speed.rtsp_input_speed",
):
    """
    Identify OSM features that match each speed segment for surface transit
    """

    match_features_with_osm(speed_table)


def analyze_speed(
    speed_table: str = "speed.rtsp_input_speed",
    match_table: str = "osm_matched_speed_rtsp_input_speed",
):

    # Make a table of all OSM features that matched a speed feature
    new_tbl = "osm_speed"

    query = f"""
        select *
        from  osm_edges
        where osmuuid in (
            select distinct osmuuid::uuid
            from {match_table}
        )
    """
    db.make_geotable_from_query(query, new_tbl, "LINESTRING", 26918)

    # Add columns to the OSM edge layer called 'avgspeed' and 'num_obs'
    make_speed_col = f"""
        alter table {new_tbl} drop column if exists avgspeed;
        alter table {new_tbl} add column avgspeed float;

        alter table {new_tbl} drop column if exists num_obs;
        alter table {new_tbl} add column num_obs float;
    """
    db.execute_via_psycopg2(make_speed_col)

    # Analyze each speed feature
    query = f"select distinct osmuuid from {match_table}"
    osmid_list = db.query_as_list(query)
    for osmuuid in tqdm(osmid_list, total=len(osmid_list)):
        osmuuid = osmuuid[0]

        speed_query = f"""
            select
                sum(cnt * speed) / sum(cnt) as avgspeed,
                count(speed) as num_obs
            from {speed_table}
            where uid in (select distinct data_uid
                          from {match_table} m
                          where m.osmuuid = '{osmuuid}')
        """
        result = db.query_via_psycopg2(speed_query)
        avgspeed, num_obs = result[0]

        update_query = f"""
            UPDATE {new_tbl}
            SET avgspeed = {avgspeed},
                num_obs = {num_obs}
            WHERE osmuuid = '{osmuuid}';
        """
        db.execute_via_psycopg2(update_query)

    # Draw a line from the centroid of the speed feature to the OSM centroid
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
            {new_tbl} s
            on s.osmuuid = m.osmuuid::uuid
        left join
            {speed_table} f
            on f.uid = m.data_uid
    """
    db.make_geotable_from_query(qaqc, "osm_speed_qaqc", "LINESTRING", 26918)

    # Add a length column to the QAQC table
    length_col = f"""
        alter table {new_tbl}_qaqc add column feat_len float;
        update {new_tbl}_qaqc set feat_len = st_length(geom);
    """
    db.execute_via_psycopg2(length_col)


if __name__ == "__main__":
    match_speed_features_with_osm()
    analyze_speed()
