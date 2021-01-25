from tqdm import tqdm

from regional_transit_screening_platform import db, match_features_with_osm


def match_speed_features_with_osm(
    speed_table: str = "rtsp_input_speed",
):
    """
    Identify OSM features that match each speed segment for surface transit
    """

    match_features_with_osm(speed_table)


def analyze_speed(
    speed_table: str = "rtsp_input_speed",
    match_table: str = "osm_matched_rtsp_input_speed",
):

    # Make a table of all OSM features that matched a speed feature
    query = f"""
        select *
        from  osm_edges
        where osmuuid in (
            select distinct osmuuid::uuid
            from {match_table}
        )
    """
    db.make_geotable_from_query(query, "osm_speed", "LINESTRING", 26918)

    # Add columns to the OSM edge layer called 'avgspeed' and 'num_obs'
    make_speed_col = """
        alter table osm_speed drop column if exists avgspeed;
        alter table osm_speed add column avgspeed float;

        alter table osm_speed drop column if exists num_obs;
        alter table osm_speed add column num_obs float;
    """
    db.execute(make_speed_col)

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
        # print(speed_query)
        result = db.query_as_list(speed_query)
        avgspeed, num_obs = result[0]

        update_query = f"""
            UPDATE osm_speed
            SET avgspeed = {avgspeed},
                num_obs = {num_obs}
            WHERE osmuuid = '{osmuuid}';
        """
        # print(update_query)
        db.execute(update_query)

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
            osm_speed s
            on s.osmuuid = m.osmuuid::uuid
        left join
            {speed_table} f
            on f.uid = m.data_uid
    """
    db.make_geotable_from_query(qaqc, "osm_speed_qaqc", "LINESTRING", 26918)

    # Add a length column to the QAQC table
    length_col = """
        alter table osm_speed_qaqc add column feat_len float;
        update osm_speed_qaqc set feat_len = st_length(geom);
    """
    db.execute(length_col)


if __name__ == "__main__":
    match_speed_features_with_osm()
    analyze_speed()
