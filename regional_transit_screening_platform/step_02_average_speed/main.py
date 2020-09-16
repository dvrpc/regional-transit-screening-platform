from tqdm import tqdm

from regional_transit_screening_platform import db, match_features_with_osm


def match_speed_features_with_osm(
        speed_table: str = "linkspeed_byline",
        speed_mode_lookup_table: str = "linkspeedbylinenamecode"):
    """
        Identify OSM features that match each speed segment for surface transit
    """

    # Isolate features we want to analyze: non-zero/null surface transit
    # ------------------------------------------------------------------
    query = f"""
        select
            t.tsyscode,
            g.*
        from {speed_table} g
        left join
            {speed_mode_lookup_table} t
            on
                g.linename = t.linename
        where
            tsyscode in ('Bus', 'Trl')
        and
            avgspeed is not null
        and
            avgspeed > 0
    """
    db.make_geotable_from_query(
        query,
        speed_table + "_surface",
        geom_type="LINESTRING",
        epsg=26918
    )

    match_features_with_osm(speed_table + "_surface")


def analyze_speed(
        speed_table: str = "linkspeed_byline_surface"):

    # Make a table of all OSM features that matched a speed feature
    query = """
        select *
        from  osm_edges
        where osmuuid in (
            select distinct osmuuid::uuid
            from osm_matched_linkspeed_byline_surface
        )
    """
    db.make_geotable_from_query(query, "osm_speed", "LINESTRING", 26918)

    # Make a new speed column that forces values over 75 down to 75
    query_over75 = f"""
        alter table {speed_table} drop column if exists speed;

        alter table {speed_table} add column speed float;

        update {speed_table} set speed = (
            case when avgspeed < 75 then avgspeed else 75 end
        );
    """
    db.execute(query_over75)

    # Add columns to the OSM edge layer called 'avgspeed' and 'num_obs'
    make_speed_col = """
        alter table osm_speed drop column if exists avgspeed;
        alter table osm_speed add column avgspeed float;

        alter table osm_speed drop column if exists num_obs;
        alter table osm_speed add column num_obs float;
    """
    db.execute(make_speed_col)

    # Analyze each speed feature
    query = "select distinct osmuuid from osm_matched_linkspeed_byline_surface"
    osmid_list = db.query_as_list(query)
    for osmuuid in tqdm(osmid_list, total=len(osmid_list)):
        osmuuid = osmuuid[0]

        speed_query = f"""
            select
                sum(cnt * speed) / sum(cnt) as avgspeed,
                count(speed) as num_obs
            from {speed_table}
            where uid in (select distinct data_uid
                          from osm_matched_linkspeed_byline_surface m
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
            osmuuid,
            data_uid,
            st_makeline(
                (select st_centroid(geom)
                    from {speed_table}
                    where uid = data_uid
                ),
                (select st_centroid(geom)
                    from osm_speed
                    where osmuuid = osmuuid
                )
            ) as geom
        from osm_speed_matchup
    """
    db.make_geotable_from_query(qaqc, "osm_speed_qaqc", "LINESTRING", 26918)

    # Add a length column to the QAQC table
    length_col = """
        alter table osm_speed_qaqc add column feat_len float;
        update table osm_speed_qaqc set feat_len = st_length(geom);
    """
    db.execute(length_col)


if __name__ == "__main__":
    match_speed_features_with_osm()
    analyze_speed()
