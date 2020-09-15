from tqdm import tqdm
import pandas as pd

from regional_transit_screening_platform import db


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

    # Iterate over surface transit features and identify matching OSM features
    # ------------------------------------------------------------------------

    result_df = pd.DataFrame(columns=["osmuuid", "speed_uid"])

    uid_list = db.query_as_list(f"SELECT uid FROM {speed_table}_surface")

    for uid in tqdm(uid_list, total=len(uid_list)):
        uid = uid[0]

        # Inner query that gives the geometry(/buffer) of one speed feature
        inner_query = f"""
            SELECT geom
            FROM {speed_table}_surface
            WHERE uid = {uid}
        """
        inner_buffer = inner_query.replace("geom", "st_buffer(geom, 20)")

        query_matching_osm_features = f"""
            select
                osmuuid,
                st_length(geom) as original_geom,
                st_length(
                    st_intersection(geom, ({inner_buffer}))
                ) as intersected_geom,
                degrees(st_angle(geom, ({inner_query}))) as angle_diff
            from
                osm_edges
            where
                st_intersects(geom, ({inner_buffer}))
        """

        df = db.query_as_df(query_matching_osm_features)

        # Flag features that match the geometry test:
        #   1) The intersection is at least 25 meters, OR
        #   2) The feature is 80% or more within the buffer
        df["geom_match"] = "No"
        df["pct_in_buffer"] = df["intersected_geom"] / df["original_geom"]
        df.loc[(df.intersected_geom >= 25) |
               (df.pct_in_buffer >= 0.8), "geom_match"] = "Yes"

        # Flag any features that have a reasonably similar angle:
        #   - between 0 and 20 degrees, OR
        #   - between 160 and 200 degrees, OR
        #   - more than 340 degrees
        df["angle_match"] = "No"
        df.loc[((df.angle_diff > 0) & (df.angle_diff < 20)) |
               ((df.angle_diff > 160) & (df.angle_diff < 200)) |
               ((df.angle_diff > 340)), "angle_match"] = "Yes"

        # Filter the df to those that match the geometry and angle criteria
        matching_df = df[(df.angle_match == "Yes") & (df.geom_match == "Yes")]

        # Insert a result row for each unique combo of osm & speed uids
        for _, osm_row in matching_df.iterrows():
            new_row = {"osmuuid": osm_row.osmuuid,
                       "speed_uid": uid}
            result_df = result_df.append(new_row, ignore_index=True)

    # Write the result to the DB
    db.import_dataframe(result_df, "osm_speed_matchup", if_exists="replace")


def analyze_speed(
        speed_table: str = "linkspeed_byline_surface"):

    # Make a table of all OSM features that matched a speed feature
    query = """
        select *
        from  osm_edges
        where osmuuid in (
            select distinct osmuuid from osm_speed_matchup
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
    query = "select distinct osmuuid from osm_speed_matchup"
    osmid_list = db.query_as_list(query)
    for osmuuid in tqdm(osmid_list, total=len(osmid_list)):
        osmuuid = osmuuid[0]

        speed_query = f"""
            select
                sum(cnt * speed) / sum(cnt) as avgspeed,
                count(speed) as num_obs
            from {speed_table}
            where uid in (select distinct speed_uid
                          from osm_speed_matchup m
                          where m.osmuuid = '{osmuuid}')
        """
        result = db.query_as_list(speed_query)
        avgspeed, num_obs = result[0]

        update_query = f"""
            UPDATE osm_speed
            SET avgspeed = {avgspeed},
                num_obs = {num_obs}
            WHERE osmuuid = '{osmuuid}';
        """
        db.execute(update_query)

    # Draw a line from the centroid of the speed feature to the OSM centroid
    qaqc = f"""
        select
            osmuuid,
            speed_uid,
            st_makeline(
                (select st_centroid(geom)
                    from {speed_table}
                    where uid = speed_uid
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
