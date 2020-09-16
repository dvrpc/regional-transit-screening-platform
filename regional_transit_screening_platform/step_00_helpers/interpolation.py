import pandas as pd
from tqdm import tqdm

from regional_transit_screening_platform import db


def match_features_with_osm(
        data_table: str,
        osm_table: str = "osm_edges"):
    """
        Identify OSM features that match each segment
        within the `data_table`
    """

    print("-" * 80, f"\nMATCHING {data_table} TO OSM SEGMENTS")

    # Iterate over features and identify matching OSM features
    # --------------------------------------------------------

    result_df = pd.DataFrame(columns=["osmuuid", "data_uid"])

    uid_list = db.query_as_list(f"SELECT uid FROM {data_table}")

    for uid in tqdm(uid_list, total=len(uid_list)):
        uid = uid[0]

        # Inner query that gives the geometry(/buffer) of one feature
        inner_query = f"""
            SELECT geom
            FROM {data_table}
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
                       "data_uid": uid}
            result_df = result_df.append(new_row, ignore_index=True)

    # ----------------------------------
    # After iterating over all features,
    # write the result to the DB

    db.import_dataframe(
        result_df,
        f"osm_matched_{data_table}",
        if_exists="replace"
    )
