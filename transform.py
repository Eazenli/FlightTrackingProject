import polars as pl
from datetime import datetime, timezone

"""
JSON -> Polars DataFrame with time to collect the data 
"""


def transform_to_pl_df(data: dict) -> pl.DataFrame:
    states = data['states']
    schema = {
        'icao24': pl.String,
        'callsign': pl.String,
        'origin_country': pl.String,
        'time_position': pl.Int64,
        'last_contact': pl.Int64,
        'longitude': pl.Float64,
        'latitude': pl.Float64,
        'baro_altitude': pl.Float64,
        'on_ground': pl.Boolean,
        'velocity': pl.Float64,
        'true_track': pl.Float64,
        'vertical_rate': pl.Float64,
        'sensors': pl.Int64,
        'geo_altitude': pl.Float64,
        'squawk': pl.String,
        'spi': pl.Boolean,
        'position_source': pl.Int64
    }
    df_states = pl.DataFrame(states, schema=schema, orient='row')
    df_selected = df_states.select([
        'icao24',
        'callsign',
        'origin_country',
        'time_position',
        'longitude',
        'latitude',
        'on_ground',
        'velocity',
        'true_track',
        'baro_altitude',
        'geo_altitude'
    ])

    df_filtered = df_selected.filter(
        pl.col('latitude').is_not_null() &
        pl.col('longitude').is_not_null()
    ).with_columns(
        pl.lit(datetime.now(timezone.utc)).alias('retrieved_at')
    )

    return df_filtered


def transform_trajectory(data: dict):
    flight_path = data['path']
    schema = [
        ('time', pl.Int64),
        ('latitude', pl.Float64),
        ('longitude', pl.Float64),
        ('altitude', pl.Float64),
        ('track', pl.Float64),
        ('on_ground', pl.Boolean),
    ]
    df_track_point = pl.DataFrame(
        data=flight_path,
        schema=schema,
        orient='row')

    df_track_point = df_track_point.with_columns(
        pl.from_epoch('time', time_unit='s').alias('time'),
        pl.lit(data['icao24']).alias('icao24'),
        pl.lit(data['callsign']).alias('callsign')
    )
    df_track_point = df_track_point.filter(
        pl.col('latitude').is_not_null() &
        pl.col('longitude').is_not_null()).sort('time')

    list_path = (
        df_track_point
        .select(["longitude", "latitude"])
        .to_pandas()
        .values
        .tolist()
    )

    pydeck_data = [
        {
            'callsign': df_track_point['callsign'][0],
            'path': list_path
        }
    ]

    return df_track_point, pydeck_data
