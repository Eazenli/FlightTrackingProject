import polars as pl
from datetime import datetime, timezone
"""
JSON -> Polars DataFrame with time to collect the data 
"""


def normalize_state_row(row: list) -> list:
    """
    'category' can be absent in the real raw data, in this case add None 
    """
    if len(row) == 17:
        return row + [None]
    if len(row) == 18:
        return row
    raise ValueError(
        f"Ligne inattendue : longueur {len(row)} au lieu de 17 ou 18")


def create_raw_df(data: dict) -> pl.DataFrame:
    """
    create a raw data
    """
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
        'sensors': pl.List(pl.Int64),
        'geo_altitude': pl.Float64,
        'squawk': pl.String,
        'spi': pl.Boolean,
        'position_source': pl.Int64,
        'category': pl.Int64
    }
    normalize_state = [normalize_state_row(row) for row in states]
    df_states = pl.DataFrame(normalize_state, schema=schema, orient='row')
    df_states = df_states.with_columns(
        pl.lit(datetime.now(timezone.utc)).alias('retrieved_at')
    )
    return df_states


def transform_raw_df(df: pl.DataFrame) -> pl.DataFrame:
    df_cleaned = df.select([
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
        'geo_altitude',
        'retrieved_at'
    ]).filter(
        pl.col('latitude').is_not_null() &
        pl.col('longitude').is_not_null() &
        (~pl.col('on_ground')) &
        (pl.col('velocity') >= 30)
    ).with_columns(
        pl.from_epoch('time_position', time_unit='s')
        .dt.replace_time_zone('UTC')
        .alias('time_position_dt_utc'),
        (pl.col('velocity')*3.6).round(2).alias('velocity_kmh')
    ).rename(
        {'velocity': 'velocity_ms'}
    ).drop(
        ['time_position', 'on_ground', 'retrieved_at']
    ).select([
        'icao24',
        'callsign',
        'origin_country',
        'longitude',
        'latitude',
        'velocity_ms',
        'velocity_kmh',
        'true_track',
        'baro_altitude',
        'geo_altitude',
        'time_position_dt_utc',
    ]).sort(['icao24', 'time_position_dt_utc'])
    return df_cleaned


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
