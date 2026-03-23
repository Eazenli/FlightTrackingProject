import streamlit as st
from pathlib import Path
from datetime import datetime, timezone, timedelta
import polars as pl
import pydeck as pdk
from transform import clean_df_pl

st.set_page_config(
    page_title='FlightTracker',
    layout='wide'
)

st.header('Flight Tracker')


def load_recent_snapshot(n_files: int) -> pl.DataFrame:
    base_dir = Path('data')
    days = sorted(base_dir.glob('*'))
    if not days:
        return pl.DataFrame()

    latest_day = days[-1]

    files = sorted(latest_day.glob('*.parquet'))
    if not files:
        return pl.DataFrame()

    recent_files = files[-n_files:]

    return pl.concat([pl.read_parquet(f) for f in recent_files])


with st.sidebar:
    st.header('Filter the data')

    n_files = st.slider(
        'Get the last x snapshot of aircraft positions:',
        min_value=1,
        max_value=20,
        value=1,
        step=1
    )

df = load_recent_snapshot(n_files)

df_cleaned = clean_df_pl(df).filter(
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
    'geo_altitude',
    'time_position_dt_utc',
]).sort('icao24')

df_pd = df_cleaned.to_pandas()

with st.sidebar:
    callsign_filter = st.sidebar.text_input('Search a callsign that contains')

    country = sorted(df_pd['origin_country'].dropna().unique())
    select_country = st.multiselect(
        'Filter by origin country',
        country
    )

    min_altitude = st.slider(
        'Flights over x geo-altitude (m):',
        min_value=0,
        max_value=15000,
        value=0
    )

# Apply the filters

df_filtered = df_pd.copy()

if callsign_filter:
    callsign_filter = callsign_filter.upper()
    df_filtered = df_filtered[
        df_filtered['callsign'].str.contains(callsign_filter, na=False)]

if select_country:
    df_filtered = df_filtered[df_filtered['origin_country'].isin(
        select_country)]

df_filtered = df_filtered[
    df_filtered['geo_altitude']
    .fillna(-1) >= min_altitude]

st.dataframe(df_filtered)

df_last = (
    df_filtered
    .sort_values(['icao24', 'time_position_dt_utc'])
    .groupby('icao24')
    .tail(1)
)

scatter_history = pdk.Layer(
    'ScatterplotLayer',
    data=df_filtered,
    get_position='[longitude, latitude]',
    get_radius=500,
    radius_min_pixels=1,
    radis_max_pixels=2,
    get_fill_color=[101, 175, 255],
    pickable=True,
)

scatter_last = pdk.Layer(
    'ScatterplotLayer',
    data=df_last,
    get_position='[longitude, latitude]',
    get_radius=700,
    radius_min_pixels=1,
    radis_max_pixels=2,
    get_fill_color=[251, 177, 60],
    pickable=True,
)

view_state = pdk.ViewState(
    latitude=df_filtered['latitude'].mean(),
    longitude=df_filtered['longitude'].mean(),
    zoom=4,
)

st.pydeck_chart(
    pdk.Deck(
        layers=[scatter_history, scatter_last],
        initial_view_state=view_state,
    )
)
