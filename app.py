import streamlit as st

from datetime import datetime, timezone, timedelta
import pydeck as pdk
from callOpenSkyAPI import call_tracks_api
from transform import transform_trajectory
from load import load_recent_snapshot
import time

st.set_page_config(
    page_title='FlightTracker',
    layout='wide'
)

st.header('Flight Tracker')

with st.sidebar:
    st.header('Filter the data')

    n_files = st.slider(
        "Get the last x snapshot of airplanes' positions:",
        min_value=1,
        max_value=20,
        value=1,
        step=1
    )

df = load_recent_snapshot(n_files, sub_dir='serving')
df_pd = df.to_pandas()

with st.sidebar:

    min_altitude = st.slider(
        'Flights over x geo-altitude (m):',
        min_value=0,
        max_value=15000,
        value=0
    )

    country = sorted(df_pd['origin_country'].dropna().unique())
    select_country = st.multiselect(
        'Filter by origin country',
        country
    )

    callsign_filter = st.sidebar.text_input('Search a callsign that contains:')

    st.divider()
    icao24 = st.sidebar.text_input(
        'Check the live track of a flight by ICAO24:')


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

st.subheader(f"Data for {df_filtered['icao24'].nunique()} airplanes")
st.dataframe(df_filtered)

df_last = (
    df_filtered
    .sort_values(['icao24', 'time_position_dt_utc'])
    .groupby('icao24')
    .tail(1)
)

placeholder = st.empty()

scatter_history = pdk.Layer(
    'ScatterplotLayer',
    data=df_filtered,
    get_position='[longitude, latitude]',
    get_radius=500,
    radius_min_pixels=1,
    radius_max_pixels=10,
    get_fill_color=[101, 175, 255],
    pickable=True,
)

scatter_last = pdk.Layer(
    'ScatterplotLayer',
    data=df_last,
    get_position='[longitude, latitude]',
    get_radius=700,
    radius_min_pixels=1,
    radius_max_pixels=10,
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

st.divider()

if icao24:
    track_data_json = call_tracks_api(icao24)

    if track_data_json is None:
        placeholder.warning('No track available at the moment')
    else:
        df_track_point, pydeck_data = transform_trajectory(track_data_json)

        st.subheader(f'Live track of aircraft with ICAO24: {icao24}')

        path_layer = pdk.Layer(
            'PathLayer',
            data=pydeck_data,
            get_path='path',
            get_width=4,
            get_color=[101, 175, 255],
            width_min_pixels=2,
            pickable=True,
        )

        last_point = (
            df_track_point
            .select(["longitude", "latitude", "callsign", "icao24"])
            .tail(1)
            .to_dicts()
        )

        scatter_last = pdk.Layer(
            'ScatterplotLayer',
            data=last_point,
            get_position='[longitude, latitude]',
            get_radius=1000,
            radius_min_pixels=2,
            radius_max_pixels=20,
            get_fill_color=[251, 177, 60],
            pickable=True,
        )

        view_state = pdk.ViewState(
            latitude=df_track_point['latitude'].mean(),
            longitude=df_track_point['longitude'].mean(),
            zoom=5,
            pitch=0,
        )

        st.pydeck_chart(pdk.Deck(
            layers=[path_layer, scatter_last],
            initial_view_state=view_state
        ))

else:
    st.warning(
        'Please enter a ICAO24 code in the sidebar to the side to view the live track.')
