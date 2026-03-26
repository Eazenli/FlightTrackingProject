import streamlit as st
import pydeck as pdk
import altair as alt
from storage.s3 import load_recent_snapshot
import time

st.set_page_config(
    page_title='✈️ FlightTracker',
    layout='wide'
)

st.header('✈️ Flight Tracker')
st.markdown(
    'All the data displayed in this app come from *OpenSky Network API*.\n\n'
    'The app refreshes every 30 seconds to retrieve the most recent information.'
)

with st.sidebar:
    st.header('Filter the data')

    n_files = st.slider(
        'Get the last N snapshot(s) of aircraft position: ',
        min_value=1,
        max_value=20,
        value=1,
        step=1,
        help='The yellow points on the map indicate the latest known positions of aircraft, while the blue ones represent previous positions.'
    )

df = load_recent_snapshot(n_files, sub_dir='serving')
df_pd = df.to_pandas()

with st.sidebar:

    min_altitude = st.slider(
        'Flights over N geo-altitude (m):',
        min_value=0,
        max_value=15000,
        value=0
    )

    country = sorted(df_pd['origin_country'].dropna().unique())
    select_country = st.multiselect(
        'Filter by origin country',
        country
    )

    callsign_filter = st.sidebar.text_input(
        'Search the aircraft whose callsign contains:')

    st.divider()

    icao24 = st.sidebar.text_input(
        'View the altitude and speed plots of an aircraft by ICAO24:')


# --- Apply the filters and display the DataFrame ---

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

st.markdown(f"#### Data for {df_filtered['icao24'].nunique()} airplanes")
st.dataframe(df_filtered)

# --- MAP ---

df_path = (
    df_filtered
    .sort_values(['icao24', 'time_position_dt_utc'])
    .groupby('icao24')
    .apply(lambda x: x[['longitude', 'latitude']].values.tolist())
    .reset_index(name='path')
)

df_last = (
    df_filtered
    .sort_values(['icao24', 'origin_country', 'time_position_dt_utc'])
    .groupby('icao24')
    .tail(1)
)

path_layer = pdk.Layer(
    'PathLayer',
    data=df_path,
    get_path='path',
    get_color=[101, 175, 255, 50],
    width_min_pixels=2,
)

scatter_history = pdk.Layer(
    'ScatterplotLayer',
    data=df_filtered,
    get_position='[longitude, latitude]',
    get_radius=500,
    radius_min_pixels=1,
    radius_max_pixels=10,
    get_fill_color=[101, 175, 255, 50],
    pickable=True,
)

scatter_last = pdk.Layer(
    'ScatterplotLayer',
    data=df_last,
    get_position='[longitude, latitude]',
    get_radius=1000,
    radius_min_pixels=1,
    radius_max_pixels=10,
    get_fill_color=[251, 177, 60],
    pickable=True,
)

if 'map_view_state' not in st.session_state:
    st.session_state['map_view_state'] = pdk.ViewState(
        latitude=df_filtered['latitude'].mean(),
        longitude=df_filtered['longitude'].mean(),
        zoom=4,
        pitch=0,
    )

st.pydeck_chart(
    pdk.Deck(
        layers=[path_layer, scatter_history, scatter_last],
        initial_view_state=st.session_state['map_view_state'],
    ),
    key='flight_map'
)

# --- PLOT ---

st.divider()


def create_plot(df, col) -> st.altair_chart:
    df_plot = df[['time_position_dt_utc', f'{col}']].dropna()

    line_chart = alt.Chart(df_plot).mark_line(color='#65afff').encode(
        x=alt.X('time_position_dt_utc:T', title='Time',
                axis=alt.Axis(format='%H:%M:%S')),
        y=alt.Y(f'{col}:Q', title=f'{col}')
    )

    point_chart = alt.Chart(df_plot).mark_circle(color='#65afff', size=60).encode(
        x='time_position_dt_utc:T',
        y=f'{col}:Q'
    )

    st.altair_chart(line_chart + point_chart, use_container_width=True)


if icao24:
    df_aircraft = df_filtered[
        df_filtered['icao24'].str.lower() == icao24].copy()
    df_aircraft = df_aircraft.sort_values('time_position_dt_utc').tail(n_files)

    if df_aircraft.empty:
        st.warning(f'No data found for aircraft with ICAO24 code {icao24}.')
    else:
        callsign = df_aircraft.loc[
            df_aircraft['icao24'].str.lower() == icao24.lower(),
            'callsign'
        ].iloc[0]
        if callsign:
            st.sidebar.info(f'The callsign of this aircraft is: {callsign}')
        else:
            st.sidebar.info('The callsign of this aircraft is not available.')

        col1, col2 = st.columns(2)

        with col1:
            st.markdown(f'##### Geo-altitude(m) of aircraft {callsign}')
            create_plot(df_aircraft, 'geo_altitude')

        with col2:
            st.markdown(f'##### Velocity(Km/H) of aircraft {callsign}')
            create_plot(df_aircraft, 'velocity_kmh')

else:
    st.warning('Please enter a ICAO24 code to view the plots')

time.sleep(30)
st.rerun()
