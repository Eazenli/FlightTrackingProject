import time
from pathlib import Path
import polars as pl
from datetime import datetime, timezone
from transform import transform_to_pl_df
from callOpenSkyAPI import TokenManager, call_states_api


def write_snapshot(df: pl.DataFrame) -> None:
    """
    organise the snapshot parquet according to the hour and the day
    """
    now = datetime.now(timezone.utc)
    day_str = now.strftime('%Y-%m-%d')
    time_str = now.strftime('%Hh%Mm%S')

    dir_path = Path('data') / f'date={day_str}'
    dir_path.mkdir(parents=True, exist_ok=True)

    output_path = dir_path/f'states_{time_str}.parquet'

    df.write_parquet(output_path)


def collect_states():
    """
    call the API "/states/all" every 30 seconds and collect all the data 
    """
    token_manager = TokenManager()

    max_calls = 1000
    calls = 0
    current_day = datetime.now().date()

    while True:
        now = datetime.now().date()

        # reset the call after the midnight
        if now != current_day:
            current_day = now
            calls = 0

        if calls < max_calls:
            try:
                data_json = call_states_api(token_manager)
                df_pl = transform_to_pl_df(data_json)
                write_snapshot(df_pl)
                calls += 1
                time.sleep(30)
                print(f'Calls: {calls} / {max_calls}')
            except Exception as e:
                print(f'Collector error: {e}')
                time.sleep(30)
        else:
            time.sleep(300)


if __name__ == "__main__":
    collect_states()
