import time
from pathlib import Path
import polars as pl
from datetime import datetime, timezone
from callOpenSkyAPI import TokenManager, call_states_api
from transform import create_raw_df, transform_raw_df


def write_snapshot(df: pl.DataFrame, sub_dir: str, prefix: str, snapshot_time: datetime) -> None:
    """
    organise the snapshot parquet according to the hour and the day
    """

    day_str = snapshot_time.strftime('%Y-%m-%d')
    time_str = snapshot_time.strftime('%Hh%Mm%S')

    dir_path = Path('data') / f'{sub_dir}' / f'date={day_str}'
    dir_path.mkdir(parents=True, exist_ok=True)

    output_path = dir_path/f'{prefix}_{time_str}.parquet'

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
                snapshot_time = datetime.now(timezone.utc)
                df_raw_pl = create_raw_df(data_json)
                write_snapshot(
                    df_raw_pl,
                    sub_dir='raw',
                    prefix='states',
                    snapshot_time=snapshot_time
                )
                # transform the raw data and ingest into another dir
                df_serving_pl = transform_raw_df(df_raw_pl)
                write_snapshot(
                    df_serving_pl,
                    sub_dir='serving',
                    prefix='serving',
                    snapshot_time=snapshot_time
                )
                calls += 1
                print(f'Calls: {calls} / {max_calls}')
                time.sleep(30)
            except Exception as e:
                print(f'Collector error: {e}')
                time.sleep(30)
        else:
            time.sleep(300)


if __name__ == "__main__":
    collect_states()
