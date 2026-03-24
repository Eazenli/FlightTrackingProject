import polars as pl
from pathlib import Path
from datetime import datetime


def write_snapshot(df: pl.DataFrame, sub_dir: str, prefix: str, snapshot_time: datetime) -> None:
    """
    organise the snapshot parquet according to the hour and the day ans and save the data locally
    """

    day_str = snapshot_time.strftime('%Y-%m-%d')
    time_str = snapshot_time.strftime('%Hh%Mm%S')

    dir_path = Path('data') / f'{sub_dir}' / f'date={day_str}'
    dir_path.mkdir(parents=True, exist_ok=True)

    output_path = dir_path/f'{prefix}_{time_str}.parquet'

    df.write_parquet(output_path)


def load_recent_snapshot(n_files: int, sub_dir: str) -> pl.DataFrame:
    base_dir = Path(f'data/{sub_dir}')
    days = sorted(base_dir.glob('*'))
    if not days:
        return pl.DataFrame()

    latest_day = days[-1]

    files = sorted(latest_day.glob('*.parquet'))
    if not files:
        return pl.DataFrame()

    recent_files = files[-min(n_files, len(files)):]

    return pl.read_parquet(recent_files)
