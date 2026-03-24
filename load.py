from pathlib import Path
import polars as pl


def load_recent_snapshot(n_files: int, sub_dir: str) -> pl.DataFrame:
    base_dir = Path(f'data/{sub_dir}')
    days = sorted(base_dir.glob('*'))
    if not days:
        return pl.DataFrame()

    latest_day = days[-1]

    files = sorted(latest_day.glob('*.parquet'))
    if not files:
        return pl.DataFrame()

    recent_files = files[-n_files:]

    return pl.read_parquet(recent_files)
