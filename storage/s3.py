import polars as pl
from pathlib import Path
import boto3
import os
from datetime import datetime

s3 = boto3.client('s3')
bucket_name = 'flight-tracker-eazen-211425018362-eu-west-1-an'


def write_snapshot(df: pl.DataFrame, sub_dir: str, prefix: str, snapshot_time: datetime) -> None:
    '''
    organise the snapshot parquet according to the hour and the day and then ingest into S3 bucket 
    '''

    day_str = snapshot_time.strftime('%Y-%m-%d')
    time_str = snapshot_time.strftime('%Hh%Mm%S')

    # S3 key
    s3_key = f'{sub_dir}/date={day_str}/{prefix}_{time_str}.parquet'
    # tempo file : create a 'tmp' dir in Windows, in Linux, use the 'tmp' dir
    if os.name == 'nt':
        base_dir = Path('tmp')
    else:
        base_dir = Path('/tmp')

    tmp_dir = base_dir / sub_dir / f'date={day_str}'
    tmp_dir.mkdir(parents=True, exist_ok=True)

    tmp_path = tmp_dir / f'{prefix}_{time_str}.parquet'

    df.write_parquet(tmp_path)
    s3.upload_file(tmp_path, bucket_name, s3_key)

    print(f'Uploaded: {s3_key}')


def load_recent_snapshot(n_files: int, sub_dir: str) -> pl.DataFrame:
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=f'{sub_dir}/'
    )

    if 'Contents' not in response:
        return pl.DataFrame()

    # Retrieve all the object keys in S3 which ends with '.parquet'
    all_keys = [
        obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')
    ]
    if not all_keys:
        return pl.DataFrame()

    # Just want the data of latest day
    latest_day = max(
        key.split('/')[1] for key in all_keys if key.startswith(f'{sub_dir}/date='))
    # Retrieve all the object keys within the latest day
    latest_day_keys = [key for key in all_keys if key.startswith(
        f'{sub_dir}/{latest_day}')]

    # Get the N latest snapshots
    recent_keys = sorted(latest_day_keys)[-min(n_files, len(latest_day_keys)):]

    if os.name == 'nt':
        base_dir = Path('tmp')
    else:
        base_dir = Path('/tmp')

    tmp_dir = base_dir / f'download_s3_{sub_dir}' / latest_day
    tmp_dir.mkdir(parents=True, exist_ok=True)

    local_files = []
    for key in recent_keys:
        local_file = tmp_dir / Path(key).name
        s3.download_file(bucket_name, key, str(local_file))
        local_files.append(str(local_file))

    return pl.read_parquet(local_files) if local_files else pl.DataFrame()


if __name__ == '__main__':

    # Check the connection with S3
    Path('tmp').mkdir(exist_ok=True)
    path = 'tmp/test.txt'
    with open(path, 'w') as f:
        f.write('hello')

    s3.upload_file(path, bucket_name, 'test/test.txt')
    print('Unpload ok')
