import polars as pl
import boto3
import io
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

    # Create a Buffer memory
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    s3.upload_fileobj(buffer, bucket_name, s3_key)

    print(f'Uploaded: {s3_key}')


def load_recent_snapshot(n_files: int, sub_dir: str) -> pl.DataFrame:
    list_objects = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=f'{sub_dir}/'
    )

    if 'Contents' not in list_objects:
        return pl.DataFrame()

    # Retrieve all the object keys in S3 which ends with '.parquet'
    all_keys = [
        obj['Key'] for obj in list_objects['Contents'] if obj['Key'].endswith('.parquet')
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

    dfs = []
    for key in recent_keys:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        body = response['Body'].read()
        buffer = io.BytesIO(body)
        dfs.append(pl.read_parquet(buffer))

    return pl.concat(dfs) if dfs else pl.DataFrame()


if __name__ == '__main__':
    from datetime import timezone

    df = pl.DataFrame({
        "icao24": ["abc123"],
        "latitude": [48.85],
        "longitude": [2.35],
    })

    write_snapshot(
        df=df,
        sub_dir="test",
        prefix="test",
        snapshot_time=datetime.now(timezone.utc),
    )
