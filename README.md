# ✈️ Flight Tracking Project

## Overview

This project implements a flight tracking system using periodic API polling using the OpenSky Network API: https://openskynetwork.github.io/opensky-api/rest.html.

It collects aircraft state data every 30 seconds, stores raw and transformed data in a S3 bucket or local filesystem, and visualizes aircraft positions and trajectories using a Streamlit application.

The goal is to demonstrate a simple ELT data pipeline architecture with ingestion, transformation, storage, and visualization layers.

---

## Architecture

The pipeline follows an ELT approach: raw data is first stored, then transfored into a serving layer.

1. **Data Ingestion**
   * The OpenSky API is called every 30 seconds, with token management (refreshed every 30 minutes).
   * Aircraft states are retrieved as JSON data.

2. **Raw Data Storage**
   * Data is transformed into a Polars DataFrame as raw structured data.
   * Stored as Parquet files (`raw/`) and partitioned by date.

3. **Data Transformation**
   * Filtering (valid coordinates, speed threshold).
   * Feature engineering (timestamps, velocity in km/h).

4. **Serving Layer**
   * Cleaned data is stored as Parquet files (`serving/`) and partitioned by date.

5. **Visualization**
   * Streamlit reads recent snapshots from S3.
   * Displays aircraft positions and trajectories on a map by using PyDeck.
   * Displays a special aircraft's geo-altitude and velocity states by entering its ICAO24 code.
   * Streamlit app refreshes every 30 seconds to get the latest data.
6. **Containerization**
   * Docker Compose is used to separate the data pipeline and the Streamlit application
7. **Monitoring**
   * Explored AWS CloudTrail to monitor S3 read/write data events 
   

---

## Features

* periodic data polling 
* Raw + serving data architecture
* Parquet-based partitioning data lake (S3 or local)
* Aircraft trajectory visualization
* Interactive filtering in Streamlit
* Dockerized environment

---

## Tech Stack

* Python
* Polars (primary) & Pandas
* Streamlit
* PyDeck & Altair as visualization
* AWS S3
* Docker

---

## Project Structure

```
.
├── app.py               # Streamlit application
├── collector.py         # ELTL pipeline with periodic polling
├── transform.py         # Data transformation
├── callOpenSkyAPI.py    # Token management and API calls
├── storage/
│   ├── local.py         # Local storage backend
│   ├── s3.py            # S3 storage backend (main)
│   └── __init__.py      # Backend selector
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
```

## Usage

* Select the number of recent snapshots (between 1 and 20 snapshots)
* Filter aircraft by:
  * callsign
  * origin country
  * altitude
* Visualize:
  * aircraft positions
  * trajectories
  * evolutions of geo-altitude and velocity of an aircraft 
<img width="1876" height="901" alt="image" src="https://github.com/user-attachments/assets/fd31434c-be01-46d3-ba48-64a80ddc44ae" />

---

## Future Improvements

* Add a database (e.g., TimescaleDB or Timestream)
* Improve scalability with AWS ECS / Fargate
* Add historical analytics

---

## Notes

This project is designed as a learning exercise for:

* data engineering concepts 
* cloud architecture
* containerization
