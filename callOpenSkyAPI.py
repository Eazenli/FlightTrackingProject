import os
from dotenv import load_dotenv
import requests
import json
from datetime import datetime, timedelta
"""
Token management + API calling 
"""
# --- get the env variables ---

load_dotenv()

token_url = os.getenv('TOKEN_URL')
api_url = os.getenv('API_URL')
client_id = os.getenv('Client_Id')
client_secret = os.getenv('Client_Secret')


class TokenManager:
    def __init__(self):
        '''
        call the API every 30 seconds, 
        so generate a new token 60 seconds before the real expiration time can avoid 401 error and latency
        '''
        self.token = None
        self.token_expire_time = None
        self.expire_time_margin = 60

    def get_token(self):
        r = requests.post(
            token_url,
            data={
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret})

        r.raise_for_status()
        token_data = r.json()
        self.token = token_data['access_token']
        expire_in = token_data['expires_in']

        self.token_expire_time = datetime.now() + timedelta(seconds=expire_in -
                                                            self.expire_time_margin)
        return self.token

    def get_valid_token(self):
        if self.token is None or self.token_expire_time is None or datetime.now() > self.token_expire_time:
            return self.get_token()
        return self.token

# --- call API with valid token ---


def call_states_api(token_manager) -> dict:
    valid_token = token_manager.get_valid_token()
    response = requests.get(
        url=api_url,
        headers={
            'Authorization': f'Bearer {valid_token}'
        },
        timeout=10
    )

    # when invalid token and force to get a new one:
    if response.status_code == 401:
        valid_token = token_manager.get_token()
        response = requests.get(
            url=api_url,
            headers={
                'Authorization': f'Bearer {valid_token}'
            },
            timeout=10
        )

    response.raise_for_status()
    data = response.json()
    return data


def call_tracks_api(icao24: str) -> dict:
    try:
        response = requests.get(
            url=f'https://opensky-network.org/api/tracks/all?icao24={icao24}&time=0',
            timeout=10
        )
        if response.status_code == 404:
            return None

        if response.status_code == 429:
            raise RuntimeError("Rate limit OpenSky atteinte")

        response.raise_for_status()
        data = response.json()
        return data

    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"Erreur HTTP OpenSky: {e}") from e
