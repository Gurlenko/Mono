import requests


def get_data_from_api(account: int, from_date: int, to_date: int, token: str) -> dict:
    url = f'https://api.monobank.ua/personal/statement/{account}/{from_date}/{to_date}'

    headers = {
        'Content-Type': 'application/json',
        'X-Token': token 
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()

