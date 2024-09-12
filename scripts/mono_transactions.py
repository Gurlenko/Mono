import requests


def get_data_from_api(account: int, from_date: int, to_date: int, xtoken: str) -> dict:
    url = f'https://api.monobank.ua/personal/statement/{account}/{from_date}/{to_date}'
    print(xtoken)
    print(url)
    headers = {
        'Content-Type': 'application/json',
        'X-Token': xtoken 
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    print(response.status_code)
    print(response.json())
    return response.json()



def from_json_to_dataframe():
    pass


def insert_in_postgres():
    pass