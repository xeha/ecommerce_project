from urllib.parse import urlencode
import pandas as pd


import requests


BASE_URL = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'


def download_data(public_key: str, local_path: str = None, separ: str = ';', parse_date: list[str] = None) -> pd.DataFrame:
    """Функция позволяет загружать данные из указанного URL-адреса (с Яндекс Диска) или из локального файла,
    в зависимости от наличия успешного запроса.

    Args:
        public_key (str): публичный ключ, который используется для создания URL-адреса, с которого нужно загрузить данные.
        local_path (str, optional): (необязательный аргумент): путь к локальному файлу, который будет загружен, если запрос к URL-адресу неуспешен.
        separ (str, optional): разделитель, который будет использоваться для чтения загруженных данных. По умолчанию установлено значение ';'.
        parse_date (list[str], optional): столбец, который необходимо перевести в формат "дата и время"

    Returns:
        pd.DataFrame: возвращает df из исходной таблицы csv
    """
    # получаем url
    final_url = BASE_URL + urlencode(dict(public_key=public_key))
    response = requests.get(final_url)
    if response.ok:
        download_url_orders = response.json()['href']
        return pd.read_csv(download_url_orders, delimiter=separ, parse_dates = parse_date)

    return pd.read_csv(local_path, delimiter=separ, parse_dates = parse_date)