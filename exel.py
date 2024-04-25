import json
import os.path
import datetime as dt
from os import getenv

import pymongo
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import datetime

from vk import vk_main

SCOPES = ["****"]

SAMPLE_SPREADSHEET_ID = "****"
SAMPLE_RANGE_NAME = "***"

ROOT = getenv("****", "")

LOCAL_FILES = f"local_files"

# Настройки MongoDB
_mongo_account = json.load(open(f'{LOCAL_FILES}/mongo_data.json'))
_login, _password = _mongo_account['login'], _mongo_account['password']
CA_FILE = f"{LOCAL_FILES}/CA.pem"
CONNECTION_STRING = f"mongodb://{_login}:{_password}@rc1b-okazhb06hqauc9ep.mdb.yandexcloud.net:****/?replicaSet=rs01&authSource=google_services"
DB_NAME = "google_services"

# Настройки Google Sheets
_collection = pymongo.MongoClient(CONNECTION_STRING, ssl=True, tlsCAFile=CA_FILE)["google_services"]["service_accounts"]
TABLE_CONFIG_C = _collection.find_one({"name": "counting_messages_bot_c"})
TABLE_CONFIG_T = _collection.find_one({"name": "counting_messages_bot_t"})
TABLE_KEY_LIST = "*********"
print(TABLE_CONFIG_C)
print(TABLE_CONFIG_T)


def create_dict_list1():
    creds = None
    if TABLE_CONFIG_T:
        creds = Credentials.from_authorized_user_info(
            info={
                'token': TABLE_CONFIG_T['token'],
                'refresh_token': TABLE_CONFIG_T['refresh_token'],
                'client_id': TABLE_CONFIG_T['client_id'],
                'client_secret': TABLE_CONFIG_T['client_secret'],
                'scopes': TABLE_CONFIG_T['scopes']
            }
        )

    try:
        service = build("sheets", "v4", credentials=creds)

        sheet1_result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME)
            .execute()
        )
        sheet1_values = sheet1_result.get("values", [])

        if not sheet1_values:
            print("No data found in Лист1.")
            return None
        else:
            data_list = []
            for row in sheet1_values[1:]:
                if len(row) >= 3 and (row[0] != "#Н/Д" and row[2] != "#Н/Д" and row[0] != "#N/A" and row[2] != "#N/A"):
                    row_dict = {
                        "ФИ": row[0],
                        "ТОКЕН": row[2]
                    }
                    data_list.append(row_dict)

            return data_list

    except HttpError as err:
        print(err)


def create_named_cells_and_write_data(service, free_column_range):
    try:
        today = dt.date.today()
        previous_date = today - dt.timedelta(days=1)
        date_string = previous_date.strftime("%d.%m")

        update_range = f"****{convert_to_column_name(free_column_range[0])}2:{convert_to_column_name(free_column_range[1])}2"
        value_input_option = "USER_ENTERED"
        value_range_body = {
            "values": [["Общее кол-во отправленных сообщений с 10:00 до 22:00", "Средняя скорость"]],
        }
        service.spreadsheets().values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            range=update_range,
            valueInputOption=value_input_option,
            body=value_range_body
        ).execute()

        update_range_date = f"Статистика!{convert_to_column_name(free_column_range[0])}1:{convert_to_column_name(free_column_range[1])}1"
        value_range_body_date = {
            "values": [[f"{date_string}"]]
        }
        service.spreadsheets().values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            range=update_range_date,
            valueInputOption=value_input_option,
            body=value_range_body_date
        ).execute()

        merge_request = {
            "requests": [
                {
                    "mergeCells": {
                        "range": {
                            "sheetId": 1909858738,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": free_column_range[0],
                            "endColumnIndex": free_column_range[1] + 1
                        },
                        "mergeType": "MERGE_ALL"
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": 1909858738,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": free_column_range[0],
                            "endColumnIndex": free_column_range[1] + 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "horizontalAlignment": "CENTER"
                            }
                        },
                        "fields": "userEnteredFormat.horizontalAlignment"
                    }
                }
            ]
        }

        service.spreadsheets().batchUpdate(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            body=merge_request
        ).execute()

        print("Шапка создана")

    except HttpError as err:
        print(err)


def update_statistics_sheet(service, data_list, free_column_range):
    try:
        today = dt.date.today()
        previous_date = today - datetime.timedelta(days=1)
        date_string = previous_date.strftime("%d.%m")

        b_column_index = None

        statistics_result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                 range=f"Статистика!A1:{convert_to_column_name(free_column_range[1])}")
            .execute()
        )
        statistics_values = statistics_result.get("values", [])

        if statistics_values:
            headers = statistics_values[0]
            for i, header in enumerate(headers):
                if header == date_string:
                    b_column_index = i + 1
                    break

        if b_column_index is not None:
            fio_list = [data['ФИ'] for data in data_list]

            for fio in fio_list:
                found = False
                for row in statistics_values:
                    if row and row[0] == fio:
                        found = True
                        break

                if found:
                    for data in data_list:
                        if data['ФИ'] == fio:
                            count_message = vk_main(data['ТОКЕН'])
                            for row_index, row_data in enumerate(statistics_values, start=1):
                                if row_data and row_data[0] == fio:
                                    update_range = f'Статистика!{convert_to_column_name(free_column_range[0])}{row_index}:{convert_to_column_name(free_column_range[1])}{row_index}'
                                    value_input_option = 'USER_ENTERED'
                                    value_range_body = {
                                        'values': [[count_message, count_message / 12]]
                                    }
                                    service.spreadsheets().values().update(
                                        spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                        range=update_range,
                                        valueInputOption=value_input_option,
                                        body=value_range_body
                                    ).execute()
                                    break
        else:
            print("Нужная дата не найдена в таблице.")

    except HttpError as err:
        print(err)


def find_free_column_range(service, spreadsheet_id, sheet_name):
    try:
        result = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = result.get('sheets', [])

        for sheet in sheets:
            if sheet['properties']['title'] == sheet_name:
                grid_properties = sheet['properties']['gridProperties']
                column_count = grid_properties['columnCount']

                for i in range(column_count - 2):
                    if is_column_empty(service, spreadsheet_id, sheet_name, i) and is_column_empty(service,
                                                                                                   spreadsheet_id,
                                                                                                   sheet_name, i + 1):
                        return i, i + 1

                for i in range(26, column_count - 2):
                    if is_column_empty(service, spreadsheet_id, sheet_name, i) and is_column_empty(service,
                                                                                                   spreadsheet_id,
                                                                                                   sheet_name, i + 1):
                        return i, i + 1

        print("Не удалось найти свободную пару столбцов.")
        return None

    except HttpError as err:
        print(err)
        return None


def is_column_empty(service, spreadsheet_id, sheet_name, column_index):
    try:
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                                     range=f"{sheet_name}!{convert_to_column_name(column_index)}1:{convert_to_column_name(column_index)}").execute()
        values = result.get('values', [])
        return not values

    except HttpError as err:
        print(err)
        return False


def convert_to_column_name(index):
    """Конвертирует индекс столбца в соответствующее название столбца.
    Например: 0 -> 'A', 1 -> 'B', ..., 25 -> 'Z', 26 -> 'AA', 27 -> 'AB', ..."""
    div, mod = divmod(index, 26)
    if div > 0:
        return convert_to_column_name(div - 1) + chr(mod + 65)
    else:
        return chr(mod + 65)


def main():
    creds = None

    if TABLE_CONFIG_T:
        creds = Credentials.from_authorized_user_info(
            info={
                'token': TABLE_CONFIG_T['token'],
                'refresh_token': TABLE_CONFIG_T['refresh_token'],
                'client_id': TABLE_CONFIG_T['client_id'],
                'client_secret': TABLE_CONFIG_T['client_secret'],
                'scopes': TABLE_CONFIG_T['scopes']
            }
        )

    service = build("sheets", "v4", credentials=creds)

    spreadsheet_id = "**************"
    sheet_id = "*****"
    free_column_range = find_free_column_range(service, spreadsheet_id, sheet_id)
    create_named_cells_and_write_data(service, free_column_range)
    data_list = create_dict_list1()
    update_statistics_sheet(service, data_list, free_column_range)


if __name__ == "__main__":
    main()
