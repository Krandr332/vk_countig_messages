import time

import requests
from datetime import datetime, timedelta

access_token = ''


def get_user_id():
    url = 'https://api.vk.com/method/users.get'
    params = {
        'access_token': access_token,
        'v': '5.131'
    }

    response = requests.get(url, params=params)
    data = response.json()

    if 'error' in data:
        print(f"Ошибка: {data['error']['error_msg']}")
        return None

    user_id = data['response'][0]['id']
    return user_id


def get_messages_count(start_time, end_time):
    your_user_id = get_user_id()
    if not your_user_id:
        print("Не удалось получить ID пользователя.")
        return -1

    url = 'https://api.vk.com/method/messages.getConversations'
    params = {
        'count': 200,
        'v': '5.131',
        'access_token': access_token
    }

    response = requests.get(url, params=params)
    data = response.json()

    if 'error' in data:
        print(f"Ошибка: {data['error']['error_msg']}")
        return -1

    total_messages_count = 0

    conversations = data['response']['items']
    for conversation in reversed(conversations):  # Обходим диалоги в обратном порядке
        peer_id = conversation['conversation']['peer']['id']
        messages_count = get_messages_count_for_chat(peer_id, start_time, end_time, your_user_id)
        total_messages_count += messages_count

    return total_messages_count


def get_messages_count_for_chat(peer_id, start_time, end_time, your_user_id):
    url = 'https://api.vk.com/method/messages.getHistory'
    total_messages_count = 0
    offset = 0
    has_more = True

    while has_more:
        params = {
            'peer_id': peer_id,
            'count': 200,
            'v': '5.131',
            'access_token': access_token,
            'start_time': start_time,
            'end_time': end_time,
            'offset': offset
        }

        response = requests.get(url, params=params)
        data = response.json()

        if 'error' in data:
            print(f"Ошибка: {data['error']['error_msg']}")
            return 0

        items = data['response']['items']
        if not items:
            break

        for item in items:
            if item['date'] < start_time:  # Если дата сообщения меньше времени начала, выходим из цикла
                has_more = False
                break

            if item['from_id'] == your_user_id:
                total_messages_count += 1

        offset += len(items)

        if len(items) < 200:
            break

    return total_messages_count


def vk_main(token):
    global access_token
    access_token = token
    start_time = datetime.now() - timedelta(days=1)
    start_time = start_time.replace(hour=10, minute=0, second=0, microsecond=0)
    end_time = datetime.now() - timedelta(days=1)
    end_time = end_time.replace(hour=22, minute=0, second=0, microsecond=0)

    start_time_unix = int(start_time.timestamp())
    end_time_unix = int(end_time.timestamp())

    count = get_messages_count(start_time_unix, end_time_unix)
    number = 0
    while count == -1:
        if number == 10:
            break
        number+=1
        print(count)
        count = get_messages_count(start_time_unix, end_time_unix)
        time.sleep(1)
    return count


print(vk_main("*************"))