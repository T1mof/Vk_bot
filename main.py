import vk_api
from vk_api import VkUpload
from vk_api.utils import get_random_id
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
import threading
import pymysql
import pymysql.cursors
import requests
import re


def get_connection():
    connection = pymysql.connect(host="localhost",
                                 user="root",
                                 password="",
                                 db="vktest",
                                 charset="utf8mb4",
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection


def send_message(id_user, id_keyboard, message_text):
    try:
        vk.messages.send(
             user_id=id_user,
             random_id=get_random_id(),
             keyboard=open(id_keyboard, 'r', encoding='UTF-8').read(),
             message=message_text)
    except:
        print("Ошибка отправки сообщения у id" + id_user)


def add_new_line(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO user (iduser, position) VALUES (%s, %s)"
            cursor.execute(sql, (id_user, "1"))
        connection.commit()
    finally:
        connection.close()
    return


def take_position(id_user):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT position FROM user WHERE iduser = %s"
            cursor.execute(sql, (id_user))
            line = cursor.fetchone()
            if line is None:
                return_count = 0
            else:
                return_count = line["position"]
    finally:
        connection.close()
    return return_count


def update_position(id_user, new_position):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET position = %s WHERE iduser = %s"
            cursor.execute(sql, (new_position, id_user))
        connection.commit()
    finally:
        connection.close()
    return

def get_all_userid():
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT iduser FROM user"
            cursor.execute(sql)
            results = cursor.fetchall()
            iduser_values = [result['iduser'] for result in results]
    finally:
        connection.close()
    return iduser_values


def processing_message(id_user, message_text):
    number_position = take_position(id_user)

    if number_position == 0:
        if message_text == "начать":
            vk.messages.send(
                user_id=id_user,
                random_id=get_random_id(),
                message='Введите ваш уникальный код.')
            add_new_line(id_user)

    elif number_position == 1:
        if re.match(r"^\d{16}$", message_text):
            update_position(id_user, "2")
            send_message(id_user, "keyboard_main.json", "Вы успешно авторизовались!")
        else:
            vk.messages.send(
                user_id=id_user,
                random_id=get_random_id(),
                message='Неверный код. Попробуйте еще раз.')

    elif number_position == 2:
        if message_text == "Уведомления":
            all_users = get_all_userid()
            for id_user in all_users:
                send_message(id_user, "keyboard_main.json", "Напоминаем, что нужно оплатить занятия.")
                update_position(id_user, "2")
        elif message_text == "Расписание":
            send_message(id_user, "keyboard_main.json", 'Ваше расписание')
        else:
            send_message(id_user, "keyboard_main.json", "Непонятная команда")

    else:
        vk.messages.send(
            user_id=id_user,
            random_id=get_random_id(),
            message='Непонятная команда')


if __name__ == '__main__':
    while True:
        session = requests.Session()
        vk_session = vk_api.VkApi(token="")
        vk = vk_session.get_api()
        upload = VkUpload(vk_session)
        longpoll = VkBotLongPoll(vk_session, "225100819")
        try:
            for event in longpoll.listen():
                if event.type == VkBotEventType.MESSAGE_NEW and event.from_user:
                    threading.Thread(target=processing_message, args=(event.obj.message["from_id"], event.obj.message["text"])).start()
        except Exception:
            pass
