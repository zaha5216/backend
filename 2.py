import sqlite3

accruals = [
    (1, 1, 1),
    (2, 3, 2),
    (3, 23, 3),
    (4, 30, 5),
    (5, 11, 7),
    (6, 14, 10),
    (7, 21, 11),
    (8, 25, 4),
    (9, 23, 3),
]

payments = [
    (1, 5, 4),
    (2, 12, 2),
    (3, 6, 8),
    (4, 3, 9),
    (5, 12, 7),
    (6, 21, 1),
    (7, 1, 3),
    (8, 6, 5),
    (9, 14, 12),
    (10, 14, 11),
    (11, 13, 11),
]


def pay_function(cursor):
    """
    функция приводит в соответствие долги и платежи, принимает курсор БД sqlite3
    обращается к таблицам платежей и долгов и приводит их в соответствие в 2 этапа:

    сперва в соответствие долгам приводятся платежи совпадающие с ними по месяцу, и имеющие более позднюю дату
    затем остальные, имеющие более позднюю дату в порядке от самых старых долгов

    :param cursor:
    :return: таблица распределенных платежей в виде списка, а также список не нашедших соответствия платежей
    """

    # при выборке из БД упорядочиваем по возрастанию даты платежей и долгов
    sql = "SELECT * FROM accrual ORDER BY month, date"
    cursor.execute(sql)
    accrual_list = list(cursor.fetchall())

    sql = "SELECT * FROM payment ORDER BY month, date"
    cursor.execute(sql)
    payment_list = list(cursor.fetchall())

    # создаем словарь с ключами в виде долгов, которым в соответствие будем приводить id платежей.
    payment_table = {accrual: None for accrual in accrual_list}

    # проверка и присваивание первого этапа, более приоритетная
    for accrual in payment_table:
        if not payment_table[accrual]:
            for payment in payment_list:
                if accrual[2] == payment[2] and accrual[1] < payment[1]:
                    payment_table[accrual] = payment
                    payment_list.remove(payment)

    # здесь будут храниться платежи, которые не нашли себе долга
    list_of_not_used_payments = []

    # проверка и присваивание второго этапа, оставшиеся платежи
    searching = True
    while searching:
        if payment_list:
            for accrual in payment_table:
                if not payment_table[accrual]:
                    for payment in payment_list:
                        if payment[2] < accrual[2] or payment[2] == accrual[2] and payment[1] < accrual[1]:
                            list_of_not_used_payments.append(payment)
                            payment_list.remove(payment)
                        else:
                            payment_table[accrual] = payment
                            payment_list.remove(payment)
                        break
                    break
        else:
            searching = False
        if all(payment_table.values()):
            # Добавляем в список оставшиеся платежи
            list_of_not_used_payments.extend(payment_list)
            searching = False

    # Создадим список с кортежами из долгов и платежей.

    payment_table = [(accrual, payment) for accrual, payment in payment_table.items()]
    return payment_table, list_of_not_used_payments


if __name__ == '__main__':
    conn = sqlite3.connect("mydatabase.db")
    cur = conn.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS accrual(
       id INT PRIMARY KEY,
       date INT,
       month INT)
    """)

    cur.execute("""CREATE TABLE IF NOT EXISTS payment(
       id INT PRIMARY KEY,
       date INT,
       month INT)
    """)

    sql = "DELETE FROM accrual"
    cur.execute(sql)

    sql = "DELETE FROM payment"
    cur.execute(sql)
    cur.executemany("INSERT INTO accrual VALUES(?, ?, ?);", accruals)
    cur.executemany("INSERT INTO payment VALUES(?, ?, ?);", payments)

    conn.commit()

    print(pay_function(cur))
