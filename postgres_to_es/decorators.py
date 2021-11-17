import time
from functools import wraps


def backoff(
    exceptions: list,
    logger,
    title: str,
    start_sleep_time=0.1,
    factor=2,
    border_sleep_time=10,
    max_tries=100,
):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост
    времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param exceptions: Список Исключений, которые следует отловить
    :param logger: Логер для сохранения состояния программы
    :param title: Название процесса для backoff
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :param max_tries: граничное кол-во попыток запроса к БД
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            time_out = start_sleep_time
            for try_number in range(max_tries):
                logger.info(f"Try number: {try_number + 1} - delay {time_out}")
                try:
                    connection = func(*args, **kwargs)
                    return connection
                except exceptions as e:
                    logger.exception(
                        f"Try {title} again after\n"
                        f"time_out in {time_out} seconds\n"
                        f"Error message: {e}"
                    )
                    if time_out >= border_sleep_time:
                        time_out = border_sleep_time
                    else:
                        time_out += start_sleep_time * 2 ** factor
                    time.sleep(time_out)

        return inner

    return func_wrapper
