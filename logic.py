# -*- coding: utf-8 -*-
from system_api import convert_hours_and_day_to_epoch
import vk_api
import config
import sql_schedule_api
import logging
import time
logging.basicConfig(level=logging.DEBUG)


def how_many_posts_in_queue(vk_group):
    # возвращаем в виде
    # (тэг, время, на сколько дней вперед есть посты)
    # (#blitztime, 10, 7)
    result = []
    postponed_posts_time = vk_api.get_postponed_posts_times(vk_group)
    schedule = sql_schedule_api.get_all_tags_and_time_in_order()
    for tag, hour in schedule:
        is_exist_post_in_queue = True
        post_in_queue = 0
        while is_exist_post_in_queue:
            time_for_post_in_future = int(convert_hours_and_day_to_epoch(hour, post_in_queue))
            if time_for_post_in_future not in postponed_posts_time and time_for_post_in_future > time.time():
                is_exist_post_in_queue = False
            else:
                post_in_queue += 1

        result.append((tag, hour, post_in_queue))
    return result


if __name__ == "__main__":
    print how_many_posts_in_queue(config.vk_group)
