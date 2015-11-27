# -*- coding: utf-8 -*-
from system_api import convert_hours_and_day_to_epoch
import system_api
import vk_api
import config
import sql_schedule_api
import logging
import time
import wg_api
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
        #first_skip_excusable
        first_skip = False
        post_in_queue = 1
        while is_exist_post_in_queue:
            time_for_post_in_future = int(convert_hours_and_day_to_epoch(hour, post_in_queue))
            if time_for_post_in_future in postponed_posts_time:
                post_in_queue += 1
                first_skip = False
            elif not first_skip:
                first_skip = True
                post_in_queue += 1
            else:
                is_exist_post_in_queue = False



        result.append((tag, hour, post_in_queue-1))
    return result


def when_my_post(nickname, vk_group):
    nickname = nickname.strip()
    if not wg_api.username_exist_wot_blitz_ru(nickname):
        return u"Указанный Никней не найден на Ру сервере WOT Blitz. Вот здесь можете себя проверить clck.ru/9cURR"
    posts_time = vk_api.find_posts_time_with_key_word(nickname, vk_group)
    result_dates = []
    for i in posts_time:
        date_of_post = system_api.day_and_month_from_epoch(i)
        result_dates.append(date_of_post)
    if not posts_time:
        result_dates = ["Пост с запрошенным ником в очереди не найден. Проверьте, что правильно указали свой ник, когда отправляли свою работу."]
    return "<br>".join(result_dates)



if __name__ == "__main__":
    #print how_many_posts_in_queue(config.vk_group)
    print when_my_post("Nyusha1909", config.vk_group)
