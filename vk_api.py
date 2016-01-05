# -*- coding: utf-8 -*-
from vk_auth import token, call_api, AccessTokenExpiredException
import config
import os
import logging
import system_api


def get_group_id_by_url(url2group):
    """
    >>> get_group_id_by_url("http://vk.com/team")
    22822305
    >>> get_group_id_by_url(22822305)
    22822305
    >>> get_group_id_by_url("22822305")
    22822305
    >>> get_group_id_by_url("vk.com/team")
    22822305
    """
    logging.debug("start get_group_id_by_url")
    if str(url2group).isdigit():
        return int(url2group)
    url2group = url2group.split('/')[-1]
    try:
        group_info = call_api("groups.getById", [("group_id", url2group)], token)
    except AccessTokenExpiredException:
        os.remove(config.vk_token_filename)
        group_info = call_api("groups.getById", [("group_id", url2group)], token)
    group_id = group_info[0]["gid"]
    return group_id


def get_all_posts(group_id):
    # date = {"year": 2015, "month": 12, day: 3 }
    logging.debug("start getting all posts")
    all_posts = []
    group_id = get_group_id_by_url(group_id)
    if int(group_id) > 0:
        group_id = str(-int(group_id))
    posts_data_raw = call_api("wall.get", [("owner_id", group_id), ("count", 1)], token)
    count = 100
    offset = -count
    while offset + count < posts_data_raw[0]:
        offset += count
        # print "offset = {}".format(offset)
        posts_data_raw = call_api("wall.get", [("owner_id", group_id), ("count", count),
                                               ("offset", offset)], token)
        all_posts += posts_data_raw[1:]
        date_last_post = system_api.date_from_epoch(posts_data_raw[-1]["date"])
    return all_posts


def count_likes_for_posts(posts_list):
    likes = 0
    reposts = 0
    for post in posts_list:
        like = post["likes"]["count"]
        likes += int(like)
        repost = post["reposts"]["count"]
        reposts += int(repost)
    return (likes, reposts)


def get_postponed_posts_times(group_id):
    logging.debug("start getting postponed posts")
    group_id = get_group_id_by_url(group_id)
    postponed_posts_times = []
    if int(group_id) > 0:
        group_id = str(-int(group_id))
    posts_data_raw = call_api("wall.get", [("owner_id", group_id), ("filter", "postponed"), ("count", 1)], token)
    count = 100
    offset = -count
    while offset + count < posts_data_raw[0]:
        offset += count
        posts_data_raw = call_api("wall.get", [("owner_id", group_id), ("filter", "postponed"), ("count", count),
                                               ("offset", offset)], token)
        for post_data in posts_data_raw[1:]:
            postponed_posts_times.append(post_data["date"])
    logging.debug("finish getting postponed posts")
    return postponed_posts_times


def get_postponed_posts_times_with_text(group_id):
    logging.debug("start getting postponed posts with text")
    group_id = get_group_id_by_url(group_id)
    postponed_posts_times_with_text = dict()
    if int(group_id) > 0:
        group_id = str(-int(group_id))
    posts_data_raw = call_api("wall.get", [("owner_id", group_id), ("filter", "postponed"), ("count", 1)], token)
    count = 100
    offset = -count
    while offset + count < posts_data_raw[0]:
        offset += count
        posts_data_raw = call_api("wall.get", [("owner_id", group_id), ("filter", "postponed"), ("count", count),
                                               ("offset", offset)], token)
        for post_data in posts_data_raw[1:]:
            postponed_posts_times_with_text[post_data["date"]] = post_data["text"]
    logging.debug("finish getting postponed posts with text")
    return postponed_posts_times_with_text


def find_posts_time_with_key_word(keyword, group_id):
    posts = get_postponed_posts_times_with_text(group_id)
    result_times = []
    for post_time, post_text in posts.items():
        if keyword.lower() in post_text.lower():
            result_times.append(post_time)
    return result_times


def get_name_by_id(user_id):
    # if not str(user_id).isdigit():
    #     print "I'm here"
    #     return user_id
    user_id = str(abs(int(user_id)))
    response = call_api("users.get", [("user_ids", user_id)], token)
    try:
        result =  response[0]["first_name"]+ " " + response[0]["last_name"]
    except IndexError:
        print user_id
        raise
    return result


if __name__ == "__main__":
    # print count_likes_for_posts(get_all_posts(config.vk_group))
    print get_name_by_id(-79514596)
    # print get_name_by_id(79514596)
#     print get_postponed_posts_times_with_text(config.vk_group)
#     print find_posts_time_with_key_word(u"Maxshef91", config.vk_group)

