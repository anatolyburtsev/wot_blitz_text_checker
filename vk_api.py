from vk_auth import token, call_api
import config
import logging


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
    group_info = call_api("groups.getById", [("group_id", url2group)], token)
    group_id = group_info[0]["gid"]
    return group_id


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