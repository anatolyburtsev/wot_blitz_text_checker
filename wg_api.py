# -*- coding: utf-8 -*-
__author__ = 'onotole'
import config
import doctest
import requests
import logging
import re


class InvalidClanException(Exception):
    pass


def get_clan_id_by_tag(clan_tag):
    """
    return clan_id by clan_tag

    >>> get_clan_id_by_tag("XG")
    10
    """
    assert type(clan_tag) == str or type(clan_tag) == unicode
    req_url = 'http://api.wotblitz.ru/wotb/clans/list/?application_id=' + config.wargaming_id + '' \
        '&fields=tag%2Cclan_id&search=' + clan_tag
    req = requests.get(req_url).json()
    for clan_data in req["data"]:
        if clan_data["tag"] == clan_tag:
            return clan_data["clan_id"]
    raise InvalidClanException


def get_nicknames_by_ids(user_ids):
    """
    get users nickname by his id

    >>> get_nicknames_by_ids([30926868])
    set([u'NoOneiSPerfect'])
    """
    assert type(user_ids) == list
    if type(user_ids[0]) == int:
        user_ids = [str(x) for x in user_ids]
    req_url = 'http://api.wotblitz.ru/wotb/account/info/?application_id=' + config.wargaming_id + '&fields=nickname&' \
        'account_id=' + ",".join(user_ids)
    req = requests.get(req_url).json()
    clan_nicknames = set()
    for nickname_data in req["data"].values():
        nickname = nickname_data["nickname"]
        clan_nicknames.add(nickname)

    return clan_nicknames


def get_nicknames_by_clan_tag(clan_tag):
    """
    return list of user_ids for use from clan with clan_tag
    >> type(get_nicknames_by_clan_tag("XG"))
    {'BANDIT_N1_', '____BAUR____', 'CrazyRussMan', 'RoniN311', 'tema62regeon', 'Misterformazon', 'LiverpoolRed'...}
    >>> type(get_nicknames_by_clan_tag("XG"))
    <type 'set'>
    >>> 'NoOneiSPerfect' in get_nicknames_by_clan_tag("XG")
    True
    """
    clan_id = get_clan_id_by_tag(clan_tag)
    req_url = "http://api.wotblitz.ru/wotb/clans/info/?application_id=" + config.wargaming_id + \
              "&fields=members_ids&clan_id=" + str(clan_id)
    req = requests.get(req_url).json()
    #print(req)
    users_list = req["data"][str(clan_id)]["members_ids"]
    nicknames_list = get_nicknames_by_ids(users_list)
    return nicknames_list


def extract_probable_nickname_from_text(text):
    """
    eject all words from text
    >>> extract_probable_nickname_from_text("blah blah2*blah3")
    ['blah', 'blah2', 'blah3']

    """
    p = re.compile('[a-zA-Z0-9_]+')
    good_words = p.findall(text)
    return good_words


def check_text_for_user_from_clans(text, clans_tag=config.clans_for_watching):
    """
    eject all words from text and check it for clan


    """
    possible_nicks = extract_probable_nickname_from_text(text)
    users_from_clan = set()
    nicks_from_our_clan = set()
    for clan_tag in clans_tag:
        users_from_clan.update(get_nicknames_by_clan_tag(clan_tag))

    for word in possible_nicks:
        if word in users_from_clan:
            nicks_from_our_clan.add(word)
    return list(nicks_from_our_clan)


def username_exist_wot_blitz_ru(username):
    """
    check nickname for exist at ru server
    >>> username_exist_wot_blitz_ru("NoOneIsPerfect")
    True
    >>> username_exist_wot_blitz_ru("Bl")
    False
    >>> username_exist_wot_blitz_ru(123123)
    False
    """
    if type(username) != str:# or type(username) != unicode:
        return False
    if len(username) < 3 or len(username) > 25:
        return False
    if " " in username:
        return False

    req_url = "http://api.wotblitz.ru/wotb/account/list/?application_id={}&type=exact&search={}".format(\
        config.wargaming_id, username)
    req = requests.get(req_url).json()
    if req["meta"]["count"] == 1:
        return True
    else:
        return False


def get_users_data_by_id(user_id):
    assert str(user_id).isdigit()
    user_id = str(user_id)
    req_url = "http://api.wotblitz.ru/wotb/account/info/?application_id={}&account_id={}".format(\
        config.wargaming_id, user_id)
    req = requests.get(req_url).json()
    try:
        nickname = req["data"][user_id]["nickname"]
        damage_dealt = req["data"][user_id]["statistics"]["all"]["damage_dealt"]
        frags = req["data"][user_id]["statistics"]["all"]["frags"]
    except KeyError:
        logging.error("bad user id: " + str(user_id))
        raise
        # return None
    return {"nickname": nickname, "damage_dealt": damage_dealt, "frags": frags}


def get_data_for_all_user_from_clans(clans_id):
    assert type(clans_id) == list
    all_members_list = []
    for clan_id in clans_id:
        members_list = [str(x) for x in get_clans_members_list_by_id(clan_id)]
        all_members_list += members_list
    req_url = "http://api.wotblitz.ru/wotb/account/info/?application_id={}" \
              "&fields=statistics.all.frags%2Cnickname%2Cstatistics.all.damage_dealt&" \
              "account_id={}".format(config.wargaming_id, ",".join(all_members_list))
    req = requests.get(req_url).json()
    try:
        data = req["data"]
    except:
        logging.error("PROBLEM with clan_id: " + str(clans_id))
        raise
    result_user_data = dict()
    # {user_id, [nickname, dmg, frags]}
    for user_id, user_data in data.items():
        try:
            nickname = user_data["nickname"]
            dmg = user_data["statistics"]["all"]["damage_dealt"]
            frags = user_data["statistics"]["all"]["frags"]
        except Exception as e:
            print (e)
            print ("user_id = " + str(user_id))
            print ("user_data = " + str(user_data))
        result_user_data[user_id] = [nickname, dmg, frags]
    return result_user_data



def get_clans_members_list_by_id(clan_id):
    assert str(clan_id).isdigit()
    clan_id = str(clan_id)
    req_url = "http://api.wotblitz.ru/wotb/clans/info/?application_id={}&fields=members_ids&clan_id={}".format(\
        config.wargaming_id, clan_id)
    req = requests.get(req_url).json()
    try:
        members_ids = req["data"][clan_id]["members_ids"]
    except KeyError:
        logging.error("bad clan id: " + clan_id)
        raise
    return members_ids


def get_all_clans_list():
    init_req_url = "http://api.wotblitz.ru/wotb/clans/list/?application_id={}&fields=clan_id%2Ctag&limit=1".format(
        config.wargaming_id
    )

    req = requests.get(init_req_url).json()
    number_of_clans = req["meta"]["total"]
    all_clans_data = dict()
    S = requests.session()

    for page_no in range(number_of_clans/100 +1):
        req_url = "http://api.wotblitz.ru/wotb/clans/list/?application_id={}&fields=clan_id%2Ctag%2Cmembers_co" \
                  "unt&limit=100".format(config.wargaming_id)

        if page_no > 0:
            req_url += "&page_no={}".format(str(page_no))

        req = S.get(req_url).json()
        try:
            small_clan_data = req["data"]
        except KeyError:
            print (req_url)
            print (req)
            raise
        for clan_data in small_clan_data:
            all_clans_data[clan_data["clan_id"]] = [clan_data["tag"], clan_data["members_count"]]
        # print "page number " + str(page_no) + " done"

    return all_clans_data


def get_all_tanks_name():
    req_url = "https://api.wotblitz.ru/wotb/encyclopedia/vehicles/?application_id={}&fields=name".format(
        config.wargaming_id)
    req = requests.get(req_url).json()
    for i in req["data"].values():
        print i["name"]


def get_all_tanks_name_pc():
    req_url = "https://api.worldoftanks.ru/wot/encyclopedia/vehicles/?application_id={}&fields=name".format(
        config.wargaming_id)
    req = requests.get(req_url).json()
    for i in req["data"].values():
        print i["name"]


if __name__ == "__main__":
    # get_all_tanks_name()
    get_all_tanks_name_pc()
    pass
    # print(get_clan_id_by_tag("XG"))
    # print(get_nicknames_by_ids([30926868]))
    # check_text_for_user_from_clans("blah blah2*blah3")
    # print (get_nicknames_by_clan_tag("XG"))
    # print(check_text_for_user_from_clans("WubZero % a;slkdjf aabs liasdflk Lihei 4 Gadino_"))
    # print get_users_data_by_id(1388496)
    # print get_clans_members_list_by_id(10)
    #print get_data_for_all_user_from_clans([855])
