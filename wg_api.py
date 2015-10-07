__author__ = 'onotole'
import config
#import doctest
import requests
import re


class InvalidClanException(Exception):
    pass


def get_clan_id_by_tag(clan_tag):
    """
    return clan_id by clan_tag

    >>> get_clan_id_by_tag("XG")
    10
    """
    assert type(clan_tag) == str
    req_url = 'https://api.wotblitz.ru/wotb/clans/list/?application_id=' + config.wargaming_id + '' \
        '&fields=tag%2Cclan_id&search=' + clan_tag
    req = requests.get(req_url).json()
    for clan_data in req["data"]:
        if clan_data["tag"] == clan_tag:
            return clan_data["clan_id"]
    raise InvalidClanException


def get_nicknames_by_ids(user_ids):
    """
    get users nickname by his id

    >>> get_nickname_by_id([30926868])
    {'NoOneiSPerfect'}
    """
    assert type(user_ids) == list
    if type(user_ids[0]) == int:
        user_ids = [str(x) for x in user_ids]
    req_url = 'https://api.wotblitz.ru/wotb/account/info/?application_id=' + config.wargaming_id + '&fields=nickname&' \
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
    set
    >>> type(get_nicknames_by_clan_tag("XG")[0])
    str
    >>> 'NoOneiSPerfect' in get_nicknames_by_clan_tag("XG")
    True
    """
    clan_id = get_clan_id_by_tag(clan_tag)
    req_url = "https://api.wotblitz.ru/wotb/clans/info/?application_id=" + config.wargaming_id + \
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




if __name__ == "__main__":
    #print(get_clan_id_by_tag("XG"))
    #print(get_nickname_by_id([30926868]))
    #check_text_for_user_from_clans("blah blah2*blah3")
    #print (get_nicknames_by_clan_tag("XG"))
    print(check_text_for_user_from_clans("WubZero % a;slkdjf aabs liasdflk Lihei 4 Gadino_"))
