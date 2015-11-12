#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import cookielib
import urllib2
import urllib
from urlparse import urlparse
from HTMLParser import HTMLParser
import config
import json
from urllib import urlencode
import time
import logging


class RecursionError(Exception):
    pass


class APIErrorException(Exception):
    pass


def call_api(method, params, token, launch_counter=0):
    time.sleep(0.35)
    logging.debug("launch call_api. method:" + method + " params:" + str(params))
    if launch_counter == 5:
        logging.error("I had a recursion. method:" + method + " params:" + str(params))
        raise RecursionError
    params_initial = params[:]
    params.append(("access_token", token))
    url = "https://api.vk.com/method/%s?%s" % (method, urlencode(params))
    try:
        result_raw = json.loads(urllib2.urlopen(url).read())
    except urllib2.URLError:
        logging.error('couldnt load site ' + url)
        raise
    if "response" in result_raw.keys():
        result = result_raw["response"]
    elif "error" in result_raw.keys():
        if result_raw["error"]["error_code"] == 9:
            return True
        # Too many requests per second - error 6
        # captcha - error 14
        elif result_raw["error"]["error_code"] == 6 or result_raw["error"]["error_code"] == 14:
            time.sleep(5 + 15*launch_counter)
            logging.debug("Too many requests per second. method:" + method + " params:" + str(params))
            return call_api(method, params_initial, token, launch_counter+1)
        else:
            print(result_raw["error"]["error_msg"])
            logging.warning("error with vk api")
            logging.warning(url)
            logging.warning(result_raw)
            raise APIErrorException
    return result


class FormParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.url = None
        self.params = {}
        self.in_form = False
        self.form_parsed = False
        self.method = "GET"

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag == "form":
            if self.form_parsed:
                raise RuntimeError("Second form on page")
            if self.in_form:
                raise RuntimeError("Already in form")
            self.in_form = True
        if not self.in_form:
            return
        attrs = dict((name.lower(), value) for name, value in attrs)
        if tag == "form":
            self.url = attrs["action"]
            if "method" in attrs:
                self.method = attrs["method"].upper()
        elif tag == "input" and "type" in attrs and "name" in attrs:
            if attrs["type"] in ["hidden", "text", "password"]:
                self.params[attrs["name"]] = attrs["value"] if "value" in attrs else ""

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag == "form":
            if not self.in_form:
                raise RuntimeError("Unexpected end of <form>")
            self.in_form = False
            self.form_parsed = True

def auth(email, password, client_id, scope):
    def split_key_value(kv_pair):
        kv = kv_pair.split("=")
        return kv[0], kv[1]

    # Authorization form
    def auth_user(email, password, client_id, scope, opener):
        response = opener.open(
            "http://oauth.vk.com/oauth/authorize?" + \
            "redirect_uri=http://oauth.vk.com/blank.html&response_type=token&" + \
            "client_id=%s&scope=%s&display=wap" % (client_id, ",".join(scope))
            )
        doc = response.read()
        parser = FormParser()
        parser.feed(doc)
        parser.close()
        if not parser.form_parsed or parser.url is None or "pass" not in parser.params or \
          "email" not in parser.params:
              raise RuntimeError("Something wrong")
        parser.params["email"] = email
        parser.params["pass"] = password
        if parser.method == "POST":
            response = opener.open(parser.url, urllib.urlencode(parser.params))
        else:
            raise NotImplementedError("Method '%s'" % parser.method)
        return response.read(), response.geturl()

    # Permission request form
    def give_access(doc, opener):
        parser = FormParser()
        parser.feed(doc)
        parser.close()
        if not parser.form_parsed or parser.url is None:
              raise RuntimeError("Something wrong")
        if parser.method == "POST":
            response = opener.open(parser.url, urllib.urlencode(parser.params))
        else:
            raise NotImplementedError("Method '%s'" % parser.method)
        return response.geturl()


    if not isinstance(scope, list):
        scope = [scope]
    opener = urllib2.build_opener(
        urllib2.HTTPCookieProcessor(cookielib.CookieJar()),
        urllib2.HTTPRedirectHandler())
    doc, url = auth_user(email, password, client_id, scope, opener)
    if urlparse(url).path != "/blank.html":
        # Need to give access to requested scope
        url = give_access(doc, opener)
    if urlparse(url).path != "/blank.html":
        raise RuntimeError("Expected success here")
    answer = dict(split_key_value(kv_pair) for kv_pair in urlparse(url).fragment.split("&"))
    if "access_token" not in answer or "user_id" not in answer:
        raise RuntimeError("Missing some values in answer")
    return answer["access_token"], answer["user_id"]


def get_token(username, password, application_id, scopes):
    try:
        with open(config.vk_token_filename, 'r') as f:
            user_id = f.readline()
            token = f.readline()
            call_api("messages.get", [("count", 1)], token)
    except: #IOError or KeyError:
        logging.info("token for vk is outdated, start getting new")
        token, user_id = auth(username, password, application_id, scopes)
        logging.info("got new token for vk")
        f = open(config.vk_token_filename, 'w')
        f.write(user_id+'\n')
        f.write(token)
        f.close()
    return [user_id, token]


start_time = time.time()

user_id, token = get_token(config.vk_username, config.vk_password, config.vk_application_id, config.vk_scope)

elapsed = time.time() - start_time
logging.debug("Finish checking token for vk in " + str(elapsed) + " seconds")
