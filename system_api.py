import time
import datetime
import logging


def convert_hours_and_day_to_epoch(time_string, skip_days=0):
    logging.debug('convert time: ' + str(time_string))
    if str(time_string).isdigit():
        hours = str(time_string)
        minutes = '0'
    else:
        hm_delimiter = ":"
        assert hm_delimiter in time_string
        hours, minutes = time_string.split(hm_delimiter)
    assert hours.isdigit()
    assert minutes.isdigit()
    assert 0 <= int(hours) < 25
    assert 0 <= int(minutes) < 61
    assert str(skip_days).isdigit()
    skip_days = int(skip_days)
    hours = int(hours)
    minutes = int(minutes)

    fd = future_day = time.localtime(time.time() + 24*60*60*skip_days)
    result_time = datetime.datetime(fd.tm_year, fd.tm_mon, fd.tm_mday, hours, minutes).strftime('%s')
    return result_time


def day_and_month_from_epoch(epoch):
    """
    >>> day_and_month_from_epoch(1449122400)
    '03 Dec 2015'
    """
    epoch = int(epoch)
    return time.strftime("%d %b %Y", time.gmtime(epoch))


def date_from_epoch(epoch):
    """
    >>> date_from_epoch(1449122400)
    {'year': 2015, 'day': 3, 'month': 12}
    """
    epoch = int(epoch)
    day = int(time.strftime("%d", time.gmtime(epoch)))
    month = int(time.strftime("%m", time.gmtime(epoch)))
    year = int(time.strftime("%Y", time.gmtime(epoch)))
    return {"day": day, "month": month, "year": year}


def unixtime_to_datetime(u):
    """

    >>> unixtime_to_datetime(1441866800)
    '2015-09-10 06:33:20'
    """
    return str(datetime.datetime.utcfromtimestamp(u))


def unixtime_to_weekofday(u):
    """
    >>> unixtime_to_weekofday(1441866800)
    3
    >>> unixtime_to_weekofday("1441866800")
    3
    """
    assert str(u).isdigit()
    dt = datetime.datetime.utcfromtimestamp(int(u))
    return dt.weekday()


def unixtime_to_month(u):
    """
    >>> unixtime_to_month(1441866800)
    9
    """
    assert str(u).isdigit()
    dt = datetime.datetime.utcfromtimestamp(int(u))
    return dt.month


# print convert_hours_and_day_to_epoch("10:00")
# print convert_hours_and_day_to_epoch(10)

if __name__ == "__main__":
    # print day_and_month_from_epoch(1441866800)
    # print (1441866800)
    print unixtime_to_datetime(1441866800)
    pass