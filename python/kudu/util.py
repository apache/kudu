# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
import six
from decimal import Decimal, getcontext
from pytz import utc


def _epoch():
    """
    Return the unix epoch in datetime.datetime form for the
    timezone provided.

    Returns
    -------
    epoch : datetime.datetime
    """
    return datetime.datetime.fromtimestamp(0, utc)


def indent(text, spaces):
    block = ' ' * spaces
    return '\n'.join(block + x for x in text.split('\n'))


def to_unixtime_micros(timestamp, format = "%Y-%m-%dT%H:%M:%S.%f"):
    """
    Convert incoming datetime value to a integer representing
    the number of microseconds since the unix epoch

    Parameters
    ---------
    timestamp : datetime.datetime or string
      If a string is provided, a format must be provided as well.
      A tuple may be provided in place of the timestamp with a
      string value and a format. This is useful for predicates
      and setting values where this method is indirectly called.
      Timezones provided in the string are not supported at this
      time. UTC unless provided in a datetime object.
    format : Required if a string timestamp is provided
      Uses the C strftime() function, see strftime(3) documentation.

    Returns
    -------
    int : Microseconds since unix epoch
    """
    # Validate input
    if isinstance(timestamp, datetime.datetime):
        pass
    elif isinstance(timestamp, six.string_types):
        timestamp = datetime.datetime.strptime(timestamp, format)
    elif isinstance(timestamp, tuple):
        timestamp = datetime.datetime.strptime(timestamp[0], timestamp[1])
    else:
        raise ValueError("Invalid timestamp type. " +
                         "You must provide a datetime.datetime or a string.")

    # If datetime has a valid timezone assigned, convert it to UTC.
    if timestamp.tzinfo and timestamp.utcoffset():
        timestamp = timestamp.astimezone(utc)
    # If datetime has no timezone, it is assumed to be UTC
    else:
        timestamp = timestamp.replace(tzinfo=utc)

    # Return the unixtime_micros for the provided datetime and locale.
    # Avoid timedelta.total_seconds() for py2.6 compatibility.
    td = timestamp - _epoch()
    td_micros = td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6
    return int(td_micros)


def from_unixtime_micros(unixtime_micros):
    """
    Convert the input unixtime_micros value to a datetime in UTC.

    Parameters
    ----------
    unixtime_micros : int
      Number of microseconds since the unix epoch.

    Returns
    -------
    timestamp : datetime.datetime in UTC
    """
    if isinstance(unixtime_micros, int):
        return _epoch() + datetime.timedelta(microseconds=unixtime_micros)
    else:
        raise ValueError("Invalid unixtime_micros value." +
                         "You must provide an integer value.")


def from_hybridtime(hybridtime):
    """
    Convert a raw HybridTime value to a datetime in UTC.

    Parameters
    ----------
    hybridtime : long

    Returns
    -------
    timestamp : datetime.datetime in UTC
    """
    # Add 1 so the value is usable for snapshot scans
    return from_unixtime_micros(int(hybridtime >> 12) + 1)


def to_unscaled_decimal(decimal, context=None):
    """
    Convert incoming decimal value to a int representing
    the unscaled decimal value.

    Parameters
    ---------
    decimal : Decimal
      The decimal value to convert to an unscaled int
    context :  Context
      The optional context to use

    Returns
    -------
    int : The unscaled decimal int
    """
    if context is None:
        context = getcontext()

    scale = get_decimal_scale(decimal)
    return decimal.scaleb(scale, context).to_integral_exact(None, context)


def from_unscaled_decimal(unscaled_decimal, scale, context=None):
    """
    Convert the input unscaled_decimal value to a Decimal instance.

    Parameters
    ----------
    unscaled_decimal : int
      The unscaled int value of a decimal
    scale : int
      The scale that should be used when converting
    context :  Context
      The optional context to use

    Returns
    -------
    decimal : The scaled Decimal
    """
    if context is None:
        context = getcontext()

    return Decimal(unscaled_decimal, context).scaleb(-scale, context)


def get_decimal_scale(decimal):
    """
       Get the scale of the decimal.

       Parameters
       ---------
       decimal : Decimal
         The decimal value

       Returns
       -------
       int : The calculated scale
       """
    return max(0, -decimal.as_tuple().exponent)

def unix_epoch_days_to_date(ndays):
    """
    Convert number of days since the unix epoch into datetime.date in UTC timezone.
    Number of days converted into timestamp by multiplying days on number
    of seconds per day (86400).

    Parameters
    ---------
    ndays : integer
      The number of days since the unix epoch

    Returns
    -------
    datetime.date : calendar date for "ndays" days since unix epoch
    """
    return datetime.datetime.utcfromtimestamp(ndays * 86400).date()

def date_to_unix_epoch_days(date):
    """
    Convert datetime.date value to a integer representing
    the number of days since the unix epoch.

    Parameters
    ---------
    date : datetime.date

    Returns
    -------
    int : Number of days since unix epoch
    """
    delta = date - datetime.datetime.utcfromtimestamp(0).date()
    return delta.days