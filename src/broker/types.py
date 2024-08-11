from enum import Enum


class OrderType(Enum):
    MARKET = "MKT"
    LIMIT = "LMT"
    STOP = "STP"
    STOP_LIMIT = "STP LMT"
    TRAILING = "TRAIL"
    TRAILING_LIMIT = "TRAIL LIMIT"
    CABINET = "CABINET"
    NON_MARKETABLE = "NON_MARKETABLE"
    MARKET_ON_CLOSE = "MOC"
    MARKET_ON_OPEN = "MKT"
    LIMIT_ON_CLOSE = "LOC"


class TimeInForce(Enum):
    DAY = "DAY"
    AM = "AM"
    PM = "PM"
    EXTENDED = "EXTENDED"
    GOOD_TILL_CANCEL = "GOOD_TILL_CANCEL"
    GTC_EXTENDED = "GTC_EXT"
    FILL_OR_KILL = "FILL_OR_KILL"
