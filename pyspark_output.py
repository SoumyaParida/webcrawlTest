import re
import datetime

from pyspark.sql import Row
import sys
import os
from pyspark import SparkContext

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parseApacheLogLine(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(4)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(4))
    return (Row(
        unique_id     = match.group(1),
        depth         = match.group(2),
        response_code = int(match.group(3)),
        content_size  = size,
        url           = match.group(5),
        cookies       = match.group(6),
        objecttype    = match.group(7),
        host          = match.group(8),
        ip_address    = match.group(9),
        asn_number    = match.group(10),
        start_time    = match.group(11),
        end_time      = match.group(12)
    ), 1)

APACHE_ACCESS_LOG_PATTERN = '(\S+)  (\d{1}) (\d{3}) (\w+)   (\S+)   (\S+)   (\S+)   (\S+)   (\S+)   (\S+)   (\S+)   (\S+)'



# baseDir = os.path.join('data')
# print baseDir
# print dir
# inputPath = os.path.join('cs100', 'lab2', 'apache.access.log.PROJECT')
# print inputPath
logFile = os.path.join('/home/soumya/Documents/courses/output1m.csv')
print logFile


sc=SparkContext("local", "pyspark_output.py")

# Row(    unique_id     = match.group(1),
#         depth         = match.group(2),
#         response_code = int(match.group(3)),
#         content_size  = size,
#         url           = match.group(5),
#         cookies       = match.group(6),
#         objecttype    = match.group(7),
#         host          = match.group(8),
#         ip_address    = match.group(9),
#         asn_number    = match.group(10),
#         start_time    = parse_apache_time(match.group(11)),
#         end_time      = parse_apache_time(match.group(12))
#     )

def parseLogs():
    """ Read and parse log file """
    parsed_logs = (sc.textFile(logFile).map(parseApacheLogLine).cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    print "failed",failed_logs_count
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
    return parsed_logs, access_logs, failed_logs


parsed_logs, access_logs, failed_logs = parseLogs()