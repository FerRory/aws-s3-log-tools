#!/usr/bin/env python
import fileinput
import sys
import re


# Source http://blog.kowalczyk.info/article/a1e/Parsing-s3-log-files-in-python.html

s3_line_logpats  = r'(\S+) (\S+) \[(.*?)\] (\S+) (\S+) ' \
           r'(\S+) (\S+) (\S+) "([^"]+)" ' \
           r'(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) ' \
           r'"([^"]+)" "([^"]+)"'

s3_line_logpat = re.compile(s3_line_logpats)

(S3_LOG_BUCKET_OWNER, S3_LOG_BUCKET, S3_LOG_DATETIME, S3_LOG_IP,
S3_LOG_REQUESTOR_ID, S3_LOG_REQUEST_ID, S3_LOG_OPERATION, S3_LOG_KEY,
S3_LOG_HTTP_METHOD_URI_PROTO, S3_LOG_HTTP_STATUS, S3_LOG_S3_ERROR,
S3_LOG_BYTES_SENT, S3_LOG_OBJECT_SIZE, S3_LOG_TOTAL_TIME,
S3_LOG_TURN_AROUND_TIME, S3_LOG_REFERER, S3_LOG_USER_AGENT) = range(17)

s3_names = ("bucket_owner", "bucket", "datetime", "ip", "requestor_id", 
"request_id", "operation", "key", "http_method_uri_proto", "http_status", 
"s3_error", "bytes_sent", "object_size", "total_time", "turn_around_time",
"referer", "user_agent")

def parse_s3_log_line(line):
    match = s3_line_logpat.match(line)
    result = [match.group(1+n) for n in range(17)]
    return result

def dump_parsed_s3_line(parsed):
    output = "{ "
    for (name, val) in zip(s3_names, parsed):
        output += ("\"%s\": \"%s\", " % (name, val) )
    sys.stdout.write(output[:-2] + " }")
    sys.stdout.write("\n")

def test():
    l = r'607c4573f2972c26aff39f7e56ff0490881a35c19b9bf94072cbab8c3219f948 kjkpub [06/Mar/2009:23:13:28 +0000] 41.221.20.231 65a011a29cdf8ec533ec3d1ccaae921c C46E93FF2E865AC1 REST.GET.OBJECT sumatrapdf/rel/SumatraPDF-0.9.1.zip "GET /sumatrapdf/rel/SumatraPDF-0.9.1.zip HTTP/1.1" 206 - 43457 1003293 697 611 "http://kjkpub.s3.amazonaws.com/sumatrapdf/rel/" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)"'
    parsed = parse_s3_log_line(l)
    dump_parsed_s3_line(parsed)

if __name__ == "__main__":
    for line in fileinput.input():
        parsed = parse_s3_log_line(line)
        dump_parsed_s3_line(parsed)
