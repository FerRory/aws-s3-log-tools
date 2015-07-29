// S3 logs to Keen IO

var AWS = require('aws-sdk');
var https = require('https');

console.log("Version 0.1.0");

var config = { 
         "keen_project_id": "example",
         "keen_write_key": "example"
}

var s3 = new AWS.S3();

exports.handler = function(event, context) {
  
    for (var i = 0; i < event.Records.length; i++) {
        var srcBucket = event.Records[i].s3.bucket.name;
        var srcKey = unescape(event.Records[i].s3.object.key);

        s3.getObject({ Bucket: srcBucket, Key: srcKey}, function(error, data) {
            if (error !== null) {
                context.done(error);
            } else {
                parsedLogs = parse(data.Body.toString());
                jsonData = { "s3logs": parsedLogs};
                sendEventData(jsonData, function(error, response) {
                    context.done(error, response);
                });
            }
            
        });
    }
};

// Source https://github.com/ifit/s3-log-parser
function parse(log) {
    var logs = log.split('\n')
    , parsedLogs = []
    , bracketRegEx = /\[(.*?)\]/
    , quoteRegex = /\"(.*?)\"/
    ;


    for(var i = 0; i < logs.length; i++) {
        var logString = logs[i];
        if(logString.length === 0) 
            continue;
        var time = bracketRegEx.exec(logString)[1];
        time = time.replace(/\//g, ' ');
        time = time.replace(/:/, ' ');
        time = new Date(time);
        logString = logString.replace(bracketRegEx, '');

        var requestUri = quoteRegex.exec(logString)[1];
        logString = logString.replace(quoteRegex, '');

        var referrer = quoteRegex.exec(logString)[1];
        logString = logString.replace(quoteRegex, '');

        var userAgent = quoteRegex.exec(logString)[1];
        logString = logString.replace(quoteRegex, '');

        var logStringSplit = logString.split(' ')
        , bucketOwner    = logStringSplit[0]
        , bucket         = logStringSplit[1]
        , remoteIp       = logStringSplit[3]
        , requestor      = logStringSplit[4]
        , requestId      = logStringSplit[5]
        , operation      = logStringSplit[6]
        , key            = logStringSplit[7]
        , statusCode     = logStringSplit[9]
        , errorCode      = logStringSplit[10]
        , bytesSent      = logStringSplit[11]
        , objectSize     = logStringSplit[12]
        , totalTime      = logStringSplit[13]
        , turnAroundTime = logStringSplit[14]
        , ctime          = logStringSplit[17]
        ;

        var event = {
            "bucket_owner":          bucketOwner,
            "bucket":                bucket,
            "ip":                    remoteIp,
            "requestor_id":          requestor,
            "request_id":            requestId,
            "operation":             operation,
            "key":                   key,
            "http_method_uri_proto": requestUri,
            "http_status":           (statusCode == '-' ? 0 : parseInt(statusCode, 10)),
            "s3_error":              errorCode,
            "bytes_sent":            (bytesSent == '-' ? 0 : parseInt(bytesSent, 10)),
            "object_size":           (objectSize == '-' ? 0 : parseInt(objectSize, 10)),
            "total_time":            (totalTime == '-' ? 0 : parseInt(totalTime, 10)),
            "turn_around_time":      (turnAroundTime == '-' ? 0 : parseInt(turnAroundTime, 10)),
            "referer":               referrer,
            "user_agent":            userAgent,
            "keen":                 { "timestamp": time }
        }

        parsedLogs.push(event);
    }

    return parsedLogs; 
}

// Source https://github.com/keen/keen-tracking.js
function sendEventData(eventData, callback) {
    var data = JSON.stringify(eventData);

    var options = {
        host: "api.keen.io",
        path: "/3.0/projects/" + config['keen_project_id'] + "/events",
        method: 'POST',
        headers: {
            'Authorization': config['keen_write_key'],
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(data)
        }
    };
    var req = https.request(options, function(response) {
        var body = '';
        response.on('data', function(d) {
            body += d;
        });
        response.on('end', function() {
            var res = JSON.parse(body), error;
            if (res.error_code) {
                error = new Error(res.message || 'Unknown error occurred');
                error.code = res.error_code;
                callback(error, null);
            }
            else {
                callback(null, res);
            }
        });
        
    });
    req.on('error', callback);
    req.write(data);
    req.end();
}
