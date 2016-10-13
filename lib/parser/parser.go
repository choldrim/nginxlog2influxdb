package parser

import (
    "fmt"
    "time"
    "regexp"
    "strconv"
    "strings"
)

// log: $remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"
var logReg = regexp.MustCompile(`(\S+) - (\S+) \[([^\]]+)\] "([^"]+)" (\S+) (\S+) "([^"]*?)" "([^"]*?)"( "([^"]*?)")?`)
const TIME_FORMAT = "02/Jan/2006:15:04:05 -0700"

type Request struct {
    Project string
    Ip string
    Time time.Time
    Method string
    Path string
    Proto string
    StatusCode int
    Bytes uint64
    Referer string
    UserAgent string
}


func ParseRequest(line string, r *Request) error {
	matchs := logReg.FindAllStringSubmatch(line, 1)
    if len(matchs) == 0 {
        return fmt.Errorf("not a legal go line")
    }

    match := matchs[0]
    r.Ip = match[1]
	ts, err := time.Parse(TIME_FORMAT, match[3])
    if err != nil {
        return err
    }

    r.Time = ts
    r.StatusCode, err = strconv.Atoi(match[5])
    if err != nil {
        return fmt.Errorf("error: status code is not a number")
    }

    r.Bytes, err = strconv.ParseUint(match[6], 10, 64)
    if err != nil {
        return fmt.Errorf("error: bytes_sent is not a number")
    }

    r.Referer = match[7]
    r.UserAgent = match[8]
    err = parseRequestPart(match[4], r)
    if err != nil {
        return err
    }

    return nil
}

func parseRequestPart(str string, r *Request) error {
    fields := strings.Fields(str)
    if len(fields) != 3 {
        return fmt.Errorf("failed to parse request part: %s\n", str)
    }

    r.Method = fields[0]
    r.Path = fields[1]
    r.Proto = fields[2]
    return nil
}
