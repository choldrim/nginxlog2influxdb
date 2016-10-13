package influxdb

import (
    "../parser"
    "fmt"

    "github.com/influxdata/influxdb/client/v2"
)

const (
    DBNAME = "mydb"
    HOST = "http://localhost:8086"
    USERNAME = "deepin"
    PASSWORD = "pylqKELdqRXHGU2a"  // testing random passwd
)

type DB struct {
    Client client.Client
}

func (db *DB)CleanUp(measurement string) error {
    q := client.NewQuery("drop measurement " + measurement, DBNAME, "s")
    if response, err := db.Client.Query(q); err == nil && response.Error() == nil {
        fmt.Println(response.Results)
    } else {
        return err
    }

    return nil
}

func New() (*DB, error) {
    c, err := client.NewHTTPClient(client.HTTPConfig{
        Addr: HOST,
        Username: USERNAME,
        Password: PASSWORD,
    })

    if err != nil {
        return nil, err
    }

    db := &DB{c}
    return db, nil
}

func NewBP() (client.BatchPoints, error) {
    bp, err := client.NewBatchPoints(client.BatchPointsConfig{
        Database: DBNAME,
        Precision: "s",
    })

    if err != nil {
        return nil, err
    }

    return bp, nil
}

func (db *DB)WriteRequests(requests []*parser.Request) error {
    // split into sub array
    points := make([]*parser.Request, 0)
    for _, r := range requests {
        points = append(points, r)
        if len(points) == 100 {
            db.WritePoints(points)
            points = points[:0]
        }
    }

    if len(points) > 0 {
        db.WritePoints(points)
    }

    return nil
}


func (db *DB)WritePoints(requests []*parser.Request) error {
    bp, err := NewBP()
    if err != nil {
        return err
    }

    fillBatchPoints(requests, bp)
    err = db.writeDB(&bp)
    if err != nil {
        return err
    }

    return nil
}


func (db *DB)writeDB(bp *client.BatchPoints) error {
    err := db.Client.Write(*bp)
    if err != nil {
        return err
    }

    return nil
}

func fillBatchPoints(requests []*parser.Request, bp client.BatchPoints) error {
    for _, r := range requests {
        fields := map[string]interface{} {
            "status_code": r.StatusCode,
            "bytes": r.Bytes,
        }

        tags := map[string]string{
            "ip": r.Ip,
            "method": r.Method,
            "path": r.Path,
            "proto": r.Proto,
            "referer": r.Referer,
            "agent": r.UserAgent,
        }

        pt, err := client.NewPoint(r.Project, tags, fields, r.Time)
        if err != nil {
            return err
        }

        bp.AddPoint(pt)
    }

    return nil
}
