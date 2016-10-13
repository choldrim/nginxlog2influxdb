package main

import (
    "bufio"
    "compress/gzip"
    "fmt"
    libpath "path"
    "path/filepath"
    "regexp"
    "os"

    log "github.com/Sirupsen/logrus"

    "./lib/parser"
    "./lib/report"
    influxdb "./lib/db"
)


func getProjectName(path string) (string, error) {
    logReg := regexp.MustCompile(`(\S+)_access.log.\d+-\d+-\d+.gz`)
    fileName := libpath.Base(path)
    matchs := logReg.FindAllStringSubmatch(fileName, 1)
    if len(matchs) == 0 {
        return "", fmt.Errorf("Failed to extract project from log file name\n")
    }

    project := matchs[0][1]

    return project, nil
}


func _walkFunc(path string, info os.FileInfo, err error) error {
    if info.IsDir() {
        return nil
    }

    log.Info("handling ", path)

    project, err := getProjectName(path)
    if err != nil {
        return err
    }

    file, err := os.Open(path)
    if err != nil {
        return err
    }

    defer file.Close()

    gzipReader, err := gzip.NewReader(file)
    if err != nil {
        return err
    }

    defer gzipReader.Close()

    requests := make([]*parser.Request, 0)
    scan := bufio.NewScanner(gzipReader)

    rep := report.New()

    for scan.Scan() {
        line := scan.Text()
        r := parser.Request{}
        r.Project = project
        err := parser.ParseRequest(line, &r)
        if err != nil {
            rep.AddParsingError(err)
            //log.Warn(err)
        }
        requests = append(requests, &r)
    }

    if err := scan.Err(); err != nil {
        return err
    }

    db, err := influxdb.New()
    if err != nil {
        return err
    }
    defer db.Client.Close()

    // write to db
    err = db.WriteRequests(requests)
    if err != nil {
        return err
    }

    rep.Print()

    return nil
}


func work(path string) error {
    err := filepath.Walk(path, _walkFunc)
    if err != nil {
        return err
    }

    return nil
}


func main() {
    err := work("data")
    if err != nil {
        log.Fatalln(err)
    }
}
