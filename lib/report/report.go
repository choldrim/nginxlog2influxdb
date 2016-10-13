package report

import (
    log "github.com/Sirupsen/logrus"
)

type Report struct {
    ParsingErrors []string
}

func New() *Report {
    return &Report{}
}

func (r *Report)Print() {
    log.Infof("Parsing error num: %d", len(r.ParsingErrors))
}

func (r *Report)AddParsingError(err error) {
    r.ParsingErrors = append(r.ParsingErrors, err.Error())
}
