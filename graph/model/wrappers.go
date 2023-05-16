package model

import "time"

type Project struct {
	Id string
}

type Drop struct {
	Project *Project
	Id      string
}

type DropStats struct {
	Drop  *Drop
	Start time.Time
	End   *time.Time
}

type DropTimeSeriesStats struct {
	Stats  *DropStats
	Window Window
}
