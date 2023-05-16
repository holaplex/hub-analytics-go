package graph

import (
	"github.com/holaplex/hub-analytics/influx"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

type Resolver struct {
	Influx influx.QueryClient
	Org    string
}
