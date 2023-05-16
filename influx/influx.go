package influx

import (
	"context"
	"crypto/x509"
	"fmt"
	"log"
	"net/url"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxApi "github.com/influxdata/influxdb-client-go/v2/api"
	influxDomain "github.com/influxdata/influxdb-client-go/v2/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	ORGANIZATION_TAG = "organization_id"
	PROJECT_TAG      = "project_id"

	MINT_SERIES         = "mints"
	MINT_COLLECTION_TAG = "collection"
	MINT_USER_KEY       = "user"
)

type ConnectParams struct {
	Url, Token, Organization, Bucket string
	BatchSize                        uint
}

func Connect(ctx context.Context, params ConnectParams) (client influxdb2.Client, write WriteClient, query QueryClient, err error) {
	client = influxdb2.NewClientWithOptions(
		params.Url,
		params.Token,
		influxdb2.DefaultOptions().SetBatchSize(params.BatchSize),
	)

	// _, err = client.Health(context.Background())
	// if err != nil {
	//   err = fmt.Errorf("Error running InfluxDB health check: %w", err)
	//   return
	// }

	//// Write
	buckets := client.BucketsAPI()

	bucket, bucketErr := buckets.FindBucketByName(ctx, params.Bucket)
	if bucketErr != nil {
		log.Printf("Couldn't find bucket '%s': %s", params.Bucket, bucketErr)
	}

	if bucket == nil {
		var org *influxDomain.Organization
		org, err = client.OrganizationsAPI().FindOrganizationByName(ctx, params.Organization)
		if err != nil {
			err = fmt.Errorf("Error finding organization '%s': %w", params.Organization, err)
			return
		}

		bucket, err = buckets.CreateBucketWithName(ctx, org, params.Bucket)
		if err != nil {
			err = fmt.Errorf("Error creating bucket '%s': %w", params.Bucket, err)
			return
		}
	}

	writeApi := client.WriteAPI(params.Organization, params.Bucket)

	go func() {
		for err := range writeApi.Errors() {
			log.Println("Error writing point: ", err)
		}
	}()

	write = WriteClient{writeApi}

	//// Query
	pool, err := x509.SystemCertPool()

	trans := grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, ""))
	opts := []grpc.DialOption{
		trans,
	}

	parsed, err := url.Parse(params.Url)
	if err != nil {
		err = fmt.Errorf("Error parsing InfluxDB URL: %w", err)
		return
	}

	arrowClient, err := flightsql.NewClient(parsed.Host, nil, nil, opts...)
	if err != nil {
		err = fmt.Errorf("Error constructing FlightSQL client: %w", err)
		return
	}

	query = QueryClient{
		arrow:  arrowClient,
		bucket: params.Bucket,
		bearer: params.Token,
	}

	return
}

type WriteClient struct{ influxApi.WriteAPI }

type QueryClient struct {
	arrow          *flightsql.Client
	bucket, bearer string
}

func (q *QueryClient) Project(ctx context.Context, orgId, projectId string) ProjectQueryClient {
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+q.bearer)
	ctx = metadata.AppendToOutgoingContext(ctx, "bucket-name", q.bucket)

	return ProjectQueryClient{q.arrow, ctx}
}

type ProjectQueryClient struct {
	arrow *flightsql.Client
	ctx   context.Context
}

func (q *ProjectQueryClient) Prepare(query string, opts ...grpc.CallOption) (ret PreparedStatement, err error) {
	stmt, err := q.arrow.Prepare(q.ctx, query, opts...)
	if err != nil {
		return
	}

	ret = PreparedStatement{stmt, q, opts}
	return
}

func (q *ProjectQueryClient) DoGet(ticket *flight.Ticket) (*flight.Reader, error) {
	return q.arrow.DoGet(q.ctx, ticket)
}

type PreparedStatement struct {
	s    *flightsql.PreparedStatement
	c    *ProjectQueryClient
	opts []grpc.CallOption
}

func (p *PreparedStatement) SetParameters(binding arrow.Record) {
	p.s.SetParameters(binding)
}

func (p *PreparedStatement) Execute() (*flight.FlightInfo, error) {
	return p.s.Execute(p.c.ctx, p.opts...)
}

func (p *PreparedStatement) Close() { p.s.Close(p.c.ctx, p.opts...) }
