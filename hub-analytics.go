//go:generate make proto

package main

import (
	"context"
	"log"
	"os"

	"google.golang.org/protobuf/types/known/timestamppb"
	"github.com/holaplex/hub-analytics/proto/analytics"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxApi "github.com/influxdata/influxdb-client-go/v2/api"
	influxDomain "github.com/influxdata/influxdb-client-go/v2/domain"
	"github.com/joho/godotenv"
)

func connectInflux(log *log.Logger) (client influxdb2.Client, write influxApi.WriteAPI, err error) {
	url := os.Getenv("DB_URL")
	tok := os.Getenv("DB_TOKEN")
	client = influxdb2.NewClient(url, tok)

	_, err = client.Health(context.Background())
	if err != nil {
		return
	}

	orgName := os.Getenv("DB_ORG")
	bucketName := os.Getenv("DB_BUCKET")
	buckets := client.BucketsAPI()
	ctx := context.Background()

	bucket, bucketErr := buckets.FindBucketByName(ctx, bucketName)
	if err != nil {
		log.Printf("Couldn't find bucket %s: %s", bucketName, bucketErr)
	}

	if bucket == nil {
		var org *influxDomain.Organization
		org, err = client.OrganizationsAPI().FindOrganizationByName(ctx, orgName)
		if err != nil {
			return
		}

		bucket, err = buckets.CreateBucketWithName(ctx, org, bucketName)
		if err != nil {
			return
		}
	}

	write = client.WriteAPI(orgName, bucketName)

	return
}

func insert(
	write influxApi.WriteAPI,
	msg *analytics.Datapoint,
) {
	values := map[string]interface{}{}

	for k, v := range msg.Values {
		if v == nil {
			values[k] = nil
			continue
		}

		switch v.Value.(type) {
		case *analytics.Datapoint_Value_Unsigned:
			values[k] = v.GetUnsigned()
		case *analytics.Datapoint_Value_Signed:
			values[k] = v.GetSigned()
		case *analytics.Datapoint_Value_String_:
			values[k] = v.GetString_()
		default:
			panic("Unexpected datapoint value type")
		}
	}

	write.WritePoint(influxdb2.NewPoint(msg.Series, msg.Tags, values, msg.Ts.AsTime()))
}

func drainErrors(ch <-chan error) {
	for {
		err := <-ch

		log.Println("Error writing point: ", err)
	}
}

func main() {
	log := log.Default()

	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env: ", err)
	}

	client, write, err := connectInflux(log)
	if err != nil {
		log.Fatalln("Error connecting to InfluxDB: ", err)
	}

	go drainErrors(write.Errors())

	insert(
		write,
		&analytics.Datapoint{
			Ts:     timestamppb.Now(),
			Series: "nft",
			Tags:   map[string]string{"nft": "foo"},
			Values: map[string]*analytics.Datapoint_Value{"fuc": nil},
		},
	)
	write.Flush()

	log.Println("Client: ", client)
}
