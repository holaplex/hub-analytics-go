//go:generate make proto

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/holaplex/hub-analytics/proto/analytics"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxApi "github.com/influxdata/influxdb-client-go/v2/api"
	influxDomain "github.com/influxdata/influxdb-client-go/v2/domain"
	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type ServiceConfig struct {
	MessageGroup    string
	RequestedTopics []string
	Name            string
}

func connectKafka(log *log.Logger, config *ServiceConfig) (client *kgo.Client, err error) {
	seedList := os.Getenv("KAFKA_BROKERS")
	brokerSeeds := strings.Split(seedList, ",")

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokerSeeds...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)),
		kgo.ConsumerGroup(fmt.Sprintf("%s@%s", config.MessageGroup, config.Name)),
		kgo.ConsumeTopics(config.RequestedTopics...),
	}

	user := os.Getenv("KAFKA_USERNAME")
	pass := os.Getenv("KAFKA_PASSWORD")
	sslStr := os.Getenv("KAFKA_SSL")

	ssl := false
	if len(sslStr) > 0 {
		ssl, err = strconv.ParseBool(sslStr)
		if err != nil {
			return
		}
	}

	if len(user) > 0 || len(pass) > 0 {
		opts = append(opts, kgo.SASL(scram.Sha512(func(ctx context.Context) (scram.Auth, error) {
			return scram.Auth{
				User: user,
				Pass: pass,
			}, nil
		})))
	}

	if ssl {
		dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 60 * time.Second}}

		opts = append(opts, kgo.Dialer(dialer.DialContext))
	}

	client, err = kgo.NewClient(opts...)
	if err != nil {
		return
	}

	return
}

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

func recvMessages(log *log.Logger, client *kgo.Client, ch chan<- *analytics.Datapoint) {
	for {
		fetches := client.PollFetches(context.Background())
		if fetches.IsClientClosed() {
			break
		}

		fetches.EachError(func(topic string, part int32, err error) {
			log.Printf("Kafka fetch error in topic %s, partition %d: %v", topic, part, err)
		})
	}
}

func drainErrors(log *log.Logger, ch <-chan error) {
	for err := range ch {
		log.Println("Error writing point: ", err)
	}
}

func main() {
	log := log.Default()

	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env: ", err)
	}

	config := &ServiceConfig{
		MessageGroup:    "AnalyticsGroup",
		RequestedTopics: []string{},
		Name:            "hub-analytics",
	}

	kafkaClient, err := connectKafka(log, config)
	if err != nil {
		log.Fatalln("Error connecting to Kafka: ", err)
	}
	defer kafkaClient.Close()

	msgs := make(chan *analytics.Datapoint)
	go recvMessages(log, kafkaClient, msgs)

	influxClient, write, err := connectInflux(log)
	if err != nil {
		log.Fatalln("Error connecting to InfluxDB: ", err)
	}
	defer influxClient.Close()

	go drainErrors(log, write.Errors())

	for msg := range msgs {
		insert(write, msg)
		write.Flush() // TODO
	}
}
