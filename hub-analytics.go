//go:generate make gen

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

	"github.com/holaplex/hub-analytics/influx"
	"github.com/holaplex/hub-analytics/proto/analytics"
	"github.com/holaplex/hub-analytics/server"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"google.golang.org/protobuf/types/known/timestamppb"
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
			err = fmt.Errorf("Error parsing KAFKA_SSL env var: %w", err)
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
		err = fmt.Errorf("Error constructing Kafka client: %w", err)
		return
	}

	return
}

func connectInflux(log *log.Logger, ctx context.Context) (client influxdb2.Client, write influx.WriteClient, query influx.QueryClient, err error) {
	url := os.Getenv("DB_URL")
	tok := os.Getenv("DB_TOKEN")
	org := os.Getenv("DB_ORG")
	bucket := os.Getenv("DB_BUCKET")
	batchSizeStr := os.Getenv("INFLUX_BATCH_SIZE")

	batchSize := uint64(512)
	if len(batchSizeStr) > 0 {
		batchSize, err = strconv.ParseUint(batchSizeStr, 10, 32)
		if err != nil {
			err = fmt.Errorf("Error parsing INFLUX_BATCH_SIZE env var: %w", err)
			return
		}
	}

	client, write, query, err = influx.Connect(ctx, influx.ConnectParams{
		Url:          url,
		Token:        tok,
		Organization: org,
		Bucket:       bucket,
		BatchSize:    uint(batchSize),
	})

	return
}

func insert(
	write influx.WriteClient,
	ctx context.Context,
	msg *analytics.Datapoint,
) (err error) {
	ts := msg.GetTs()
	if ts == nil || msg.Payload == nil {
		return
	}

	series := ""
	tags := make(map[string]string)
	values := make(map[string]interface{})

	tags[influx.ORGANIZATION_TAG] = msg.GetOrganizationId()
	tags[influx.PROJECT_TAG] = msg.GetProjectId()

	switch payload := msg.Payload.(type) {
	case *analytics.Datapoint_Mint:
		series = influx.MINT_SERIES

		mint := payload.Mint
		if payload.Mint == nil {
			return
		}

		tags[influx.MINT_COLLECTION_TAG] = mint.CollectionId
		values[influx.MINT_USER_KEY] = mint.UserId
	default:
		return
	}

	write.WritePoint(influxdb2.NewPoint(series, tags, values, ts.AsTime()))
	return
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

	influxClient, write, query, err := connectInflux(log, context.Background())
	if err != nil {
		log.Fatalln("Error connecting to InfluxDB: ", err)
	}
	defer influxClient.Close()

	go server.Start(query)

	// TODO: remove this, stub for debugging insertion without Kafka producers
	//       being set up
	insert(write, context.Background(), &analytics.Datapoint{
		Ts:             timestamppb.Now(),
		OrganizationId: "e1e5f723-9639-488a-abef-f4263f8989e4",
		ProjectId:      "6c2b5151-299a-4bd0-8629-975f252afda5",
		Payload: &analytics.Datapoint_Mint{
			Mint: &analytics.MintDatapoint{
				UserId:       "1ff1be2a-0448-4c0c-9e9a-cec733679839",
				CollectionId: "foobar",
			},
		},
	})

	for msg := range msgs {
		insert(write, context.Background(), msg)
	}
}
