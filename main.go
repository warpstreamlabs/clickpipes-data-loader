package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"golang.org/x/time/rate"
)

var (
	file             = flag.String("file", "./logs-532.csv.gz", "path to logs file (gzipped)")
	limitF           = flag.Int("limit", -1, "number of CSV rows to process, <= 0 means infinite")
	bootstrapBrokerF = flag.String("broker", "", "url for bootstrap broker")
	saslUsernameF    = flag.String("username", "", "username for SASL authentication")
	saslPasswordF    = flag.String("password", "", "password for SASL authentication")
	tls              = flag.Bool("tls", true, "use TLS to connect to Kafka")
	partitions       = flag.Int("partitions", 16, "how many partitions to create for the topic")
	topic            = flag.String("topic", "dns_logs_topic_test", "name of topic to create / write to")
	ratelimit        = flag.Int("ratelimit", 10_000, "target write rate (rows/s), <= 0 means infinite")
)

func main() {
	flag.Parse()

	if *file == "" {
		panic("file must not be empty")
	}
	if *bootstrapBrokerF == "" {
		panic("broker URL must not be empty")
	}
	if *saslUsernameF == "" {
		panic("SASL username must not be empty")
	}
	if *saslPasswordF == "" {
		panic("SASL password must not be empty")
	}

	file, err := os.Open(*file)
	if err != nil {
		panic(fmt.Sprintf("error opening file: %v", err))
	}
	defer file.Close()

	buf := bufio.NewReader(file)
	gzipReader, err := gzip.NewReader(buf)
	if err != nil {
		panic(fmt.Sprintf("error wrapping buffer in gzip reader: %v", err))
	}

	csvReader := csv.NewReader(gzipReader)

	limit := *limitF
	if limit <= 0 {
		limit = math.MaxInt
	}

	opts := kgoClientOpts()
	client, err := kgo.NewClient(opts...)
	if err != nil {
		panic(fmt.Sprintf("error creating Kafka client: %v", err))
	}

	admClient := kadm.NewClient(client)
	ctx, cc := context.WithTimeout(context.Background(), 15*time.Second)
	_, err = admClient.CreateTopic(ctx, int32(*partitions), 1, nil, *topic)
	cc()
	if err != nil && !strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
		panic(fmt.Sprintf("error creating topic: %v", err))
	}

	var (
		schema      []string
		limiter     *rate.Limiter
		start       = time.Now()
		writenBytes = 0
	)
	if *ratelimit > 0 {
		limiter = rate.NewLimiter(rate.Limit(*ratelimit), *ratelimit)
	}
	for i := 0; i < limit; i++ {
		if limiter != nil {
			if err := limiter.Wait(context.Background()); err != nil {
				panic(fmt.Sprintf("error waiting to acquire ratelimiter: %v", err))
			}
		}

		if i%10_000 == 0 {
			fmt.Printf("wrote %d records in %s, rows/s: %f, mib/s:%f\n",
				i, time.Since(start), float64(i)/(time.Since(start).Seconds()),
				(float64(writenBytes)/1024/1024)/(time.Since(start).Seconds()))
		}

		record, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			fmt.Println("done reading CSV!")
			break
		}

		if err != nil {
			panic(fmt.Sprintf("error reading csv row: %v", err))
		}

		if i == 0 {
			// First row in the CSV is the column names so we use it to auto-generate
			// the schema.
			schema = append(schema, record...)
			fmt.Printf("generated schema: %v\n", schema)
			continue
		}

		doc := map[string]string{}
		for i, field := range record {
			doc[schema[i]] = field
		}

		marshaled, err := json.Marshal(doc)
		if err != nil {
			panic(fmt.Sprintf("error marshaling document as JSON: %v", err))
		}

		writenBytes += len(marshaled)

		client.Produce(context.Background(), &kgo.Record{
			Value: marshaled,
		}, nil)
	}
}

func kgoClientOpts() []kgo.Opt {
	var (
		opts = []kgo.Opt{
			kgo.SeedBrokers(*bootstrapBrokerF),
			kgo.MaxBufferedRecords(1_000_000),
			kgo.ProducerBatchMaxBytes(16_000_000),
			kgo.RecordPartitioner(kgo.UniformBytesPartitioner(1_000_000, false, false, nil)),
			kgo.DefaultProduceTopic(*topic),
		}
	)
	if *saslPasswordF != "" {
		opts = append(opts, kgo.SASL(plain.Auth{
			User: *saslUsernameF,
			Pass: *saslPasswordF,
		}.AsMechanism()))
	}
	if *tls {
		opts = append(opts, kgo.DialTLS())
	}

	return opts
}
