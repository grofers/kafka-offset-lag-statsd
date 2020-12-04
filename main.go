package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/kouhin/envflag"
	"gopkg.in/alexcesaro/statsd.v2"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	//"time"
)

var (
	activeOnly          = flag.Bool("active-only", false, "Show only consumers with an active consumer protocol.")
	kafkaBrokers        = flag.String("kafka-brokers", "localhost:9092", "Comma separated list of kafka brokers.")
	statsdHost          = flag.String("statsd-host", "localhost", "statsd address")
	statsdPort          = flag.String("statsd-port", "8125", "statsd address")
	statsdPrefix        = flag.String("statsd-prefix", "kafkalag", "statsd prefix")
	refreshInt          = flag.Int("refresh-interval", 1, "Time between offset refreshes in seconds.")
	saslUser            = flag.String("sasl-user", "", "SASL username.")
	saslPass            = flag.String("sasl-pass", "", "SASL password.")
	debug               = flag.Bool("debug", false, "Enable debug output.")
	algorithm           = flag.String("algorithm", "", "The SASL algorithm sha256 or sha512 as mechanism")
	enableCurrentOffset = flag.Bool("enable-current-offset", false, "Enables metrics for current offset of a consumer group")
)

type TopicSet map[string]map[int32]int64

func init() {
	if err := envflag.Parse(); err != nil {
		panic(err)
	}
	if *debug {
		sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	}
}

var statsdClient *statsd.Client
var err error

func main() {

	statsdAddr := *statsdHost + ":" + *statsdPort
	log.Printf("Statsd Host and Port %s\n", statsdAddr)
	statsdClient, err = statsd.New(statsd.Address(statsdAddr))
	if err != nil {
		panic(err)
	}

	go func() {

		config := sarama.NewConfig()
		config.ClientID = "kafka-offset-lag-statsd"
		config.Version = sarama.V0_9_0_0
		if *saslUser != "" {
			config.Net.SASL.Enable = true
			config.Net.SASL.User = *saslUser
			config.Net.SASL.Password = *saslPass
		}
		if *algorithm == "sha512" {
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
		} else if *algorithm == "sha256" {
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
		}
		client, err := sarama.NewClient(strings.Split(*kafkaBrokers, ","), config)

		if err != nil {
			log.Fatal("Unable to connect to given brokers.")
		}

		defer client.Close()

		for {
			topicSet := make(TopicSet)
			client.RefreshMetadata()
			topics, err := client.Topics()
			if err != nil {
				log.Printf("Error fetching topics: %s", err.Error())
				continue
			}

			for _, topic := range topics {

				if strings.HasPrefix(topic, "__") {
					continue
				}
				partitions, err := client.Partitions(topic)
				if err != nil {
					log.Printf("Error fetching partitions: %s", err.Error())
					continue
				}
				if *debug {
					log.Printf("Found topic '%s' with %d partitions", topic, len(partitions))
				}

				topicSet[topic] = make(map[int32]int64)
				for _, partition := range partitions {
					toff, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
					if err != nil {
						log.Printf("Problem fetching offset for topic '%s', partition '%d'", topic, partition)
						continue
					}
					topicSet[topic][partition] = toff
				}
			}

			var wg sync.WaitGroup

			// Now lookup our group data using the metadata
			for _, broker := range client.Brokers() {
				// Sarama plays russian roulette with the brokers
				broker.Open(client.Config())
				_, err := broker.Connected()
				if err != nil {
					log.Printf("Could not speak to broker %s. Your advertised.listeners may be incorrect.", broker.Addr())
					continue
				}

				wg.Add(1)

				go func(broker *sarama.Broker) {
					defer wg.Done()
					refreshBroker2(broker, topicSet)
				}(broker)
			}

			wg.Wait()
		}
	}()

	blockForever()
}

func blockForever() {
	select {}
}

func refreshBroker2(broker *sarama.Broker, topicSet TopicSet) {

	groupsRequest := new(sarama.ListGroupsRequest)
	groupsResponse, err := broker.ListGroups(groupsRequest)

	if err != nil {
		log.Printf("Could not list groups: %s\n", err.Error())
		return
	}

	for group, ptype := range groupsResponse.Groups {
		// do we want to filter by active consumers?
		if ptype != "consumer" {
			continue
		}

		if strings.Contains(group, "nyala") != true {
			continue
		}
		// This is not very efficient but the kafka API sucks
		for topic, data := range topicSet {
			offsetsRequest := new(sarama.OffsetFetchRequest)
			offsetsRequest.Version = 1
			offsetsRequest.ConsumerGroup = group
			for partition := range data {
				offsetsRequest.AddPartition(topic, partition)
			}

			offsetsResponse, err := broker.FetchOffset(offsetsRequest)
			if err != nil {
				log.Printf("Could not get offset: %s\n", err.Error())
			}

			for _, blocks := range offsetsResponse.Blocks {
				var lag_sum float64
				lag_sum = 0
				for partition, block := range blocks {
					if *debug {
						if lag_sum > 0 {
							log.Printf("Discovered group: %s, topic: %s, partition: %d, offset: %d, lag_sum: %d\n", group, topic, partition, block.Offset, lag_sum)
						}
					}
					// Offset will be -1 if the group isn't active on the topic
					if block.Offset >= 0 {
						// Because our offset operations aren't atomic we could end up with a negative lag
						lag := math.Max(float64(data[partition]-block.Offset), 0)
						lag_sum += lag
						//push to statsd here
						//lag on topic, partition, group
					}
				}
				if lag_sum > 0 {
					log.Printf("Final lag: %s, topic: %s, lag_sum: %d\n", group, topic, lag_sum)

					//var x float64 = lag_sum
					var y int64 = int64(lag_sum)

					statsdClient.Gauge(fmt.Sprintf("%s.test.topic.%s.consumer_group.%s.lag", *statsdPrefix, topic, group), y)

				}
			}
		}
	}
}
func refreshBroker(broker *sarama.Broker, topicSet TopicSet) {
	groupsRequest := new(sarama.ListGroupsRequest)
	groupsResponse, err := broker.ListGroups(groupsRequest)

	if err != nil {
		log.Printf("Could not list groups: %s\n", err.Error())
		return
	}

	for group, ptype := range groupsResponse.Groups {
		// do we want to filter by active consumers?
		if *activeOnly && ptype != "consumer" {
			continue
		}
		// This is not very efficient but the kafka API sucks
		for topic, data := range topicSet {
			offsetsRequest := new(sarama.OffsetFetchRequest)
			offsetsRequest.Version = 1
			offsetsRequest.ConsumerGroup = group
			for partition := range data {
				offsetsRequest.AddPartition(topic, partition)
			}

			offsetsResponse, err := broker.FetchOffset(offsetsRequest)
			if err != nil {
				log.Printf("Could not get offset: %s\n", err.Error())
			}

			for _, blocks := range offsetsResponse.Blocks {
				var lag_sum float64
				lag_sum = 0
				for partition, block := range blocks {
					if *debug {
						log.Printf("Discovered group: %s, topic: %s, partition: %d, offset: %d, lag_sum: %d\n", group, topic, partition, block.Offset, lag_sum)
					}
					// Offset will be -1 if the group isn't active on the topic
					if block.Offset >= 0 {
						// Because our offset operations aren't atomic we could end up with a negative lag
						lag := math.Max(float64(data[partition]-block.Offset), 0)
						lag_sum += lag
						//push to statsd here
						//lag on topic, partition, group
					}
				}
				log.Printf("Final lag: %s, topic: %s, lag_sum: %d\n", group, topic, lag_sum)
			}
		}
	}
}
