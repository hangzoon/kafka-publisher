package kafka

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
)

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(producer sarama.SyncProducer) *Producer {
	return &Producer{producer}
}

func ConnectProducer() sarama.SyncProducer {

	//setup relevant config info
	brokers := []string{viper.GetString("kafka.host") + ":" + viper.GetString("kafka.port")}
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	//Connect Kafka
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Printf("Error Connect Kafka : %v\n", err)
	}

	return producer
}

func (p *Producer) ProduceMessage(message []byte, topic string) error {
	log.Printf("Producer Massage to Kafka Topic : %v\n", topic)
	messageProducer := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: 1,
		Value:     sarama.StringEncoder(message),
		Headers: []sarama.RecordHeader{
			sarama.RecordHeader{
				Key:   []byte("X-CHANEL"),
				Value: []byte("NDID"),
			},
			sarama.RecordHeader{
				Key:   []byte("GATEWAY"),
				Value: []byte("RP_GW"),
			},
		},
	}

	partition, offset, err := p.producer.SendMessage(messageProducer)

	if err != nil {
		log.Printf("Error Producer Massage to Kafka : %v\n", err)
		return err
	}

	log.Printf("Reposne Kafka partition : %#v\n", partition)
	log.Printf("Reposne Kafka offset : %#v\n", offset)
	log.Printf("Reposne Kafka err : %#v\n", err)

	return nil
}
