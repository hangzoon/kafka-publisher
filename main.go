package main

import (
	"encoding/json"
	"log"
	"runtime"
	"strings"
	"test-connect-kafka/kafka"

	"github.com/spf13/viper"
	"github.com/tOnkowzl/libs/logx"
)

func main() {
	initConfig()
	a := kafka.NewProducer(kafka.ConnectProducer())
	msg, err := json.Marshal("MY_MESSAGE: HELLO WORLD [2]")
	if err != nil {
		log.Printf("Error Convert Message Kafka : %v\n", err)
	}
	a.ProduceMessage(msg, "my-topic-world-a")
}

func initConfig() {
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		logx.Panic(err)
	}

	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	runtime.GOMAXPROCS(1)

	logx.Init(viper.GetString("log.level"), viper.GetString("log.env"))
}
