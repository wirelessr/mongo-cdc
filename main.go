package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type KeySchema struct {
	Schema  SchemaInfo `json:"schema"`
	Payload string     `json:"payload"`
}

type SchemaInfo struct {
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
}

func main() {
	// 創建 Kafka 生產者
	kafkaServer := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if kafkaServer == "" {
		kafkaServer = "localhost:9092"
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServer,
		"acks":              "all",
		"retries":           5,
	})
	if err != nil {
		log.Fatalf("創建Kafka生產者失敗: %v", err)
	}
	defer producer.Close()

	// 處理傳遞報告
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("傳遞失敗: %v\n", ev.TopicPartition.Error)
				} else {
					log.Printf("消息已發送到topic %s [%d] at offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	// MongoDB 連接設置
	ctx := context.Background()
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017"
	}

	mongoDB := os.Getenv("MONGO_DB")
	if mongoDB == "" {
		mongoDB = "test"
	}

	mongoCollection := os.Getenv("MONGO_COLLECTION")
	if mongoCollection == "" {
		mongoCollection = "test_new"
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	collection := client.Database(mongoDB).Collection(mongoCollection)

	// 設定Change Stream
	pipeline := mongo.Pipeline{}
	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetBatchSize(100)

	stream, err := collection.Watch(ctx, pipeline, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close(ctx)

	fmt.Println("開始監聽MongoDB變更...")

	for stream.Next(ctx) {
		var changeEvent bson.M
		if err := stream.Decode(&changeEvent); err != nil {
			log.Printf("解碼錯誤: %v\n", err)
			continue
		}

		// 獲取數據庫和集合名稱
		db := changeEvent["ns"].(bson.M)["db"].(string)
		collection := changeEvent["ns"].(bson.M)["coll"].(string)
		topic := fmt.Sprintf("%s.%s", db, collection)

		// 構建 key
		documentKey := changeEvent["documentKey"].(bson.M)
		keyJson, err := bson.MarshalExtJSON(documentKey, true, true)
		if err != nil {
			log.Printf("Key序列化錯誤: %v\n", err)
			continue
		}

		key := KeySchema{
			Schema: SchemaInfo{
				Type:     "string",
				Optional: false,
			},
			Payload: string(keyJson),
		}

		keyBytes, err := json.Marshal(key)
		if err != nil {
			log.Printf("Key結構序列化錯誤: %v\n", err)
			continue
		}

		// 將變更事件序列化為 JSON
		value, err := bson.MarshalExtJSON(changeEvent, true, true)
		if err != nil {
			log.Printf("Value序列化錯誤: %v\n", err)
			continue
		}

		// 發送消息到 Kafka
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            keyBytes,
			Value:          value,
		}, nil)

		if err != nil {
			log.Printf("發送到Kafka失敗: %v\n", err)
			continue
		}
	}

	// 確保所有消息都已發送
	producer.Flush(15 * 1000)

	if err := stream.Err(); err != nil {
		log.Fatal(err)
	}
}
