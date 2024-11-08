package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

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

type Message struct {
	Topic string
	Key   []byte
	Value []byte
}

func main() {
	// Kafka 配置優化
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":               getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		"acks":                           getEnv("KAFKA_ACKS", "1"),
		"retries":                        5,
		"linger.ms":                      10,     // 批次延遲時間
		"batch.size":                     1048576, // 1MB批次大小
		"compression.type":               "snappy",
		"queue.buffering.max.messages":   100000,
		"queue.buffering.max.ms":         500,
		"enable.idempotence":            true,    // 啟用冪等性
		"max.in.flight.requests.per.connection": 5,
	})
	if err != nil {
		log.Fatalf("創建Kafka生產者失敗: %v", err)
	}
	defer producer.Close()

	// 建立delivery report channel
	deliveryChan := make(chan kafka.Event, 10000)
	go handleDeliveryReports(deliveryChan)

	// MongoDB 連接優化
	ctx := context.Background()
	clientOptions := options.Client().
		ApplyURI(getEnv("MONGO_URI", "mongodb://localhost:27017")).
		SetMaxPoolSize(100).
		SetMaxConnIdleTime(30 * time.Second).
		SetCompressors([]string{"snappy"})

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	collection := client.Database(getEnv("MONGO_DB", "test")).
		Collection(getEnv("MONGO_COLLECTION", "test_new"))

	// Change Stream 設置
	pipeline := mongo.Pipeline{}
	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetBatchSize(1000)

	stream, err := collection.Watch(ctx, pipeline, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close(ctx)

	// 消息處理channel
	messageChan := make(chan Message, 1000)
	
	// 啟動單一批次處理goroutine
	go processBatchesInOrder(messageChan, producer, deliveryChan)

	fmt.Println("開始監聽MongoDB變更...")

	// 主循環：按順序讀取變更事件
	for stream.Next(ctx) {
		var changeEvent bson.M
		if err := stream.Decode(&changeEvent); err != nil {
			log.Printf("解碼錯誤: %v\n", err)
			continue
		}

		// 準備消息
		msg, err := prepareMessage(changeEvent)
		if err != nil {
			log.Printf("準備消息錯誤: %v\n", err)
			continue
		}

		// 發送到處理channel
		messageChan <- msg
	}

	close(messageChan)
	producer.Flush(15 * 1000)

	if err := stream.Err(); err != nil {
		log.Fatal(err)
	}
}

// 準備消息
func prepareMessage(changeEvent bson.M) (Message, error) {
	db := changeEvent["ns"].(bson.M)["db"].(string)
	coll := changeEvent["ns"].(bson.M)["coll"].(string)
	topic := fmt.Sprintf("%s.%s", db, coll)

	documentKey := changeEvent["documentKey"].(bson.M)
	keyJson, err := bson.MarshalExtJSON(documentKey, true, true)
	if err != nil {
		return Message{}, fmt.Errorf("key序列化錯誤: %v", err)
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
		return Message{}, fmt.Errorf("key結構序列化錯誤: %v", err)
	}

	value, err := bson.MarshalExtJSON(changeEvent, true, true)
	if err != nil {
		return Message{}, fmt.Errorf("value序列化錯誤: %v", err)
	}

	return Message{
		Topic: topic,
		Key:   keyBytes,
		Value: value,
	}, nil
}

// 按順序處理批次消息
func processBatchesInOrder(messageChan chan Message, producer *kafka.Producer, deliveryChan chan kafka.Event) {
	const batchSize = 100
	const batchTimeout = 100 * time.Millisecond

	batch := make([]Message, 0, batchSize)
	timer := time.NewTimer(batchTimeout)
	defer timer.Stop()

	for {
		select {
		case msg, ok := <-messageChan:
			if !ok {
				// 處理最後的批次
				if len(batch) > 0 {
					sendBatchInOrder(batch, producer, deliveryChan)
				}
				return
			}

			batch = append(batch, msg)
			if len(batch) >= batchSize {
				sendBatchInOrder(batch, producer, deliveryChan)
				batch = make([]Message, 0, batchSize)
				timer.Reset(batchTimeout)
			}

		case <-timer.C:
			if len(batch) > 0 {
				sendBatchInOrder(batch, producer, deliveryChan)
				batch = make([]Message, 0, batchSize)
			}
			timer.Reset(batchTimeout)
		}
	}
}

// 按順序發送批次
func sendBatchInOrder(batch []Message, producer *kafka.Producer, deliveryChan chan kafka.Event) {
	for _, msg := range batch {
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &msg.Topic, Partition: kafka.PartitionAny},
			Key:            msg.Key,
			Value:          msg.Value,
		}, deliveryChan)

		if err != nil {
			log.Printf("發送到Kafka失敗: %v\n", err)
		}
	}
}

func handleDeliveryReports(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("傳遞失敗: %v\n", ev.TopicPartition.Error)
			}
		}
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
