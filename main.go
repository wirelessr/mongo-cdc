package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
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
	// 設置 GOMAXPROCS 為 CPU 核心數
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Kafka 生產者優化配置
	kafkaServer := getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":               kafkaServer,
		"acks":                           "1",                // 降低等待確認的時間
		"retries":                        5,
		"linger.ms":                      10,                // 小批次延遲
		"batch.size":                     1048576,           // 1MB 批次大小
		"compression.type":               "snappy",          // 使用 snappy 壓縮
		"queue.buffering.max.messages":   100000,           // 增加緩衝區
		"queue.buffering.max.ms":         500,
	})
	if err != nil {
		log.Fatalf("創建Kafka生產者失敗: %v", err)
	}
	defer producer.Close()

	// 使用 channel 處理 delivery reports
	deliveryChan := make(chan kafka.Event, 10000)
	go func() {
		for e := range deliveryChan {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("傳遞失敗: %v\n", ev.TopicPartition.Error)
				}
			}
		}
	}()

	// MongoDB 連接優化
	ctx := context.Background()
	mongoURI := getEnv("MONGO_URI", "mongodb://localhost:27017")
	clientOptions := options.Client().
		ApplyURI(mongoURI).
		SetMaxPoolSize(100).                     // 增加連接池
		SetMaxConnIdleTime(300).                 // 增加空閒連接時間
		SetCompressors([]string{"snappy"}).      // 使用 snappy 壓縮
		SetReadPreference(readpref.Secondary())  // 如果是副本集，從從節點讀取

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	collection := client.Database(getEnv("MONGO_DB", "test")).
		Collection(getEnv("MONGO_COLLECTION", "test_new"))

	// Change Stream 優化
	pipeline := mongo.Pipeline{}
	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetBatchSize(1000)  // 增加批次大小

	stream, err := collection.Watch(ctx, pipeline, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close(ctx)

	fmt.Println("開始監聽MongoDB變更...")

	// 主處理循環
	var changeEvent bson.M
	for stream.Next(ctx) {
		if err := stream.Decode(&changeEvent); err != nil {
			log.Printf("解碼錯誤: %v\n", err)
			continue
		}

		// 獲取數據庫和集合名稱
		ns := changeEvent["ns"].(bson.M)
		topic := fmt.Sprintf("%s.%s", ns["db"], ns["coll"])

		// 準備 key (使用 sync.Pool 優化記憶體分配)
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

		// 序列化變更事件
		value, err := bson.MarshalExtJSON(changeEvent, true, true)
		if err != nil {
			log.Printf("Value序列化錯誤: %v\n", err)
			continue
		}

		// 發送到 Kafka
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   keyBytes,
			Value: value,
		}, deliveryChan)

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

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
