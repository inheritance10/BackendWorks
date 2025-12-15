package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// create_index.go - MongoDB'de index oluşturma scripti
// Bu script, performans testleri için gerekli index'leri oluşturur
//
// KULLANIM:
//   go run main.go create_index.go
//
// Index'ler neden önemli?
// - Index olmadan MongoDB tüm collection'ı tarar (COLLSCAN) - ÇOK YAVAŞ!
// - Index ile MongoDB sadece ilgili kayıtları bulur (IXSCAN) - HIZLI!
// - 1 milyon kayıt için index olmadan sorgu çok uzun sürer
func main() {
	col := GetMongo()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("🔧 Index oluşturuluyor...")

	// Index modeli oluştur
	// status alanına göre index oluştur
	// Bu, status="PAID" sorgularını çok hızlandırır
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "status", Value: 1}, // 1 = ascending (artan sırada)
		},
		Options: options.Index().
			SetName("status_1"). // Index adı
			SetBackground(true), // Background'da oluştur (non-blocking)
	}

	// Index oluştur
	indexName, err := col.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		// Index zaten varsa hata verme, sadece bilgi ver
		if mongo.IsDuplicateKeyError(err) {
			fmt.Println("ℹ️  Index zaten mevcut:", indexName)
		} else {
			panic(err)
		}
	} else {
		fmt.Println("✅ Index oluşturuldu:", indexName)
	}

	// Index'lerin listesini göster
	fmt.Println("\n📋 Mevcut index'ler:")
	cursor, err := col.Indexes().List(ctx)
	if err != nil {
		panic(err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var index bson.M
		if err := cursor.Decode(&index); err != nil {
			continue
		}
		if name, ok := index["name"].(string); ok {
			fmt.Printf("  - %s\n", name)
		}
	}

	fmt.Println("\n✅ Index oluşturma tamamlandı!")
	fmt.Println("💡 Not: Tüm kayıtları okurken index kullanılmaz (COLLSCAN normaldir)")
	fmt.Println("💡 Index'ler filtreli sorgular için faydalıdır (örn: status='PAID')")
}

