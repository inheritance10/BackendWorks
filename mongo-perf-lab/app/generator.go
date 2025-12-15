package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// generator.go - Test verisi oluşturma scripti
// Bu script, performans testleri için 1 milyon test kaydı oluşturur
// 
// Kullanım:
//   go run generator.go
//
// Not: Bu işlem birkaç dakika sürebilir (1 milyon kayıt)
func main() {
	col := GetMongo()
	ctx := context.Background()

	// Batch size: Her seferde kaç kayıt insert edilecek
	// Büyük batch size daha hızlı ama daha fazla bellek kullanır
	batchSize := 1000
	
	// Toplam kayıt sayısı
	total := 1_000_000

	fmt.Printf("🚀 %d kayıt oluşturuluyor...\n", total)
	fmt.Printf("📦 Batch size: %d\n", batchSize)
	
	start := time.Now()

	// Random seed ayarla (her çalıştırmada farklı veri için)
	rand.Seed(time.Now().UnixNano())

	// Batch'ler halinde kayıt oluştur
	// Tüm kayıtları bir kerede insert etmek yerine batch'ler halinde insert et
	// Bu sayede:
	// 1. Daha az bellek kullanımı
	// 2. İlerleme takibi yapılabilir
	// 3. Hata durumunda daha kolay recovery
	for i := 0; i < total; i += batchSize {
		var docs []interface{}

		// Bu batch için kayıtları oluştur
		for j := 0; j < batchSize && (i+j) < total; j++ {
			// Rastgele bir order dokümanı oluştur
			docs = append(docs, bson.M{
				"userId": primitive.NewObjectID(), // Rastgele user ID
				"status": []string{"PAID", "CANCELLED", "PENDING"}[rand.Intn(3)], // Rastgele status
				"total":  rand.Intn(5000), // Rastgele toplam tutar (0-5000 arası)
				"items": []bson.M{
					{
						"productId": primitive.NewObjectID(), // Rastgele ürün ID
						"price":     rand.Intn(1000),         // Rastgele fiyat (0-1000 arası)
						"qty":       rand.Intn(5) + 1,        // Rastgele miktar (1-5 arası)
					},
				},
				// Rastgele bir tarih oluştur (son 1000 saat içinden)
				"createdAt": time.Now().Add(-time.Duration(rand.Intn(1000)) * time.Hour),
			})
		}

		// Bu batch'i MongoDB'ye insert et
		// InsertMany, batch insert için optimize edilmiştir
		_, err := col.InsertMany(ctx, docs)
		if err != nil {
			panic(err)
		}

		// Her 100k kayıtta bir ilerleme göster
		if i%100_000 == 0 && i > 0 {
			elapsed := time.Since(start)
			rate := float64(i) / elapsed.Seconds()
			remaining := total - i
			eta := time.Duration(float64(remaining)/rate) * time.Second
			fmt.Printf("  ✅ İlerleme: %d/%d kayıt (%.1f kayıt/sn, Kalan: ~%v)\n", 
				i, total, rate, eta)
		}
	}

	duration := time.Since(start)
	rate := float64(total) / duration.Seconds()

	fmt.Printf("\n✅ TAMAMLANDI!\n")
	fmt.Printf("⏱️  Toplam Süre: %v\n", duration)
	fmt.Printf("📊 Hız: %.1f kayıt/saniye\n", rate)
	fmt.Printf("📦 Toplam Kayıt: %d\n", total)
	
	// Collection'daki toplam kayıt sayısını kontrol et
	count, err := col.CountDocuments(ctx, bson.M{})
	if err != nil {
		fmt.Printf("⚠️  Kayıt sayısı kontrol edilemedi: %v\n", err)
	} else {
		fmt.Printf("📋 Collection'daki toplam kayıt: %d\n", count)
	}
	
	// Status dağılımını göster
	fmt.Println("\n📊 Status Dağılımı:")
	statuses := []string{"PAID", "CANCELLED", "PENDING"}
	for _, status := range statuses {
		count, _ := col.CountDocuments(ctx, bson.M{"status": status})
		percentage := float64(count) / float64(total) * 100
		fmt.Printf("  %s: %d (%.1f%%)\n", status, count, percentage)
	}
}
