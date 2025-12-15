package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// read_v2.go - İYİLEŞTİRME 2: Projection + Batch Size Optimizasyonu
// Bu versiyon, sadece ihtiyaç duyulan alanları getirir (projection)
// ve batch size'ı optimize eder
// Avantajları:
// 1. Daha az network trafiği (sadece gerekli alanlar)
// 2. Daha az bellek kullanımı (küçük dokümanlar)
// 3. Daha hızlı deserialization (daha az alan parse edilir)
func main() {
	// Logger oluştur
	logger, err := NewLogger("read_v2_results.txt")
	if err != nil {
		fmt.Printf("Logger oluşturulamadı: %v\n", err)
		return
	}
	defer logger.Close()
	
	logger.WriteHeader("read_v2 - İYİLEŞTİRME 2 (Projection + Batch)")
	
	col := GetMongo()
	ctx := context.Background()

	// Projection: Sadece ihtiyaç duyulan alanları getir
	// Bu örnekte sadece userId ve status alanlarını getiriyoruz
	// items, createdAt gibi alanlar getirilmez (network ve bellek tasarrufu)
	projection := bson.M{
		"userId": 1,  // userId alanını getir
		"status": 1,  // status alanını getir
		"_id":    0,  // _id alanını getirme (opsiyonel, 0 = getirme)
	}

	// Batch Size: Her seferde kaç kayıt getirileceğini belirle
	// MongoDB default: 101 kayıt
	// Büyük veri setleri için daha büyük batch size daha verimli olabilir
	// Ancak çok büyük batch size memory kullanımını artırabilir
	batchSize := int32(1000) // Her seferde 1000 kayıt getir

	// Explain çalıştır - Projection ile birlikte
	// Filtre yok - TÜM kayıtları okuyacağız
	logger.Println("🔍 Sorgu analizi yapılıyor (explain with projection)...")
	findOpts := options.Find().SetProjection(projection).SetBatchSize(batchSize)
	explainResult, err := ExplainQuery(col, bson.M{}, findOpts) // Boş filter = tüm kayıtlar
	if err != nil {
		logger.Printf("⚠️  Explain hatası: %v\n", err)
	} else {
		PrintExplainResults(explainResult, "read_v2 (Projection + Batch)", logger)
	}

	// Performans ölçümü başlat
	start := time.Now()
	
	// Bellek kullanımını ölçmek için başlangıç durumunu al
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	// Sorguyu çalıştır - Projection ve batch size ile
	// TÜM kayıtları oku (filtre yok)
	cursor, err := col.Find(ctx, bson.M{}, findOpts) // Boş filter = tüm kayıtlar
	if err != nil {
		panic(err)
	}
	defer cursor.Close(ctx)

	// Streaming okuma (v1'deki gibi)
	recordCount := 0
	for cursor.Next(ctx) {
		// Projection sayesinde sadece userId ve status alanları var
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			panic(err)
		}
		
		// Burada sadece gerekli alanlar var, bu yüzden işlem daha hızlı
		// Örnek: result["userId"] ve result["status"] kullanılabilir
		_ = result // Şu an kullanmıyoruz, sadece decode ediyoruz
		
		recordCount++
		
		if recordCount%100000 == 0 {
			logger.Printf("  📊 İşlenen kayıt: %d\n", recordCount)
		}
	}

	if err := cursor.Err(); err != nil {
		panic(err)
	}

	// Bellek kullanımını ölç
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	memoryUsed := int64(memAfter.Alloc - memBefore.Alloc)

	duration := time.Since(start)

	// Sonuçları göster
	logger.Printf("\n✅ İYİLEŞTİRME 2 SONUÇLARI (Projection + Batch):\n")
	logger.Printf("📦 Okunan Kayıt: %d\n", recordCount)
	logger.Printf("⏱️  Süre: %v\n", duration)
	logger.Printf("💾 Bellek Kullanımı: %.2f MB\n", float64(memoryUsed)/(1024*1024))
	logger.Printf("📉 Projection sayesinde daha az veri transfer edildi!\n")
	
	// Execution stats'i parse et ve göster
	if explainResult != nil {
		if execStats, ok := explainResult["executionStats"].(map[string]interface{}); ok {
			metrics := QueryMetrics{
				Duration:    duration,
				RecordsRead: recordCount,
				MemoryUsed:  memoryUsed,
			}
			
			if execTime, ok := execStats["executionTimeMillis"].(int64); ok {
				metrics.ExecutionStats = &ExecutionStats{
					ExecutionTimeMillis: execTime,
				}
			}
			if totalDocs, ok := execStats["totalDocsExamined"].(int64); ok {
				if metrics.ExecutionStats == nil {
					metrics.ExecutionStats = &ExecutionStats{}
				}
				metrics.ExecutionStats.TotalDocsExamined = totalDocs
			}
			if totalKeys, ok := execStats["totalKeysExamined"].(int64); ok {
				if metrics.ExecutionStats == nil {
					metrics.ExecutionStats = &ExecutionStats{}
				}
				metrics.ExecutionStats.TotalKeysExamined = totalKeys
			}
			if nReturned, ok := execStats["nReturned"].(int64); ok {
				if metrics.ExecutionStats == nil {
					metrics.ExecutionStats = &ExecutionStats{}
				}
				metrics.ExecutionStats.NReturned = nReturned
			}
			
			PrintMetrics(metrics, "read_v2", logger)
		}
	}
	
	logger.Println("\n✅ Test tamamlandı! Sonuçlar 'read_v2_results.txt' dosyasına kaydedildi.")
}

