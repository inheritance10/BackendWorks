package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// read_v5.go - İYİLEŞTİRME 5: Aggregation Pipeline Optimizasyonu
// Bu versiyon, MongoDB aggregation pipeline kullanır
// Aggregation pipeline, MongoDB'de veri işleme için en güçlü yöntemdir
//
// Avantajları:
// 1. Veri işleme MongoDB tarafında yapılır (network trafiği azalır)
// 2. Pipeline stage'leri optimize edilebilir
// 3. $match stage'i index kullanabilir
// 4. $project stage'i sadece gerekli alanları getirir
// 5. MongoDB'nin built-in optimizasyonlarından faydalanır
func main() {
	// Logger oluştur
	logger, err := NewLogger("read_v5_results.txt")
	if err != nil {
		fmt.Printf("Logger oluşturulamadı: %v\n", err)
		return
	}
	defer logger.Close()
	
	logger.WriteHeader("read_v5 - İYİLEŞTİRME 5 (Aggregation Pipeline)")
	
	col := GetMongo()
	ctx := context.Background()

	// Aggregation pipeline oluştur
	// Pipeline stage'leri sırayla çalışır:
	// 1. $match: Filtreleme - index kullanabilir (status="PAID" için index var)
	// 2. $project: Sadece gerekli alanları getir
	// 
	// Aggregation pipeline'ın avantajları:
	// - $match stage'i index kullanabilir (IXSCAN) - çok hızlı!
	// - $project stage'i sadece gerekli alanları getirir - network trafiği azalır
	// - Veri işleme MongoDB tarafında yapılır - Go tarafında daha az işlem
	pipeline := []bson.M{
		{
			"$match": bson.M{
				"status": "PAID", // Filtreleme - index kullanılabilir
			},
		},
		{
			"$project": bson.M{
				"userId": 1,  // Sadece bu alanları getir
				"status": 1,
				"_id":    0,  // _id'yi getirme
			},
		},
	}

	// Explain için aggregation explain komutu
	// $match stage'i index kullanabilir, bu çok önemli!
	logger.Println("🔍 Aggregation pipeline analizi yapılıyor (explain with $match)...")
	
	// Aggregation explain komutu
	var explainResult map[string]interface{}
	// err zaten tanımlı (logger oluştururken), bu yüzden := yerine = kullanıyoruz
	err = col.Database().RunCommand(ctx, bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "aggregate", Value: col.Name()},
			{Key: "pipeline", Value: pipeline},
			{Key: "cursor", Value: bson.M{"batchSize": 1000}},
		}},
		{Key: "verbosity", Value: "executionStats"},
	}).Decode(&explainResult)
	
	if err != nil {
		logger.Printf("⚠️  Explain hatası: %v\n", err)
	} else {
		PrintExplainResults(explainResult, "read_v5 (Aggregation Pipeline)", logger)
	}

	// Performans ölçümü başlat
	start := time.Now()
	
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	// Aggregation pipeline'ı çalıştır
	// Aggregation, MongoDB'de veri işleme için en güçlü yöntemdir
	// Veri işleme MongoDB tarafında yapılır, sadece sonuçlar gelir
	cursor, err := col.Aggregate(ctx, pipeline, options.Aggregate().SetBatchSize(1000))
	if err != nil {
		panic(err)
	}
	defer cursor.Close(ctx)

	// Sonuçları oku
	recordCount := 0
	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			panic(err)
		}
		
		// Burada sadece işlenmiş veri var (MongoDB tarafında işlendi)
		_ = result
		recordCount++
		
		if recordCount%100000 == 0 {
			logger.Printf("  📊 İşlenen kayıt: %d\n", recordCount)
		}
	}

	if err := cursor.Err(); err != nil {
		panic(err)
	}

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	memoryUsed := int64(memAfter.Alloc - memBefore.Alloc)

	duration := time.Since(start)

	logger.Printf("\n✅ İYİLEŞTİRME 5 SONUÇLARI (Aggregation Pipeline):\n")
	logger.Printf("📦 Okunan Kayıt: %d\n", recordCount)
	logger.Printf("⏱️  Süre: %v\n", duration)
	logger.Printf("💾 Bellek Kullanımı: %.2f MB\n", float64(memoryUsed)/(1024*1024))
	logger.Printf("🚀 Aggregation pipeline sayesinde MongoDB tarafında işleme yapıldı!\n")
	
	if explainResult != nil {
		// Aggregation explain sonuçları biraz farklı yapıda olabilir
		if stages, ok := explainResult["stages"].([]interface{}); ok {
			logger.Println("\n📋 Pipeline Stage'leri:")
			for i, stage := range stages {
				if stageMap, ok := stage.(map[string]interface{}); ok {
					if stageName, ok := stageMap["stage"].(string); ok {
						logger.Printf("  Stage %d: %s\n", i+1, stageName)
					}
				}
			}
		}
		
		// Execution stats varsa göster
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
			
			PrintMetrics(metrics, "read_v5", logger)
		}
	}
	
	logger.Println("\n✅ Test tamamlandı! Sonuçlar 'read_v5_results.txt' dosyasına kaydedildi.")
}

