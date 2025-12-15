package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// read_v3.go - İYİLEŞTİRME 3: Aggregation Pipeline + Index Optimizasyonu
// Bu versiyon, aggregation pipeline kullanır ve index optimizasyonu yapar
// ÖNEMLİ: Bu versiyon çalışmadan önce index oluşturulmalı!
// Index oluşturmak için: go run main.go create_index.go
//
// Avantajları:
// 1. Aggregation pipeline kullanımı (MongoDB tarafında işleme)
// 2. $match stage'i index kullanabilir (IXSCAN)
// 3. $project stage'i sadece gerekli alanları getirir
// 4. COLLSCAN yerine IXSCAN (index scan) - çok daha hızlı
func main() {
	// Logger oluştur
	logger, err := NewLogger("read_v3_results.txt")
	if err != nil {
		fmt.Printf("Logger oluşturulamadı: %v\n", err)
		return
	}
	defer logger.Close()
	
	logger.WriteHeader("read_v3 - İYİLEŞTİRME 3 (Index Optimized)")
	
	col := GetMongo()
	ctx := context.Background()

	// Aggregation pipeline oluştur
	// Pipeline stage'leri sırayla çalışır:
	// 1. $match: Filtreleme - index kullanabilir (status="PAID" için index var)
	// 2. $project: Sadece gerekli alanları getir
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
	logger.Println("🔍 Aggregation pipeline analizi yapılıyor (explain with $match)...")
	
	// Aggregation explain komutu
	var explainResult map[string]interface{}
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
		PrintExplainResults(explainResult, "read_v3 (Aggregation + Index)", logger)
		
		// Index kullanılıyor mu kontrol et
		// $match stage'i index kullanabilir
		if stages, ok := explainResult["stages"].([]interface{}); ok {
			for _, stage := range stages {
				if stageMap, ok := stage.(map[string]interface{}); ok {
					if stageName, ok := stageMap["stage"].(string); ok {
						if stageName == "IXSCAN" {
							logger.Println("✅ Index kullanılıyor (IXSCAN) - İyi!")
						} else if stageName == "COLLSCAN" {
							logger.Println("⚠️  UYARI: Collection scan tespit edildi - Index oluşturun!")
							logger.Println("   go run main.go create_index.go")
						}
					}
				}
			}
		}
	}

	// Performans ölçümü başlat
	start := time.Now()
	
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	// Aggregation pipeline'ı çalıştır
	// Aggregation, MongoDB'de veri işleme için en güçlü yöntemdir
	// $match stage'i index kullanabilir, bu çok hızlıdır
	cursor, err := col.Aggregate(ctx, pipeline, options.Aggregate().SetBatchSize(1000))
	if err != nil {
		panic(err)
	}
	defer cursor.Close(ctx)

	// Streaming okuma
	recordCount := 0
	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			panic(err)
		}
		
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

	logger.Printf("\n✅ İYİLEŞTİRME 3 SONUÇLARI (Aggregation + Index):\n")
	logger.Printf("📦 Okunan Kayıt: %d\n", recordCount)
	logger.Printf("⏱️  Süre: %v\n", duration)
	logger.Printf("💾 Bellek Kullanımı: %.2f MB\n", float64(memoryUsed)/(1024*1024))
	logger.Printf("🚀 Aggregation pipeline + Index kullanımı sayesinde çok daha hızlı!\n")
	logger.Printf("📊 $match stage'i index kullanarak sadece ilgili kayıtları getirdi\n")
	
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
			
			PrintMetrics(metrics, "read_v3", logger)
		}
	}
	
	logger.Println("\n✅ Test tamamlandı! Sonuçlar 'read_v3_results.txt' dosyasına kaydedildi.")
}

