package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// read_v4.go - İYİLEŞTİRME 4: Paralel Aggregation Pipeline
// Bu versiyon, aggregation pipeline'ı paralel olarak çalıştırır
// Her worker ayrı bir aggregation pipeline çalıştırır
//
// Avantajları:
// 1. Aggregation pipeline kullanımı (MongoDB tarafında işleme)
// 2. $match stage'i index kullanabilir
// 3. Paralel işleme sayesinde throughput artar
// 4. CPU ve network'ü daha iyi kullanır
//
// Dikkat:
// - MongoDB connection pool size'ı yeterli olmalı
// - Çok fazla goroutine memory kullanımını artırabilir
func main() {
	// Logger oluştur
	logger, err := NewLogger("read_v4_results.txt")
	if err != nil {
		fmt.Printf("Logger oluşturulamadı: %v\n", err)
		return
	}
	defer logger.Close()
	
	logger.WriteHeader("read_v4 - İYİLEŞTİRME 4 (Parallel Reading)")
	
	col := GetMongo()
	ctx := context.Background()

	// Aggregation pipeline oluştur
	// Pipeline stage'leri sırayla çalışır:
	// 1. $match: Filtreleme - index kullanabilir
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

	// Önce eşleşen kayıt sayısını bul
	totalCount, err := col.CountDocuments(ctx, bson.M{"status": "PAID"})
	if err != nil {
		panic(err)
	}
	logger.Printf("📊 Eşleşen kayıt sayısı (status='PAID'): %d\n", totalCount)

	// Paralel okuma için ayarlar
	numWorkers := 10        // Kaç goroutine paralel çalışacak
	chunkSize := int64(100000) // Her chunk'ta kaç kayıt olacak

	// Explain çalıştır - Aggregation pipeline için
	logger.Println("🔍 Aggregation pipeline analizi yapılıyor...")
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
		PrintExplainResults(explainResult, "read_v4 (Parallel Aggregation)", logger)
	}

	// Performans ölçümü başlat
	start := time.Now()
	
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	// Paralel okuma için channel ve wait group
	var wg sync.WaitGroup
	var totalRead int64 // Atomic counter for thread-safe counting

	// Her worker için goroutine başlat
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Bu worker'ın okuyacağı chunk'ı hesapla
			skip := int64(workerID) * chunkSize
			
			// Eğer skip, toplam kayıt sayısından büyükse, bu worker'a iş yok
			if skip >= totalCount {
				return
			}

			// Bu chunk için aggregation pipeline oluştur
			// $match: Filtreleme (index kullanabilir)
			// $skip: skip kadar kayıt atla
			// $limit: chunkSize kadar kayıt getir
			// $project: Sadece gerekli alanları getir
			chunkPipeline := []bson.M{
				{
					"$match": bson.M{
						"status": "PAID", // Filtreleme - index kullanılabilir
					},
				},
				{
					"$skip": skip, // skip kadar kayıt atla
				},
				{
					"$limit": chunkSize, // chunkSize kadar kayıt getir
				},
				{
					"$project": bson.M{
						"userId": 1,
						"status": 1,
						"_id":    0,
					},
				},
			}

			// Aggregation pipeline'ı çalıştır
			cursor, err := col.Aggregate(ctx, chunkPipeline, options.Aggregate().SetBatchSize(1000))
			if err != nil {
				logger.Printf("⚠️  Worker %d hatası: %v\n", workerID, err)
				return
			}
			defer cursor.Close(ctx)

			// Bu chunk'ı oku
			localCount := 0
			for cursor.Next(ctx) {
				var result bson.M
				if err := cursor.Decode(&result); err != nil {
					logger.Printf("⚠️  Worker %d decode hatası: %v\n", workerID, err)
					continue
				}
				
				_ = result
				localCount++
			}

			if err := cursor.Err(); err != nil {
				logger.Printf("⚠️  Worker %d cursor hatası: %v\n", workerID, err)
			}

			// Toplam sayacı güncelle (thread-safe)
			atomic.AddInt64(&totalRead, int64(localCount))
			
			logger.Printf("  ✅ Worker %d tamamlandı: %d kayıt okundu\n", workerID, localCount)
		}(i)
	}

	// Tüm worker'ların bitmesini bekle
	wg.Wait()

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	memoryUsed := int64(memAfter.Alloc - memBefore.Alloc)

	duration := time.Since(start)

	logger.Printf("\n✅ İYİLEŞTİRME 4 SONUÇLARI (Parallel Aggregation):\n")
	logger.Printf("📦 Okunan Kayıt: %d\n", totalRead)
	logger.Printf("⏱️  Süre: %v\n", duration)
	logger.Printf("💾 Bellek Kullanımı: %.2f MB\n", float64(memoryUsed)/(1024*1024))
	logger.Printf("🚀 Paralel aggregation pipeline sayesinde daha hızlı!\n")
	logger.Printf("👥 Worker sayısı: %d\n", numWorkers)
	logger.Printf("📊 Her worker ayrı aggregation pipeline çalıştırdı ($match + $project)\n")
	
	if explainResult != nil {
		if execStats, ok := explainResult["executionStats"].(map[string]interface{}); ok {
			metrics := QueryMetrics{
				Duration:    duration,
				RecordsRead: int(totalRead),
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
			
			PrintMetrics(metrics, "read_v4", logger)
		}
	}
	
	logger.Println("\n✅ Test tamamlandı! Sonuçlar 'read_v4_results.txt' dosyasına kaydedildi.")
}

