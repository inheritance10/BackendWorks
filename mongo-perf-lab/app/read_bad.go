package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// read_bad.go - KÖTÜ YÖNTEM: Tüm sonuçları memory'ye yükleme
// Bu versiyon, tüm sonuçları bir kerede memory'ye yükler (cursor.All)
// 1 milyon kayıt için çok fazla bellek kullanır ve yavaştır
func main() {

	logger, err := NewLogger("read_bad_results.txt")
	if err != nil {
		fmt.Printf("Logger oluşturulamadı: %v\n", err)
		return
	}
	defer logger.Close()
	

	logger.WriteHeader("read_bad - KÖTÜ YÖNTEM (Baseline)")
	
	col := GetMongo()
	ctx := context.Background()

	// Explain çalıştırıp sorgu analizini çıkartıp iyileştirmelerimizi ona göre düzenleyeceğiz
	// Filtre yok - TÜM kayıtları okuma işlemi yapacağız
	logger.Println("🔍 Sorgu analizi yapılıyor (explain)...")
	explainResult, err := ExplainQuery(col, bson.M{}) // Boş filter = tüm kayıtlar
	if err != nil {
		logger.Printf("  Explain hatası: %v\n", err)
	} else {
		PrintExplainResults(explainResult, "read_bad (KÖTÜ YÖNTEM)", logger)
	}

	// Performans ölçümü başlat
	start := time.Now()
	
	// Bellek kullanımını ölçmek için başlangıç durumunu al
	var memBefore runtime.MemStats
	runtime.GC() // Garbage collection yap ki ölçüm doğru olsun 
	// (erişilmeyen, kullanılmayan nesneleri değişkenleri bellekten sileriz bu şekilde memory leak önune geçmiş oluruz)
	runtime.ReadMemStats(&memBefore)


	// Find: TÜM kayıtları bul (filtre yok)
	cursor, err := col.Find(ctx, bson.M{}) // Boş filter = tüm kayıtlar
	if err != nil {
		panic(err)
	}

	//  KÖTÜ YÖNTEM: cursor.All() - Tüm sonuçları bir kerede memory'ye yükle
	// Bu, 1 milyon kayıt için çok fazla bellek kullanır
	// Tüm kayıtlar memory'de bekler, bu da:
	// 1. Yüksek bellek kullanımı
	// 2. Yavaş başlangıç (tüm veri gelene kadar bekler)
	// 3. Network buffer overflow riski
	var results []interface{}
	if err := cursor.All(ctx, &results); err != nil {
		panic(err)
	}

	// Bellek kullanımını ölçmek için bitiş durumunu al
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	memoryUsed := int64(memAfter.Alloc - memBefore.Alloc)

	duration := time.Since(start)

	// Sonuçları göster
	logger.Printf("\n❌ KÖTÜ YÖNTEM SONUÇLARI:\n")
	logger.Printf("📦 Okunan Kayıt: %d\n", len(results))
	logger.Printf("⏱️  Süre: %v\n", duration)
	logger.Printf("💾 Bellek Kullanımı: %.2f MB\n", float64(memoryUsed)/(1024*1024))
	
	// Execution stats'i parse et ve göster
	if explainResult != nil {
		if execStats, ok := explainResult["executionStats"].(map[string]interface{}); ok {
			metrics := QueryMetrics{
				Duration:    duration,
				RecordsRead: len(results),
				MemoryUsed:  memoryUsed,
			}
			
			// Execution stats'i parse et
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
			
			PrintMetrics(metrics, "read_bad", logger)
		}
	}
	
	logger.Println("\n✅ Test tamamlandı! Sonuçlar 'read_bad_results.txt' dosyasına kaydedildi.")
}
