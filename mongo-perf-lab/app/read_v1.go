package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// read_v1.go - İYİLEŞTİRME 1: Cursor streaming ile okuma
// Bu versiyon, cursor.All() yerine cursor.Next() kullanır
// Bu sayede kayıtlar tek tek işlenir, tümü memory'ye yüklenmez
// Avantajları:
// 1. Daha az bellek kullanımı (streaming)
// 2. Daha hızlı başlangıç (ilk kayıtlar hemen gelir)
// 3. Büyük veri setleri için daha uygun
func main() {

	logger, err := NewLogger("read_v1_results.txt")
	if err != nil {
		fmt.Printf("Logger oluşturulamadı: %v\n", err)
		return
	}
	defer logger.Close()
	
	logger.WriteHeader("read_v1 - İYİLEŞTİRME 1 (Cursor Streaming)")
	
	col := GetMongo()
	ctx := context.Background()

	// Explain çalıştır - Sorgunun nasıl çalışacağını analiz et
	// Filtre yok - TÜM kayıtları okuyacağız
	logger.Println("🔍 Sorgu analizi yapılıyor (explain)...")
	explainResult, err := ExplainQuery(col, bson.M{}) // Boş filter = tüm kayıtlar
	if err != nil {
		logger.Printf("⚠️  Explain hatası: %v\n", err)
	} else {
		PrintExplainResults(explainResult, "read_v1 (Cursor Streaming)", logger)
	}

	// Performans ölçümü başlat
	start := time.Now()
	
	// Bellek kullanımını ölçmek için başlangıç durumunu al
	var memBefore runtime.MemStats
	runtime.GC() // Garbage collection yap ki ölçüm doğru olsun
	runtime.ReadMemStats(&memBefore)

	// Sorguyu çalıştır
	// Find: TÜM kayıtları bul (filtre yok)
	cursor, err := col.Find(ctx, bson.M{}) // Boş filter = tüm kayıtlar
	if err != nil {
		panic(err)
	}
	defer cursor.Close(ctx) // Cursor'ı kapatmayı unutma (memory leak önleme)

	// İYİLEŞTİRME: cursor.Next() kullan - Streaming okuma
	// cursor.All() yerine cursor.Next() kullanarak kayıtları tek tek işle
	// Bu sayede:
	// - Tüm kayıtlar memory'de beklemek zorunda değil
	// - İlk kayıtlar hemen işlenebilir
	// - Bellek kullanımı çok daha düşük
	recordCount := 0
	for cursor.Next(ctx) {
		var result interface{}
		if err := cursor.Decode(&result); err != nil {
			panic(err)
		}
		
		// Burada kayıt işlenebilir (örneğin: hesaplama, yazdırma, başka DB'ye kaydetme vb.)
		// Şu an sadece sayıyoruz, ama gerçek uygulamada burada işlem yapılır
		recordCount++
		
		// Her 100k kayıtta bir ilerleme göster (opsiyonel)
		if recordCount%100000 == 0 {
			logger.Printf("  📊 İşlenen kayıt: %d\n", recordCount)
		}
	}

	// Cursor'dan hata var mı kontrol et
	if err := cursor.Err(); err != nil {
		panic(err)
	}

	// Bellek kullanımını ölçmek için bitiş durumunu al
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	memoryUsed := int64(memAfter.Alloc - memBefore.Alloc)

	duration := time.Since(start)

	// Sonuçları göster
	logger.Printf("\n✅ İYİLEŞTİRME 1 SONUÇLARI (Cursor Streaming):\n")
	logger.Printf("📦 Okunan Kayıt: %d\n", recordCount)
	logger.Printf("⏱️  Süre: %v\n", duration)
	logger.Printf("💾 Bellek Kullanımı: %.2f MB\n", float64(memoryUsed)/(1024*1024))
	
	// Execution stats'i parse et ve göster
	if explainResult != nil {
		if execStats, ok := explainResult["executionStats"].(map[string]interface{}); ok {
			metrics := QueryMetrics{
				Duration:    duration,
				RecordsRead: recordCount,
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
			
			PrintMetrics(metrics, "read_v1", logger)
		}
	}
	
	logger.Println("\n✅ Test tamamlandı! Sonuçlar 'read_v1_results.txt' dosyasına kaydedildi.")
}

