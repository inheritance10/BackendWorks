package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// QueryMetrics - Sorgu performans metriklerini tutan yapı
// Bu yapı, bir MongoDB sorgusunun ne kadar sürede çalıştığını,
// kaç kayıt okunduğunu, ne kadar bellek kullanıldığını ve
// MongoDB'nin kendi execution stats'ını saklar
type QueryMetrics struct {
	Duration       time.Duration // Toplam sorgu süresi (Go tarafında ölçülen)
	RecordsRead    int           // Okunan toplam kayıt sayısı
	MemoryUsed     int64         // Kullanılan bellek miktarı (bytes)
	ExecutionStats *ExecutionStats // MongoDB'nin kendi execution istatistikleri
	QueryPlan      *QueryPlan     // MongoDB query plan bilgisi
}

// ExecutionStats - MongoDB explain komutundan gelen execution istatistikleri
// Bu veriler MongoDB'nin sorguyu nasıl çalıştırdığını gösterir:
// - Kaç doküman incelendi (totalDocsExamined)
// - Kaç index key'i incelendi (totalKeysExamined)
// - Kaç doküman döndürüldü (nReturned)
// - Sorgu ne kadar sürede çalıştı (executionTimeMillis)
type ExecutionStats struct {
	ExecutionTimeMillis int64       `json:"executionTimeMillis"` // MongoDB tarafında sorgu çalışma süresi (ms)
	TotalDocsExamined   int64       `json:"totalDocsExamined"`   // İncelenen toplam doküman sayısı
	TotalKeysExamined   int64       `json:"totalKeysExamined"`   // İncelenen toplam index key sayısı
	NReturned           int64       `json:"nReturned"`           // Döndürülen doküman sayısı
	ExecutionStages     interface{} `json:"executionStages"`      // Execution stage detayları (nested yapı)
}

// QueryPlan - MongoDB'nin sorgu planı bilgisi
// winningPlan: MongoDB'nin seçtiği en iyi execution plan
// rejectedPlans: MongoDB'nin değerlendirip reddettiği alternatif planlar
type QueryPlan struct {
	WinningPlan  interface{} `json:"winningPlan"`  // Seçilen en iyi plan
	RejectedPlans interface{} `json:"rejectedPlans"` // Reddedilen alternatif planlar
}

// ExplainQuery - MongoDB sorgusuna explain komutu çalıştırır ve sonucu döndürür
// Bu fonksiyon, bir sorgunun nasıl çalıştığını analiz etmek için MongoDB'nin
// explain özelliğini kullanır. Sorgunun hangi index'leri kullandığını,
// kaç doküman incelediğini ve ne kadar sürede çalıştığını gösterir.
//
// Parametreler:
//   - col: MongoDB collection referansı
//   - filter: Sorgu filtresi (bson.M formatında)
//   - opts: Opsiyonel find options (projection, limit, skip vb.)
//
// Döndürür:
//   - map[string]interface{}: Explain sonuçları (executionStats, queryPlanner vb.)
//   - error: Hata varsa
func ExplainQuery(col *mongo.Collection, filter bson.M, opts ...*options.FindOptions) (map[string]interface{}, error) {
	ctx := context.Background()
	
	// MongoDB explain komutu için find komutunu oluştur
	// Bu, gerçek sorguyu explain etmek için kullanılacak
	explainCmd := bson.D{
		{Key: "find", Value: col.Name()},    // Hangi collection'da arama yapılacak
		{Key: "filter", Value: filter},      // Sorgu filtresi
	}
	
	// Eğer find options verilmişse (projection, limit, skip vb.), bunları da ekle
	// Bu sayede gerçek sorguyla aynı şekilde explain yapılır
	if len(opts) > 0 && opts[0] != nil {
		if opts[0].Projection != nil {
			// Projection: Sadece belirli alanları getir (tüm dokümanı değil)
			// Bu bellek kullanımını azaltır
			explainCmd = append(explainCmd, bson.E{Key: "projection", Value: opts[0].Projection})
		}
		if opts[0].Limit != nil {
			// Limit: Maksimum kaç kayıt döndürülecek
			explainCmd = append(explainCmd, bson.E{Key: "limit", Value: *opts[0].Limit})
		}
		if opts[0].Skip != nil {
			// Skip: İlk N kaydı atla (pagination için)
			explainCmd = append(explainCmd, bson.E{Key: "skip", Value: *opts[0].Skip})
		}
	}
	
	// MongoDB'ye explain komutunu gönder
	// verbosity: "executionStats" - Detaylı execution istatistikleri iste
	var result bson.M
	err := col.Database().RunCommand(ctx, bson.D{
		{Key: "explain", Value: explainCmd},           // Explain edilecek komut
		{Key: "verbosity", Value: "executionStats"},   // Detay seviyesi: executionStats = en detaylı
	}).Decode(&result)
	
	if err != nil {
		return nil, err
	}
	
	return result, nil
}

// PrintExplainResults - Explain sonuçlarını formatlayıp yazdırır
// Bu fonksiyon, MongoDB explain çıktısını okunabilir formatta gösterir ve
// performans sorunlarını işaretler (yavaş sorgular, index eksikliği vb.)
//
// Parametreler:
//   - explainResult: Explain komutundan dönen sonuçlar
//   - version: Test edilen versiyon adı (read_bad, read_v1 vb.)
//   - logger: Logger instance'ı (nil ise sadece ekrana yazar)
func PrintExplainResults(explainResult map[string]interface{}, version string, logger *Logger) {
	// Print fonksiyonlarını seç - logger varsa onu kullan, yoksa fmt kullan
	if logger != nil {
		logger.Printf("\n=== EXPLAIN SONUÇLARI - %s ===\n", version)
	} else {
		fmt.Printf("\n=== EXPLAIN SONUÇLARI - %s ===\n", version)
	}
	
	if executionStats, ok := explainResult["executionStats"].(map[string]interface{}); ok {
		if logger != nil {
			logger.Println("\n📊 Execution İstatistikleri:")
			logger.Printf("  ⏱️  Çalışma Süresi: %v ms\n", executionStats["executionTimeMillis"])
			logger.Printf("  🔍 İncelenen Doküman Sayısı: %v\n", executionStats["totalDocsExamined"])
			logger.Printf("  🔑 İncelenen Index Key Sayısı: %v\n", executionStats["totalKeysExamined"])
			logger.Printf("  ✅ Döndürülen Doküman Sayısı: %v\n", executionStats["nReturned"])
		} else {
			fmt.Println("\n📊 Execution İstatistikleri:")
			fmt.Printf("  ⏱️  Çalışma Süresi: %v ms\n", executionStats["executionTimeMillis"])
			fmt.Printf("  🔍 İncelenen Doküman Sayısı: %v\n", executionStats["totalDocsExamined"])
			fmt.Printf("  🔑 İncelenen Index Key Sayısı: %v\n", executionStats["totalKeysExamined"])
			fmt.Printf("  ✅ Döndürülen Doküman Sayısı: %v\n", executionStats["nReturned"])
		}
		
		// Performans uyarıları:
		// Eğer sorgu 100ms'den uzun sürüyorsa, yavaş olarak işaretle
		if execTime, ok := executionStats["executionTimeMillis"].(int64); ok && execTime > 100 {
			if logger != nil {
				logger.Println("  ⚠️  UYARI: Sorgu yavaş (>100ms) - Optimizasyon gerekebilir!")
			} else {
				fmt.Println("  ⚠️  UYARI: Sorgu yavaş (>100ms) - Optimizasyon gerekebilir!")
			}
		}
		
		// Eğer döndürülen doküman sayısından çok daha fazla doküman inceleniyorsa,
		// bu index eksikliğine işaret eder
		if totalExamined, ok := executionStats["totalDocsExamined"].(int64); ok {
			if nReturned, ok := executionStats["nReturned"].(int64); ok && nReturned > 0 {
				if totalExamined > nReturned*2 {
					ratio := totalExamined / nReturned
					if logger != nil {
						logger.Printf("  ⚠️  UYARI: Döndürülenden %dx daha fazla doküman inceleniyor (index gerekebilir!)\n", ratio)
					} else {
						fmt.Printf("  ⚠️  UYARI: Döndürülenden %dx daha fazla doküman inceleniyor (index gerekebilir!)\n", ratio)
					}
				}
			}
		}
	}
	
	// Query Planner bölümünü parse et ve göster
	// Bu bölüm, MongoDB'nin sorguyu nasıl çalıştıracağını gösterir
	if queryPlanner, ok := explainResult["queryPlanner"].(map[string]interface{}); ok {
		if logger != nil {
			logger.Println("\n📋 Sorgu Planı:")
		} else {
			fmt.Println("\n📋 Sorgu Planı:")
		}
		if winningPlan, ok := queryPlanner["winningPlan"].(map[string]interface{}); ok {
			if stage, ok := winningPlan["stage"].(string); ok {
				if logger != nil {
					logger.Printf("  🎯 Stage: %s\n", stage)
				} else {
					fmt.Printf("  🎯 Stage: %s\n", stage)
				}
				
				// COLLSCAN = Collection Scan - Tüm collection'ı tarar (ÇOK YAVAŞ!)
				// Bu durumda index kullanılmıyor demektir
				if stage == "COLLSCAN" {
					if logger != nil {
						logger.Println("  ⚠️  UYARI: Collection scan tespit edildi - INDEX GEREKLİ!")
						logger.Println("     → Tüm collection taranıyor, bu çok yavaş olabilir")
					} else {
						fmt.Println("  ⚠️  UYARI: Collection scan tespit edildi - INDEX GEREKLİ!")
						fmt.Println("     → Tüm collection taranıyor, bu çok yavaş olabilir")
					}
				} else if stage == "IXSCAN" {
					// IXSCAN = Index Scan - Index kullanarak tarar (HIZLI!)
					if logger != nil {
						logger.Println("  ✅ Index scan kullanılıyor - İyi!")
						if indexName, ok := winningPlan["indexName"].(string); ok {
							logger.Printf("  📇 Kullanılan Index: %s\n", indexName)
						}
					} else {
						fmt.Println("  ✅ Index scan kullanılıyor - İyi!")
						if indexName, ok := winningPlan["indexName"].(string); ok {
							fmt.Printf("  📇 Kullanılan Index: %s\n", indexName)
						}
					}
				} else if stage == "FETCH" {
					// FETCH = Index'ten bulunan dokümanları getir
					if logger != nil {
						logger.Println("  ✅ Index kullanılıyor ve dokümanlar getiriliyor")
					} else {
						fmt.Println("  ✅ Index kullanılıyor ve dokümanlar getiriliyor")
					}
				}
			}
		}
	}
	
	// Detaylı analiz için tam JSON çıktısını da göster
	// Bu, gelişmiş kullanıcıların daha detaylı inceleme yapması için
	jsonData, _ := json.MarshalIndent(explainResult, "", "  ")
	if logger != nil {
		logger.Println("\n📄 Detaylı Explain Çıktısı (JSON):")
		logger.Print(string(jsonData))
		logger.Println("")
		logger.Printf("=" + string(make([]byte, 50)) + "\n")
	} else {
		fmt.Println("\n📄 Detaylı Explain Çıktısı (JSON):")
		fmt.Print(string(jsonData))
		fmt.Println("")
		fmt.Printf("=" + string(make([]byte, 50)) + "\n")
	}
}

// PrintMetrics - Performans metriklerini yazdırır
// Bu fonksiyon, bir sorgunun performans metriklerini okunabilir formatta gösterir
// Hem Go tarafında ölçülen süreleri hem de MongoDB'nin kendi istatistiklerini içerir
//
// Parametreler:
//   - metrics: Toplanan performans metrikleri
//   - version: Test edilen versiyon adı
//   - logger: Logger instance'ı (nil ise sadece ekrana yazar)
func PrintMetrics(metrics QueryMetrics, version string, logger *Logger) {
	if logger != nil {
		logger.Printf("\n=== PERFORMANS METRİKLERİ - %s ===\n", version)
		logger.Printf("⏱️  Toplam Süre (Go): %v\n", metrics.Duration)
		logger.Printf("📦 Okunan Kayıt Sayısı: %d\n", metrics.RecordsRead)
		logger.Printf("💾 Kullanılan Bellek: %.2f MB\n", float64(metrics.MemoryUsed)/(1024*1024))
	} else {
		fmt.Printf("\n=== PERFORMANS METRİKLERİ - %s ===\n", version)
		fmt.Printf("⏱️  Toplam Süre (Go): %v\n", metrics.Duration)
		fmt.Printf("📦 Okunan Kayıt Sayısı: %d\n", metrics.RecordsRead)
		fmt.Printf("💾 Kullanılan Bellek: %.2f MB\n", float64(metrics.MemoryUsed)/(1024*1024))
	}
	
	// MongoDB'nin kendi execution istatistikleri varsa göster
	// Bu veriler, MongoDB'nin sorguyu nasıl çalıştırdığını gösterir
	if metrics.ExecutionStats != nil {
		if logger != nil {
			logger.Println("\n📊 MongoDB Execution İstatistikleri:")
			logger.Printf("  🔍 MongoDB Çalışma Süresi: %d ms\n", metrics.ExecutionStats.ExecutionTimeMillis)
			logger.Printf("  📄 İncelenen Doküman Sayısı: %d\n", metrics.ExecutionStats.TotalDocsExamined)
			logger.Printf("  🔑 İncelenen Index Key Sayısı: %d\n", metrics.ExecutionStats.TotalKeysExamined)
			logger.Printf("  ✅ Döndürülen Doküman Sayısı: %d\n", metrics.ExecutionStats.NReturned)
		} else {
			fmt.Println("\n📊 MongoDB Execution İstatistikleri:")
			fmt.Printf("  🔍 MongoDB Çalışma Süresi: %d ms\n", metrics.ExecutionStats.ExecutionTimeMillis)
			fmt.Printf("  📄 İncelenen Doküman Sayısı: %d\n", metrics.ExecutionStats.TotalDocsExamined)
			fmt.Printf("  🔑 İncelenen Index Key Sayısı: %d\n", metrics.ExecutionStats.TotalKeysExamined)
			fmt.Printf("  ✅ Döndürülen Doküman Sayısı: %d\n", metrics.ExecutionStats.NReturned)
		}
		
		// Verimlilik oranı hesapla
		// Bu, incelenen dokümanların ne kadarının gerçekten döndürüldüğünü gösterir
		// Yüksek oran = iyi (az doküman incelenip çok doküman döndürülüyor)
		// Düşük oran = kötü (çok doküman incelenip az doküman döndürülüyor)
		if metrics.ExecutionStats.TotalDocsExamined > 0 {
			efficiency := float64(metrics.ExecutionStats.NReturned) / float64(metrics.ExecutionStats.TotalDocsExamined) * 100
			if logger != nil {
				logger.Printf("  📈 Verimlilik Oranı: %.2f%%\n", efficiency)
				if efficiency < 50 {
					logger.Println("  ⚠️  UYARI: Düşük verimlilik - Index optimizasyonu gerekebilir")
				}
			} else {
				fmt.Printf("  📈 Verimlilik Oranı: %.2f%%\n", efficiency)
				if efficiency < 50 {
					fmt.Println("  ⚠️  UYARI: Düşük verimlilik - Index optimizasyonu gerekebilir")
				}
			}
		}
	}
	if logger != nil {
		logger.Println("=" + string(make([]byte, 50)) + "\n")
	} else {
		fmt.Println("=" + string(make([]byte, 50)) + "\n")
	}
}

