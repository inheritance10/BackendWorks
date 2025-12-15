package main

import (
	"fmt"
	"io"
	"os"
	"time"
)

// Logger - Hem ekrana hem dosyaya yazma için logger yapısı
// Bu yapı, tüm çıktıları hem terminal'e hem de bir dosyaya yazar
type Logger struct {
	file   *os.File
	writer io.Writer
}

// NewLogger - Yeni bir logger oluşturur
// Parametreler:
//   - filename: Çıktıların kaydedileceği dosya adı (örn: "read_bad_results.txt")
//
// Döndürür:
//   - *Logger: Logger instance'ı
//   - error: Dosya oluşturma hatası varsa
func NewLogger(filename string) (*Logger, error) {
	// Dosyayı oluştur veya aç (append mode - varsa üzerine ekle)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("dosya oluşturulamadı: %v", err)
	}

	// Hem ekrana (os.Stdout) hem dosyaya yazmak için MultiWriter kullan
	// Bu sayede her yazı hem terminal'e hem dosyaya gider
	writer := io.MultiWriter(os.Stdout, file)

	return &Logger{
		file:   file,
		writer: writer,
	}, nil
}

// Printf - Formatlanmış string'i hem ekrana hem dosyaya yazar
// fmt.Printf gibi çalışır ama hem terminal'e hem dosyaya yazar
// any kullanıyoruz çünkü Go 1.18+ ile fmt.Printf any kullanıyor
func (l *Logger) Printf(format string, args ...any) (int, error) {
	return fmt.Fprintf(l.writer, format, args...)
}

// Print - String'i hem ekrana hem dosyaya yazar
// fmt.Print gibi çalışır ama hem terminal'e hem dosyaya yazar
func (l *Logger) Print(args ...any) (int, error) {
	return fmt.Fprint(l.writer, args...)
}

// Println - String'i hem ekrana hem dosyaya yazar ve satır sonu ekler
// fmt.Println gibi çalışır ama hem terminal'e hem dosyaya yazar
func (l *Logger) Println(args ...any) (int, error) {
	return fmt.Fprintln(l.writer, args...)
}

// Close - Logger'ı kapatır ve dosyayı kapatır
// Mutlaka defer ile çağrılmalı (dosya kaynaklarını serbest bırakmak için)
func (l *Logger) Close() error {
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// WriteHeader - Test başlığını yazar (test adı, tarih, saat vb.)
// Bu, her test dosyasının başına yazılır
func (l *Logger) WriteHeader(testName string) {
	l.Printf("\n")
	l.Printf("=" + string(make([]byte, 60)) + "\n")
	l.Printf("TEST: %s\n", testName)
	l.Printf("Tarih: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	l.Printf("=" + string(make([]byte, 60)) + "\n")
	l.Printf("\n")
}

