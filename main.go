package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Payment struct {
	CorrelationID string `json:"correlationId"`
	Amount        string `json:"amount"`
}

type ProcessedPayment struct {
	CorrelationID   string  `json:"correlationId"`
	Amount          float64 `json:"amount"`
	RequestedAt     string  `json:"requestedAt"`
	RequestedAtUnix int64   `json:"requestedAtUnix"`
	Processed       *bool   `json:"processed,omitempty"`
}

var (
	paymentPool = sync.Pool{
		New: func() interface{} {
			processed := true
			return &ProcessedPayment{Processed: &processed}
		},
	}

	responsePool = sync.Pool{
		New: func() interface{} {
			return &PaymentsSummaryResponse{}
		},
	}
)

type MemoryDatabase struct {
	data       []ProcessedPayment
	mu         sync.RWMutex
	writeQueue chan ProcessedPayment
	batchQueue chan []ProcessedPayment
	stats      struct {
		totalWrites uint64
		queueSize   int64
		batchSize   int64
	}
	searchBuffer []ProcessedPayment
	resultBuffer []byte
}

func NewMemoryDatabase() *MemoryDatabase {
	db := &MemoryDatabase{
		data:         make([]ProcessedPayment, 0, 17000),
		writeQueue:   make(chan ProcessedPayment, 8000),
		batchQueue:   make(chan []ProcessedPayment, 100),
		searchBuffer: make([]ProcessedPayment, 0, 1000),
		resultBuffer: make([]byte, 0, 64*1024),
	}

	numWorkers := 10
	for i := 0; i < numWorkers; i++ {
		go db.processWrites()
	}

	go db.batchAggregator()

	return db
}

func (db *MemoryDatabase) batchAggregator() {
	batch := make([]ProcessedPayment, 0, 500)
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case payment := <-db.writeQueue:
			atomic.AddInt64(&db.stats.queueSize, -1)
			batch = append(batch, payment)

			// Collect more payments (non-blocking)
			for len(batch) < cap(batch) {
				select {
				case payment := <-db.writeQueue:
					atomic.AddInt64(&db.stats.queueSize, -1)
					batch = append(batch, payment)
				default:
					goto checkFlush
				}
			}

		checkFlush:
			if len(batch) >= 100 || len(batch) > 0 {
				batchCopy := make([]ProcessedPayment, len(batch))
				copy(batchCopy, batch)

				select {
				case db.batchQueue <- batchCopy:
					atomic.AddInt64(&db.stats.batchSize, int64(len(batch)))
				default:
					db.directWrite(batchCopy)
				}

				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				batchCopy := make([]ProcessedPayment, len(batch))
				copy(batchCopy, batch)

				select {
				case db.batchQueue <- batchCopy:
					atomic.AddInt64(&db.stats.batchSize, int64(len(batch)))
				default:
					db.directWrite(batchCopy)
				}

				batch = batch[:0]
			}
		}
	}
}

func (db *MemoryDatabase) processWrites() {
	for batch := range db.batchQueue {
		db.directWrite(batch)
	}
}

func (db *MemoryDatabase) directWrite(batch []ProcessedPayment) {
	if len(batch) == 0 {
		return
	}

	db.mu.Lock()
	db.data = append(db.data, batch...)
	atomic.AddUint64(&db.stats.totalWrites, uint64(len(batch)))
	db.mu.Unlock()
}

func (db *MemoryDatabase) StorePayment(payment ProcessedPayment) {
	select {
	case db.writeQueue <- payment:
		atomic.AddInt64(&db.stats.queueSize, 1)
	default:
		go func() {
			db.mu.Lock()
			db.data = append(db.data, payment)
			db.mu.Unlock()
		}()
	}
}

func (db *MemoryDatabase) GetAllPayments() []ProcessedPayment {
	db.mu.RLock()
	dataLen := len(db.data)

	if cap(db.searchBuffer) >= dataLen {
		db.searchBuffer = db.searchBuffer[:dataLen]
	} else {
		db.searchBuffer = make([]ProcessedPayment, dataLen)
	}

	copy(db.searchBuffer, db.data)
	db.mu.RUnlock()

	result := make([]ProcessedPayment, dataLen)
	copy(result, db.searchBuffer)
	return result
}

func (db *MemoryDatabase) binarySearchStart(target int64) int {
	data := db.data
	left, right := 0, len(data)-1
	result := -1

	if right < 0 {
		return -1
	}

	for left <= right {
		mid := left + ((right - left) >> 1)
		midVal := data[mid].RequestedAtUnix

		if midVal >= target {
			result = mid
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	return result
}

func (db *MemoryDatabase) binarySearchEnd(target int64) int {
	data := db.data
	left, right := 0, len(data)-1
	result := -1

	if right < 0 {
		return -1
	}

	for left <= right {
		mid := left + ((right - left) >> 1)
		midVal := data[mid].RequestedAtUnix

		if midVal <= target {
			result = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return result
}

func (db *MemoryDatabase) GetPaymentsSummary(fromUnix, toUnix int64) (int64, float64) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	dataLen := len(db.data)
	if dataLen == 0 {
		return 0, 0
	}

	start := db.binarySearchStart(fromUnix)
	if start == -1 {
		return 0, 0
	}

	end := db.binarySearchEnd(toUnix)
	if end == -1 || end < start {
		return 0, 0
	}

	var totalRequests int64
	var totalAmount float64

	data := db.data
	maxIdx := len(data)

	for i := start; i <= end && i < maxIdx; i++ {
		payment := &data[i]
		if payment.Processed != nil && *payment.Processed {
			totalRequests++
			totalAmount += payment.Amount
		}
	}

	return totalRequests, totalAmount
}

type StoreRequest struct {
	Payment ProcessedPayment `json:"payment"`
}

type PaymentsSummaryResponse struct {
	Default  SummaryData `json:"default"`
	Fallback SummaryData `json:"fallback"`
}

type SummaryData struct {
	TotalRequests int64   `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

func (db *MemoryDatabase) handleRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		if r.URL.Path != "/store" {
			http.NotFound(w, r)
			return
		}

		payment := paymentPool.Get().(*ProcessedPayment)
		defer paymentPool.Put(payment)

		var req StoreRequest
		req.Payment = *payment

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		db.StorePayment(req.Payment)

	case "GET":
		switch r.URL.Path {
		case "/getAll":
			payments := db.GetAllPayments()
			w.Header().Set("Content-Type", "application/json")

			if err := json.NewEncoder(w).Encode(payments); err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}

		case "/getByRange":
			fromStr := r.URL.Query().Get("from")
			toStr := r.URL.Query().Get("to")

			var fromUnix, toUnix int64

			if fromStr != "" {
				if t, err := time.Parse(time.RFC3339, fromStr); err == nil {
					fromUnix = t.UnixMilli()
				} else {
					http.Error(w, "Invalid 'from' date format", http.StatusBadRequest)
					return
				}
			} else {
				fromUnix = 0
			}

			if toStr != "" {
				if t, err := time.Parse(time.RFC3339, toStr); err == nil {
					toUnix = t.UnixMilli()
				} else {
					http.Error(w, "Invalid 'to' date format", http.StatusBadRequest)
					return
				}
			} else {
				toUnix = time.Now().UnixMilli()
			}

			totalRequests, totalAmount := db.GetPaymentsSummary(fromUnix, toUnix)

			response := responsePool.Get().(*PaymentsSummaryResponse)
			defer responsePool.Put(response)

			response.Default.TotalRequests = totalRequests
			response.Default.TotalAmount = totalAmount
			response.Fallback.TotalRequests = 0
			response.Fallback.TotalAmount = 0

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(response); err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}

		default:
			http.NotFound(w, r)
		}

	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func main() {
	runtime.GOMAXPROCS(1)

	runtime.GC()

	db := NewMemoryDatabase()

	socketPath := os.Getenv("DATABASE_SOCKET_PATH")
	if socketPath == "" {
		socketPath = "/tmp/writer.sock"
	}

	os.Remove(socketPath)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Failed to create unix socket: %v", err)
	}
	defer listener.Close()

	if err := os.Chmod(socketPath, 0666); err != nil {
		log.Printf("Warning: Could not set socket permissions: %v", err)
	}

	server := &http.Server{
		Handler:           http.HandlerFunc(db.handleRequest),
		MaxHeaderBytes:    1024,
		ReadHeaderTimeout: 0,
	}

	log.Printf("Server running on unix socket: %s", socketPath)

	if err := server.Serve(listener); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
