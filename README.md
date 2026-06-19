# UAS Sistem Terdistribusi — Reliable Event Log Aggregator
Nama: Risky Alfienda
NIM: 11231087
Stack: **Python (FastAPI + asyncio)** + **Postgres**. Dedup kuat via **UNIQUE(topic,event_id)**, concurrency control via **SELECT … FOR UPDATE SKIP LOCKED**.


## Link Video Youtube
```
https://www.youtube.com/watch?v=4krL_np0mH0
```

## Link Drive Laporan
```
https://drive.google.com/file/d/1cl0DrieA1jnwta9N6jYWnRFMGlDlAtTn/view?usp=sharing
```

## Jalankan
```bash
docker compose up --build
```
Aggregator: `http://localhost:8080`

## Endpoint
- `POST /publish` (single/batch)
- `GET /events?topic=...`
- `GET /stats`
- `GET /health`

## Demo duplikasi cepat (PowerShell)
```powershell
curl.exe -X POST http://localhost:8080/publish `
  -H "Content-Type: application/json" `
  -d "[{`"topic`":`"auth`",`"event_id`":`"A-1`",`"timestamp`":`"2025-12-19T00:00:00Z`",`"source`":`"demo`",`"payload`":{`"x`":1}}]"
```

## Load test (opsional) dengan k6
```bash
docker compose --profile load run --rm k6
```

## Jalankan publisher simulator
Publisher akan otomatis jalan saat compose up. Untuk jalankan manual:
```bash
docker compose run --rm publisher
```

## Tests
Integration tests menggunakan **testcontainers** (butuh Docker lokal).
```bash
pip install -r aggregator/requirements.txt
pip install -r test/requirements.txt
pytest -q
```
---
# Reliable Event Log Aggregator

Sistem agregator log terdistribusi yang dirancang untuk menangani throughput tinggi dengan jaminan **idempotency**, **deduplication persisten**, dan **kontrol transaksi/konkurensi** di atas *message delivery* gaya Pub-Sub.  
Seluruh stack dijalankan di atas **Docker Compose** sehingga mudah direproduksi di lingkungan mana pun.

> Proyek ini merupakan implementasi UAS Sistem Terdistribusi: *Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi*.

---

## Teknologi Utama

![FastAPI](https://img.shields.io/badge/FastAPI-async-009688?style=for-the-badge&logo=fastapi)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=for-the-badge&logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python)
![k6](https://img.shields.io/badge/k6-load%20test-7D64FF?style=for-the-badge&logo=k6)

---

## Ringkasan Proyek

Masalah klasik pada sistem terdistribusi adalah **duplikasi event** dan **race condition** saat beberapa worker memproses log secara paralel.  
Dalam proyek ini:

- Setiap event memiliki kombinasi unik `topic + event_id`.
- Semua event disimpan ke **PostgreSQL** menggunakan **transaksi ACID**.
- Dedup dilakukan dengan **unique constraint** dan **UPSERT (`ON CONFLICT DO NOTHING`)**.
- Worker berjalan paralel tetapi tetap **idempotent**: event yang sama hanya di-*commit* satu kali.
- Data disimpan pada **named volume** sehingga tetap aman saat container dihentikan atau di-*recreate*.

Sistem diuji dengan:

- **17+ unit & integration test** menggunakan `pytest`.
- **Load test** menggunakan `k6` untuk >= 20.000 event dengan ≥ 30% duplikasi.

---

## Arsitektur Sistem

```mermaid
flowchart LR
    subgraph Clients
        P[Publisher Service]
        K[k6 Load Tester]
    end

    subgraph Infrastructure
        A[FastAPI Aggregator]
        B[Background Workers]
        DB[(PostgreSQL)]
    end

    P -->|POST /publish| A
    K -->|Stress Test| A
    A -->|Write + Dedup| DB
    A -->|Enqueue Work| B
    B -->|Update Stats / Side Effects| DB
```

> Diagram di atas merepresentasikan arsitektur logis. Di level fisik, seluruh service diorkestrasi oleh **Docker Compose** pada satu jaringan internal `appnet`.

---

## Struktur Folder

```text
.
├─ aggregator/        # Aplikasi FastAPI (API + worker + model data)
│  ├─ app/
│  │  ├─ main.py      # Entry point FastAPI
│  │  ├─ models.py    # Pydantic model response
│  │  ├─ db.py        # Koneksi & inisialisasi PostgreSQL
│  │  └─ worker.py    # Background worker & logika dedup/idempotency
│  └─ Dockerfile
├─ publisher/         # Generator event sekaligus idempotent producer sederhana
│  ├─ publisher.py
│  └─ Dockerfile
├─ aggregator/sql/    # Skrip SQL inisialisasi database (schema + constraint)
├─ k6/                # Skrip load testing
│  └─ publish.js
├─ test/              # 17+ unit & integration test
├─ docker-compose.yml # Orkestrasi seluruh layanan
├─ Teori.md           # Draft laporan teori Bab 1-13
└─ README.md          # Dokumentasi proyek
```

> Struktur aktual bisa sedikit berbeda tergantung commit terakhir, namun garis besarnya tetap seperti di atas.

---

## Konfigurasi & Environment

### 1. Service `storage` (PostgreSQL)

- Image: `postgres:16-alpine`
- Variabel:
  - `POSTGRES_DB=db`
  - `POSTGRES_USER=user`
  - `POSTGRES_PASSWORD=pass`
- Volume:
  - `pg_data:/var/lib/postgresql/data` (persistensi data)

### 2. Service `aggregator` (FastAPI)

- Build: `./aggregator`
- Port: `8080:8080`
- Env penting:

| Nama                | Contoh Nilai                                | Fungsi                                                  |
| ------------------- | ------------------------------------------- | ------------------------------------------------------- |
| `DATABASE_URL`      | `postgres://user:pass@storage:5432/db`      | Koneksi ke PostgreSQL                                  |
| `WORKERS`           | `4`                                         | Jumlah worker background                               |
| `BATCH_SIZE`        | `200`                                       | Jumlah maksimum event per batch di queue               |
| `POLL_INTERVAL_MS`  | `50`                                        | Interval polling queue (ms)                            |
| `STUCK_PROCESSING_SEC` | `300`                                    | Batas waktu deteksi job *stuck*                        |

### 3. Service `publisher`

- Build: `./publisher`
- Env penting:

| Nama          | Contoh Nilai                          | Fungsi                                            |
| ------------- | ------------------------------------- | ------------------------------------------------- |
| `TARGET_URL`  | `http://aggregator:8080/publish`      | Endpoint tujuan POST event                        |
| `COUNT`       | `20000`                               | Total event yang akan dikirim                     |
| `DUP_RATE`    | `0.30`                                | Rasio duplikasi (30% dari event akan di-*replay*) |
| `CONCURRENCY` | `50`                                  | Jumlah worker async di sisi publisher             |
| `TOPICS`      | `auth,payment,orders`                 | Daftar topic yang digunakan                       |
| `BATCH_SIZE`  | `100`                                 | Ukuran batch JSON per request                     |

### 4. Service `k6` (profil `load`)

- Image: `grafana/k6:latest`
- Profil Compose: `load`
- Skrip: `k6/publish.js`
- Env:
  - `TARGET_URL=http://aggregator:8080/publish`

---

## Cara Menjalankan

### 1. Prasyarat

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)

### 2. Menjalankan Aggregator + Database

Di root project:

```bash
docker compose up --build -d storage aggregator
```

Cek kesehatan service:

```bash
curl http://localhost:8080/health
# -> {"ok": true}
```

### 3. Mengirim Event dengan Publisher

Jalankan sekali (container akan otomatis berhenti setelah selesai):

```bash
docker compose run --rm publisher
```

Contoh output di terminal:

```text
done received=20000 inserted=14000 duplicates=6000 dup_rate=30.00%
```

Artinya:

- `received`  = total event dikirim.
- `inserted`  = event unik yang benar-benar tercatat.
- `duplicates`= event duplikat yang terdeteksi dan tidak diproses ulang.

### 4. Menjalankan Load Test dengan k6

Aktifkan profil `load`:

```bash
docker compose --profile load run --rm k6
```

Skrip `k6/publish.js` akan:

- Menjalankan **50 virtual user** secara paralel selama 20 detik.
- Menghasilkan lebih dari **20.000 event** dengan ≥ 30% duplikasi.
- Mengukur metrik seperti latency (`http_req_duration`) dan error rate.

---

## API Endpoint

Setelah `storage` dan `aggregator` berjalan:

- **Health Check**

  ```http
  GET /health
  ```

  Respon: `{"ok": true}` bila aplikasi siap menerima traffic.

- **Publish Event**

  ```http
  POST /publish
  Content-Type: application/json

  {
    "topic": "orders",
    "event_id": "demo-1",
    "timestamp": "2025-01-01T10:00:00Z",
    "source": "curl",
    "payload": { "status": "ok" }
  }
  ```

  Mendukung **single event** maupun **array of events**.

- **List Events per Topic**

  ```http
  GET /events?topic=orders
  ```

  Mengembalikan daftar event unik yang sudah diproses untuk topic tersebut.

- **Stats**

  ```http
  GET /stats
  ```

  Contoh respon:

  ```json
  {
    "received": 20000,
    "unique_processed": 14000,
    "duplicate_dropped": 6000,
    "topics": ["auth", "orders", "payment"],
    "queue": {"pending": 0, "processing": 0, "done": 14000},
    "uptime": 149.70
  }
  ```

  Field `unique_processed + duplicate_dropped + queue.pending + queue.processing` ≈ `received` selama worker masih memproses batch.

---

## Pengujian & Validasi

### 1. Menjalankan Test Otomatis

Di host (dengan dependency ter-install) atau di dalam container `aggregator`:

```bash
pip install -r aggregator/requirements.txt
pip install -r test/requirements.txt
pytest test/ -v
```

Cakupan test meliputi:

- Validasi skema request `/publish`.
- Deduplication: event sama tidak diproses dua kali.
- Konsistensi `GET /events` dan `GET /stats`.
- Perilaku multi-worker (tidak terjadi double-process).
- Persistensi: setelah *restart* service, data tetap ada.

### 2. Contoh Hasil k6 (Load Test)

Dari log k6:

- ~2.532 request dalam ~20 detik.
- Rata-rata `http_req_duration` ≈ **340 ms**.
- 99% request sukses dengan status **200/202**.
- Duplikasi yang dihasilkan di sisi aplikasi ≈ **38%**, dan seluruhnya ditangani oleh mekanisme dedup store.

Setelah load test, `/stats` menunjukkan, misalnya:

```json
{
  "received": 126550,
  "unique_processed": 78968,
  "duplicate_dropped": 47582,
  "topics": ["auth", "orders", "payment"],
  "queue": {"pending": 0, "processing": 0, "done": 78968},
  "uptime": 54.47
}
```

Ini membuktikan:

- Sistem dapat menangani puluhan ribu event dalam waktu singkat.
- Idempotency + deduplication bekerja di bawah beban tinggi.
- Counter statistik tetap konsisten.

---

## Contoh Interaksi Manual via `curl`

Publish event unik:

```bash
curl -X POST http://localhost:8080/publish   -H "Content-Type: application/json"   -d '{"topic": "orders", "event_id": "test-curl-999",
       "timestamp": "2023-10-27T10:00:00Z",
       "source": "manual",
       "payload": {"status": "ok"}}'
```

Output pertama kali:

```json
{"accepted":1,"inserted":1,"duplicates":0}
```

Kirim ulang dengan `event_id` yang sama:

```json
{"accepted":1,"inserted":0,"duplicates":1}
```

Dan `/stats` akan memperlihatkan `duplicate_dropped` bertambah, sedangkan `unique_processed` tetap.

---

## Detail Implementasi Teknis

| Komponen      | Teknologi / Pola                                      | Peran Utama                                                      |
| ------------- | ----------------------------------------------------- | ---------------------------------------------------------------- |
| API Framework | FastAPI (async)                                      | Menangani HTTP request dan routing endpoint                      |
| Database      | PostgreSQL 16 + asyncpg                              | Penyimpanan event dan statistik secara persisten                 |
| Validasi      | Pydantic                                             | Skema JSON event & query parameter                               |
| Idempotency   | `(topic, event_id)` + `UNIQUE` + UPSERT              | Menjamin event sama tidak diproses dua kali                      |
| Worker        | Async background task (multi-worker)                 | Mengambil job dari queue internal dan melakukan pemrosesan      |
| Orkestrasi    | Docker Compose                                       | Menjalankan `storage`, `aggregator`, `publisher`, dan `k6`       |
| Testing       | pytest + httpx + pytest-asyncio                      | Unit & integration test, termasuk uji race condition             |
| Load Test     | k6                                                   | Menguji throughput & stabilitas di bawah beban tinggi            |

---

## Penulis

**Risky**  
NIM: **1123187**  
Mata Kuliah: **Sistem Terdistribusi dan Paralel A**

---
