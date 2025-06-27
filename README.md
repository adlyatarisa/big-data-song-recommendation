# Sistem Rekomendasi Lagu Big Data

## Deskripsi
Sistem rekomendasi lagu yang scalable menggunakan teknologi big data seperti Kafka, Spark, MinIO, dan Trino. Sistem ini menyediakan rekomendasi lagu real-time menggunakan algoritma machine learning yang di-deploy dalam environment containerized.

## Arsitektur
![alt text](images/arsitektur.png)

Sistem ini mengikuti arsitektur big data modern:
- **Kafka**: Stream processing untuk data real-time
- **MinIO**: Object storage untuk dataset dan model
- **Spark**: Machine learning training dan processing
- **Trino**: SQL query engine untuk analitik data
- **Flask API**: Backend service rekomendasi
- **Streamlit**: Dashboard interaktif untuk visualisasi

## Struktur Project
```
big-data-song-recommendation/
├── src/
│   ├── app.py                    # Flask API (backend)
│   ├── streamlit_app.py          # Streamlit dashboard (frontend)
│   ├── data/
│   │   ├── __init__.py
│   │   └── preprocessor.py       # Logic preprocessing data
│   ├── models/
│   │   ├── __init__.py
│   │   └── recommendation_engine.py
│   ├── api/
│   │   ├── __init__.py
│   │   └── routes.py
│   └── utils/
│       ├── __init__.py
│       └── helpers.py
├── data/
│   ├── raw/                      # Dataset asli (file CSV)
│   ├── processed/                # Dataset yang sudah diproses
│   └── models/                   # Model ML yang sudah ditraining
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── requirements.txt
├── .gitignore
├── .dockerignore
└── README.md
```

## Prerequisites
- Docker Desktop terinstall dan berjalan
- Git

## Cara Setup

1. **Clone Repository**
   ```bash
   git clone <repository-url>
   cd big-data-song-recommendation
   ```

2. **Jalankan Application Stack**
   ```bash
   cd docker
   docker compose up --build
   ```

## Dokumentasi Pengerjaan 

### Tampilan web MusicBot

![image](https://github.com/user-attachments/assets/d834edc4-4a89-46a8-b1c0-41d569b62aaf)

Berikut merupakan tampilan dari web MusicBot, yang akan memberikan rekomendasi musik kepada user melalui popularitas lagu dari data yang ada. Beberapa fitur lain yang melengkapi adalah adanya fitur klik 'like' untuk user ketika user menyukai suatu lagi, serta klik 'direct link to song' untuk memberikan pengalaman mendengarkan lagu langsung kepada user dengan mengarahkan mereka ke aplikasi Spotify untuk mendengarkan lagu sesuai preferensi.

### Rekomendasi berdasar popularitas lagu

![image](https://github.com/user-attachments/assets/48991efe-9cb8-4970-b180-e2fab210fcfe)

Rekomendasi lagu diberikan berdasar data popularitas dari tiap lagu, sehingga pengguna bisa mendapatkan rekomendasi lagu untuk didengarkan.

### Fitur direct link to song

https://github.com/user-attachments/assets/488e15c0-d8ba-4ed3-9532-721bd97dcc04

Fitur direc link to song untuk memberikan pengalaman langsung kepada user untuk tidak hanya sekedar mendapatkan rekomendasi, tetapi juga bisa mendengarkan lagu tersebut.
