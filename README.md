

# ⚡ PySpark Job Orchestrator

A lightweight **PySpark job orchestration API** built with **FastAPI** and **Docker**.  
It allows you to submit, monitor, and retrieve results from Spark-based ETL or analytics jobs through a simple REST interface.

---

## 🚀 Features

- 🧠 Submit prebuilt or custom PySpark jobs via REST API  
- 📊 Real-time job status tracking (RUNNING, FAILED, SUCCEEDED)  
- 📁 Log and result management (separate folders)  
- 🔐 API key–based authentication  
- 🐳 Fully containerized using Docker & Docker Compose  
- ⚡ FastAPI + Uvicorn backend  

---

## 🧰 Tech Stack

| Component | Technology |
|------------|-------------|
| Backend Framework | FastAPI |
| Data Engine | Apache Spark 3.5.1 |
| Language | Python 3 |
| Containerization | Docker |
| Deployment | Uvicorn |
| Authentication | API Key |
| Logging | Local file logs |

---

## 📂 Project Structure


```bash
├── app/
│   ├── main.py                # FastAPI application
│   ├── jobs_db.py             # In-memory job database and status tracking
│   ├── utils.py               # Helper functions (Spark execution, logs, etc.)
│   └── ...
├── jobs/                      # Prebuilt Spark job scripts
│   └── top_customers_revenue.py
├── data/                      # Sample input CSV files
├── outputs/                   # Job result outputs
├── logs/                      # Job log files
├── Dockerfile                 # Backend Docker image
├── docker-compose.yml         # Multi-container setup
├── requirements.txt           # Python dependencies
├── .env.example               # Example environment file
└── README.md
```

---

## ⚙️ Setup Instructions

### 1️⃣ Clone the Repository
```bash
git clone https://github.com/<your-username>/pyspark-job-orchestrator.git
cd pyspark-job-orchestrator
```

2️⃣ Create a .env File
Copy the example file and fill in your details:
```bash
cp .env.example .env
```

Example:

```bash
API_KEY=my-super-secret-key-123
CORS_ORIGINS=http://localhost:3000
OUTPUTS_PATH=../outputs/
JOBS_PATH=../jobs/
LOGS_PATH=../logs/
```

3️⃣ Build and Run with Docker

```bash
docker-compose build
docker-compose up
```

Your FastAPI app will be running on:
👉 http://localhost:8000

🔑 API Usage
Submit a Job

```bash
curl -X POST "http://localhost:8000/api/jobs/submit" \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: my-super-secret-key-123" \
  -d '{
    "job_name": "top_customers_revenue",
    "job_type": "prebuilt",
    "input_paths": {
      "customers": "./data/customers.csv",
      "orders": "./data/orders.csv",
      "order_items": "./data/order_items.csv",
      "products": "./data/products.csv"
    },
    "output_path": "./outputs/",
    "timeout_seconds": 300
  }'
  ```

Check Job Status

```bash
curl -X GET "http://localhost:8000/api/jobs/<job_id>" \
  -H "X-API-KEY: my-super-secret-key-123"
```

Download Job Result

```bash
curl -X GET "http://localhost:8000/api/jobs/<job_id>/result" \
  -H "X-API-KEY: my-super-secret-key-123" -O
```

View Logs

```bash
curl -X GET "http://localhost:8000/api/jobs/<job_id>/logs" \
  -H "X-API-KEY: my-super-secret-key-123"
```


🧪 Example Job
top_customers_revenue.py calculates the top customers by total revenue
from CSV datasets of customers, orders, and order items using PySpark.

🛡️ Security


All endpoints require a valid X-API-KEY header.


.env file is excluded from version control for safety.


Example config is available in .env.example.






