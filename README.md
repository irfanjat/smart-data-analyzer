<div align="center">

# 🎬 Smart Data Quality Analyzer & Dashboard

### An end-to-end Python data pipeline for automated data cleaning, quality assessment, and interactive visualization

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.35.0-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![SQLite](https://img.shields.io/badge/SQLite-Built--in-003B57?style=for-the-badge&logo=sqlite&logoColor=white)](https://sqlite.org)
[![Plotly](https://img.shields.io/badge/Plotly-5.22.0-3F4F75?style=for-the-badge&logo=plotly&logoColor=white)](https://plotly.com)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)


[Features](#-features) • [Quick Start](#-quick-start) • [Architecture](#-architecture) • [Dashboard](#-dashboard) • [DevOps](#-devops-deployment) • [Contributing](#-contributing)

</div>

---

## 📌 Overview

Real-world datasets are rarely clean. This project builds a **complete, modular 4-stage data pipeline** that:

1. **Ingests** raw CSV data into a normalized SQLite relational database
2. **Assesses** data quality across 5 issue categories automatically
3. **Cleans** all detected issues with a full audit trail logged to the database
4. **Visualizes** before/after insights through a 6-page interactive Streamlit dashboard

Built as a term project for **Advanced Database Management Systems**, this pipeline directly demonstrates DBMS concepts including schema design, DDL/DML, transactions, data integrity, and audit logging.

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| 🗄️ **SQLite Backend** | 3-table normalized schema — raw, cleaned, and audit log |
| 🔍 **5-Category Assessment** | Missing values, duplicates, outliers, type errors, format issues |
| 🧹 **Automated Cleaning** | 6 fix functions with configurable strategies |
| 📋 **Full Audit Trail** | Every transformation logged to DB + text file |
| 📊 **Interactive Dashboard** | 6 Streamlit pages with Plotly charts |
| 💾 **CSV Export** | Download cleaned data directly from the dashboard |
| 🐳 **Docker Ready** | Containerized for consistent deployment |
| ♻️ **Idempotent Pipeline** | Safe to re-run — DELETE + re-insert on every run |

---

## 🗂️ Project Structure

```
smart_data_analyzer/
├── data/
│   ├── raw/                    ← Place netflix_titles.csv here
│   └── cleaned/                ← Auto-generated cleaned CSV
├── database/
│   └── data.db                 ← SQLite database (auto-generated)
├── src/
│   ├── __init__.py
│   ├── loader.py               ← Stage 1: CSV ingestion + schema creation
│   ├── analyzer.py             ← Stage 2: Quality assessment (5 checks)
│   ├── cleaner.py              ← Stage 3: Cleaning pipeline (6 fixes)
│   └── logger.py               ← Audit logging helper
├── dashboard/
│   └── app.py                  ← Stage 4: Streamlit dashboard
├── logs/
│   └── cleaning_log.txt        ← Human-readable audit trail
├── tests/
│   ├── __init__.py
│   ├── test_loader.py
│   ├── test_analyzer.py
│   └── test_cleaner.py
├── .github/
│   └── workflows/
│       └── ci.yml              ← GitHub Actions CI pipeline
├── Dockerfile
├── docker-compose.yml
├── .gitignore
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

---

## 🚀 Quick Start

### Option 1 — Local Setup

```bash
# 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/smart-data-analyzer.git
cd smart-data-analyzer

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate        # Linux/Mac
# venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Add your dataset
# Download netflix_titles.csv from https://kaggle.com/datasets/shivamb/netflix-shows
# Place it in data/raw/netflix_titles.csv

# 5. Run the pipeline
python src/loader.py            # Ingest raw data
python src/analyzer.py         # Assess data quality
python src/cleaner.py          # Clean the data

# 6. Launch the dashboard
streamlit run dashboard/app.py
# Open http://localhost:8501
```

### Option 2 — Docker

```bash
# Clone and run with Docker Compose
git clone https://github.com/YOUR_USERNAME/smart-data-analyzer.git
cd smart-data-analyzer

# Add your dataset to data/raw/netflix_titles.csv, then:
docker compose up --build

# Dashboard available at http://localhost:8501
```

---

## 🏗️ Architecture

```
CSV File
   │
   ▼
┌──────────────┐     ┌─────────────────────────────────────┐
│  loader.py   │────▶│           SQLite (data.db)           │
│  Stage 1     │     │  ┌─────────────┐ ┌───────────────┐  │
└──────────────┘     │  │ raw_titles  │ │ cleaning_log  │  │
                     │  └─────────────┘ └───────────────┘  │
┌──────────────┐     │  ┌───────────────────────────────┐  │
│ analyzer.py  │────▶│  │      cleaned_titles           │  │
│  Stage 2     │     │  └───────────────────────────────┘  │
└──────────────┘     └─────────────────────────────────────┘
                              │              │
┌──────────────┐              │              │
│  cleaner.py  │──────────────┘              │
│  Stage 3     │                             │
└──────────────┘                             │
                                             │
┌──────────────┐                             │
│   app.py     │◀────────────────────────────┘
│  Stage 4     │
└──────────────┘
       │
       ▼
 http://localhost:8501
```

### Database Schema

#### `raw_titles` — untouched source of truth
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER | Primary key, autoincrement |
| show_id | TEXT | Netflix identifier |
| type | TEXT | Movie / TV Show |
| title | TEXT | — |
| director | TEXT | 29.9% null in raw |
| cast | TEXT | 9.4% null in raw |
| country | TEXT | 9.4% null in raw |
| date_added | TEXT | Inconsistent formats in raw |
| release_year | TEXT | String in raw, cast to INT after cleaning |
| rating | TEXT | 3 rows had duration here (data error) |
| duration | TEXT | — |
| listed_in | TEXT | Comma-separated genres |
| description | TEXT | — |
| inserted_at | TEXT | Auto-timestamp |

#### `cleaned_titles` — same structure, `release_year` is INTEGER, no nulls  
#### `cleaning_log` — audit trail for every transformation

---

## 📊 Dashboard

The Streamlit dashboard has 6 pages:

| Page | Description |
|------|-------------|
| 📊 **Overview** | Before/After null comparison bar chart + quality score (87.6% → 100%) |
| 🔍 **Missing Values** | Null % per column + 200-row heatmap |
| 📈 **Distributions** | Release year histogram, box plots, titles added over time |
| 🗂️ **Category Analysis** | Content ratings, top 15 countries, top 15 genres |
| 📋 **Cleaning Log** | Filterable audit table — filter by column or issue type |
| 💾 **Export** | Preview + one-click download of cleaned CSV |

---

## 🧪 Running Tests

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

---

## 🐳 DevOps Deployment

See the full [DevOps Guide](#-devops-deployment) section below for:
- Docker containerization
- GitHub Actions CI/CD pipeline
- Environment variable management
- Production deployment checklist

---

## 📦 Dataset

This project uses the [Netflix Movies and TV Shows](https://www.kaggle.com/datasets/shivamb/netflix-shows) dataset from Kaggle.

- **8,807 records** | **12 columns**
- Known issues: missing directors, misplaced duration values, inconsistent date formats

> **Note:** The dataset is not included in this repository. Download `netflix_titles.csv` from Kaggle and place it in `data/raw/`.

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit your changes: `git commit -m 'Add my feature'`
4. Push to the branch: `git push origin feature/my-feature`
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License — see [LICENSE](LICENSE) for details.

---

## 👨‍💻 Author

**Irfan Ali**  


---

<div align="center">
⭐ If you found this project useful, please give it a star!
</div>
