---
title: F1 OLAP Dashboard
emoji: 🏎️
colorFrom: red
colorTo: blue
sdk: gradio
app_file: app/dashboard.py
pinned: false
---

# F1 OLAP Dashboard

An interactive analytical dashboard for Formula 1 data, built with **Gradio**, **DuckDB**, and **Plotly**. This project uses OLAP (Online Analytical Processing) techniques to explore race data and apply machine learning models for classification and clustering.

## Hosting & Deployment

This application is hosted on **Hugging Face Spaces**. It is automatically synced from this GitHub repository using GitHub Actions.

## Deployment Setup

1. **GitHub Sync**: The project uses `.github/workflows/sync-to-hub.yml` to mirror the repository to Hugging Face.
2. **Dependencies**: Managed via `pyproject.toml` and automatically exported to `requirements.txt`.
3. **Large Files**: Machine learning models (`assets/models/*.pkl`) and local databases are managed using **Git LFS**.

## Tech Stack

- **UI Framework**: [Gradio](https://gradio.app/)
- **Database Engine**: [DuckDB](https://duckdb.org/) for fast analytical queries.
- **Data Source**: Remote Parquet files from Hugging Face Datasets & local SQLite/DuckDB files.
- **Machine Learning**:
  - **Random Forest** for driver classification.
  - **K-Means** for clustering performance patterns.
  - **FP-Growth** for association rule mining.
- **Visualization**: Plotly Express and Altair.

## Project Structure

- `app/`: Main application logic, including the dashboard, database manager, and OLAP cubes.
- `assets/models/`: Pre-trained scikit-learn models (tracked via Git LFS).
- `scripts/`: Utility scripts for data loading and database maintenance.
- `sql/`: Schema definitions.

## Local Setup

Follow these steps to run the dashboard locally:

### 1. Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) (recommended)
- Git LFS

### 2. Clone and install

```bash
git clone https://github.com/nveshaan/f1_olap.git
cd f1_olap
git lfs install
git lfs pull
```

### 3. Setup Environment

Using `uv`:

```bash
uv sync
```

### 4. Data Loading

The project includes scripts to fetch data from FastF1 and populate the databases.

#### Initialize and Load Season Data

To fetch and process a specific year's data:

```bash
uv run python scripts/load.py
```

_Note: This script defaults to years 2018-2025 and loads race sessions (session 5) with telemetry._

#### Combine Yearly Databases

To merge individual year databases into a single OLAP-ready file:

```bash
uv run python scripts/combine.py --dest f1.db
```

### 5. Run the Dashboard

```bash
uv run python app/dashboard.py
```

The app will be available at `http://localhost:7860`.
