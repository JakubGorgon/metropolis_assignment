# Polish Metropolis Empire Simulator 🏙️

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://metropolis-empire-simulator.streamlit.app/)

## 📋 Overview
This project is an algorithmic simulation of **Metropolitan Gravity**, designed to model how major Polish cities (Metropolises) exert influence over surrounding towns based on population mass and distance.

It demonstrates a scalable data engineering approach to:
1.  **Strict Data Cleaning** (distinguishing true administrative capitals from districts/duplicates).
2.  **Geospatial Simulation** (Snowball effect: as cities capture towns, they grow larger and their reach expands).
3.  **Performance Optimization** (Vectorized NumPy calculations & Spark ETL).

**🚀 Live Demo:** [https://metropolis-empire-simulator.streamlit.app/](https://metropolis-empire-simulator.streamlit.app/)

---

## 🛠️ Architecture & Tech Stack

The solution is divided into two distinct phases: **Data Engineering (ETL)** and **Simulation (Core Logic)**.

### 1. The ETL Pipeline (`PySpark`)
We handle the raw Geonames dataset (`PL.txt`) using **Apache Spark** to ensure scalability and strict correctness.

*   **Logic:**
    *   **Inhabited Places Only:** Filters for specific feature codes (villages, towns, capitals etc.) to exclude hotels, forests, or abandoned sections. 
    *   **The "Shield" Logic:** Custom logic resolves district ambiguity (e.g., distinguishing the main city administrative entity from its sub-districts). Ensures we remove districts of cities (dzielnice miast) from the dataset. Use of window functions ensures we only simulate the primary administrative entity.

### 2. The Simulation Engine (`NumPy` + `Pandas`)
Once the data is clean, the physics simulation runs in memory for real-time interactivity.

*   **Logic:**
    *   **Gravity Model:** Impact decays exponentially with distance: $I = e^{-1.4 \times \frac{d}{R}}$.
    *   **Snowball Effect:** Captured towns add their population to the Metropolis, increasing its radius for the next iteration ($R = f(Population)$).
    *   All distances use the **Haversine Formula**.
*   **Optimization:**
    *   **Vectorization:** Instead of a Cross Join which scales $O(N \times M)$, we use **NumPy Broadcasting**.

---

## 📂 Project Structure

```
.
├── data/                   # Raw input data (Geonames PL.txt)
├── output/                 # Cleaned simulation-ready CSV
├── notebooks/              # Prototyping and Exploration
├── src/
│   ├── etl.py              # PySpark cleaning pipeline
│   ├── simulation.py       # Physics engine 
│   ├── config.py           # Constants, Schemas, Parameters
│   └── spark_utils.py      # Session management
├── tests/                  # PyTest Suite
│   ├── test_etl.py         # Testing the "Shield" logic
│   ├── test_simulation.py  # Testing physics & math correctness
│   └── test_data_quality.py # Integration tests (Data Validation)
├── dashboard.py            # Streamlit Interactive App
├── run_etl.py              # Entry point for Data Pipeline
└── requirements.txt        # Dependencies
```

---

## ⚙️ Setup & Usage

### Prerequisites
*   Python 3.10+
*   Java 8/11/17 (for PySpark)

### Installation
```bash
git clone https://github.com/JakubGorgon/metropolis_assignment
cd metropolis-simulator
pip install -r requirements.txt
```

### Running the ETL
Process the raw data into a clean dataset:
```bash
python run_etl.py
```

### Running the Dashboard
Launch the interactive simulation:
```bash
streamlit run dashboard.py
```

### Running Tests
Execute the test suite to verify physics and data integrity:
```bash
python -m pytest tests/
```

---

## 🧠 Key Decisions Checklist

| Requirement | Implementation Decision |
| :--- | :--- |
| **Real locations only** | Strict whitelist on `feature_code` (PPL, PPLC, etc.) + Population > 0 + Admin Code Shielding. |
| **Strict Distances** | **Haversine Formula** used throughout.|
| **Metropolis Definitions** | Dynamic population threshold in UI. Algorithm iteratively assigns towns to the strongest metro. |
| **Snowball Effect** | Implemented Iterative Loop where $Population_{t+1} = Population_t + \sum Captured_{towns}$. |
| **Performance** | **PySpark** for data prep, **NumPy Broadcasting** for $O(1)$ matrix operations in simulation. |
