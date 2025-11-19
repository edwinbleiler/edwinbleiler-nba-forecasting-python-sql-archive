# NBA Player Forecasting Project (Python + SQL)
*Created by Edwin (Ed) Bleiler*

This project forecasts NBA player performance using structured analytics, a normalized SQL data model, and a Python-based feature pipeline.  
It reflects the way I approach complex, ambiguous problems in Strategy & Operations, Product, and Chief of Staff roles: define the system, build clear metrics, and create a repeatable analytical framework that supports decision-making.

[edwinbleiler.com](https://edwinbleiler.com)  
[LinkedIn](https://www.linkedin.com/in/edwin-ed-bleiler/)  
GitHub: [@edwinbleiler](https://github.com/edwinbleiler)

---

## Why This Project Matters (Professional Context)

Although the dataset is sports-related, the work mirrors the analytical and operational challenges I’ve led professionally:

- Turning unstructured information into a clean data model  
- Designing metrics that reflect reliability, volatility, and context  
- Building forecasting frameworks grounded in real data  
- Automating repeatable processes  
- Communicating assumptions, logic, and outputs clearly  
- Applying structure in ambiguous problem spaces — a core requirement in Strategy, Product, and Chief of Staff work

The value of the project isn’t in predicting basketball outcomes — it’s in demonstrating the systems thinking, modeling discipline, and clarity of execution that translate directly into operational analytics, vendor evaluation, forecasting workflows, and product design.  It is also a fun way to learn and develop Python and SQL skills.

---

## Project Overview

**Goal:** Build a forecasting workflow that projects NBA player performance by combining:  
- Rolling 10-game performance averages  
- A consistency score (volatility measurement)  
- Opponent defensive strength by position (DvP)

**Stack:**  
Python | SQL | SQLite | nba_api | pandas | numpy | matplotlib

**Core Outputs:**  
- Fully structured SQL database  
- Feature engineering pipeline  
- Multi-factor forecast per player  
- CSV and visualization outputs  
- Reproducible notebooks documenting analysis steps

---

## Approach & Methodology

### 1. Data Ingestion
- Pull team, player, game, and box score data via the NBA API  
- Normalize and store in a relational SQLite database  
- Maintain raw and processed versions for auditing and reproducibility

### 2. SQL Schema Design
- Tables for teams, players, games, boxscores  
- Views for computed fantasy-like metrics (FPPG)  
- Efficient joins for matchup and positional relationships

### 3. Feature Engineering
Metrics include:
- **Rolling averages** (10-game windows)  
- **Consistency score** (coefficient of variation)  
- **Opponent defensive factor (DvP)**  
- **Forecast score** combining performance, volatility, and matchup context

### 4. Forecasting Formula (simple, interpretable)
base = fppg_last_10
matchup_adj = base * dvp_factor
forecast = matchup_adj * (0.5 + 0.5 * consistency_score / 100)


This framework prioritizes interpretability—useful not only for sports data but for any structured forecasting or prioritization model.

### 5. Outputs
- `outputs/player_forecasts.csv`  
- Visual summaries (player volatility, matchup strength, etc.)  
- Jupyter notebooks walking through discovery → modeling → forecasting

---

## Skills Demonstrated

This project showcases skills directly relevant to Strategy, Product, and Chief of Staff roles:

- **Analytical Modeling** — translating qualitative ideas into measurable metrics  
- **Data Architecture** — building normalized SQL schemas and ETL pipelines  
- **Product Thinking** — defining inputs, constraints, and clear interpretation logic  
- **Operational Analytics** — turning multi-source data into decision frameworks  
- **Automation** — building repeatable and reliable workflows  
- **Technical Fluency** — Python, SQL, API integration, reproducible analysis  
- **Clarity & Communication** — documenting assumptions, methodology, and results  
- **System Design** — structuring end-to-end processes from ingestion to insight  

---

## Example Output (placeholder)

| Player | Team | Avg FP (L10) | Consistency | Opp DvP | Forecast |
|--------|------|--------------|-------------|---------|----------|
| Player A | BOS | 41.2 | 88 | 1.05 | 43.7 |
| Player B | DEN | 35.9 | 76 | 0.95 | 34.1 |

---

## Repository Structure
edwinbleiler-nba-forecasting-python-sql/
│
├── data/
│ ├── raw/
│ └── processed/
├── sql/
│ ├── schema.sql
│ ├── views_forecasting.sql
│ └── queries_examples.sql
├── src/
│ ├── db.py
│ ├── fetch_data.py
│ ├── build_features.py
│ ├── calculate_consistency.py
│ ├── defense_vs_position.py
│ └── project_tonight.py
├── notebooks/
│ ├── exploratory_analysis.ipynb
│ ├── forecasting_model.ipynb
│ └── matchup_analysis.ipynb
├── outputs/
│ └── player_forecasts.csv
├── PROJECT_PLAN.md
├── LEARNING_LOG.md
├── ABOUT_THIS_PROJECT.md
├── requirements.txt
└── .gitignore


---

## About the Author

**I’m Edwin (Ed) Bleiler, a Strategy & Operations leader focused on structuring ambiguity, building data-informed decision frameworks, and designing scalable systems across product, analytics, and cross-functional operations.**  
My work centers on clarity, structure, and measurable execution — whether I’m supporting leadership decisions, improving operational processes, or designing analytical workflows like this project.

Learn more:  
**https://edwinbleiler.com**  
**https://www.linkedin.com/in/edwin-ed-bleiler/**  
GitHub: **@edwinbleiler**

