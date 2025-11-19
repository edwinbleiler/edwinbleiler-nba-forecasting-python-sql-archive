# Project Plan — NBA Player Forecasting (Python + SQL)
*Created by Edwin (Ed) Bleiler*

## 🎯 Objective
Build a structured forecasting pipeline that predicts NBA player performance using recent-game trends, player consistency, and opponent defensive context.  
The focus is learning-driven, highlighting practical data modeling and analysis in Python and SQL.

## 🧱 Phase 1 — Setup
- Create GitHub repo: `edwinbleiler-nba-forecasting-python-sql`
- Initialize folder structure: `/data`, `/sql`, `/src`, `/notebooks`, `/outputs`
- Create SQLite schema (`sql/schema.sql`)
- Test NBA API connectivity

## 🧮 Phase 2 — Data Ingestion
- Pull player, team, and game data using `nba_api`
- Save structured tables into SQLite
- Store raw JSON/csv extracts in `/data/raw/`

## 📊 Phase 3 — Feature Engineering
- Compute fantasy-like box score metrics  
- Build rolling 10-game averages  
- Generate “consistency score” using volatility (std/mean)  
- Calculate opponent defensive efficiency by position (DvP)

## 🔮 Phase 4 — Forecasting Model
Uses a simple and interpretable formula:
base = fppg_last_10
matchup_adj = base * dvp_factor
forecast = matchup_adj * (0.5 + 0.5 * consistency_score / 100)

## 📘 Phase 5 — Documentation & Publishing
- Maintain project documentation (README, PROJECT_PLAN, LEARNING_LOG)
- Add examples, visuals, and outputs
- Connect repository to personal site and LinkedIn
- Commit updates regularly as learning progresses

## 🚀 Future Enhancements
- Integrate DFS salary/value projections  
- Add Streamlit web dashboard  
- Explore team-level forecasting or betting model extensions  

© 2025 Edwin (Ed) Bleiler
