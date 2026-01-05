# The Interplay of Digital Assets and Traditional Markets under Macroeconomic Trends

**HSLU Data Lakes & Warehouses Module | Team Pacific**  
Johan Ferreira • Jade Bullock • Nhat Bui

## Overview

An AWS-based data lake and warehouse analyzing how cryptocurrencies (BTC, ETH) and traditional markets (S&P 500, NASDAQ) interact under varying macroeconomic conditions. The platform ingests data from 6 sources, transforms it through a medallion architecture, and serves analytics via Tableau dashboards.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Data Sources│ ──▶ │  S3 /raw    │ ──▶ │S3/transform │ ──▶ │ S3/curated  │ ──▶ │RDS Postgres │ ──▶ Tableau
│  (6 APIs)   │     │  (Bronze)   │     │  (Silver)   │     │   (Gold)    │     │ (Warehouse) │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
      │                   │                   │                   │
 ECS Fargate         AWS Glue            AWS Glue           Glue ETL
 EventBridge        PySpark Jobs        PySpark Jobs        to RDS
```

## Data Sources

| Source | Coverage | Key Data |
|--------|----------|----------|
| Yahoo Finance | Stocks, indices, bonds, commodities, currencies, crypto | OHLCV prices |
| FRED | 378 US economic indicators | Interest rates, CPI, GDP, employment |
| CryptoCompare | ~1,100 cryptocurrencies | Historical OHLCV |
| CoinGecko | Crypto market data | Market cap, volume rankings |
| DBnomics | 258 international series | ECB, BIS, OECD, IMF, Eurostat |
| GitHub US-Stock-Symbols | ~8,000 tickers | Filtered to ~4,000 liquid stocks |

## Project Structure

```
├── config/        # Ticker lists, state files, API configs
├── raw/           # Bronze layer - raw Parquet from APIs
├── transformed/   # Silver layer - cleaned, standardized, validated
├── curated/       # Gold layer - wide-format analytics table with derived metrics
└── rds/           # Data warehouse DDL and ETL scripts
```

## Key Features

**Data Lake (S3)**
- Medallion architecture (Bronze → Silver → Gold)
- Hive-style partitioning by asset_class/source/date
- Parquet with Snappy compression
- Per-source state tracking for incremental loads

**Transformations (AWS Glue)**
- Data quality filters (null removal, price validation)
- Forward-fill for macro indicators
- 50+ derived metrics: returns, volatility (7d/30d), correlations, z-scores, normalized prices

**Data Warehouse (RDS PostgreSQL)**
- Denormalized flat table (~70 columns, daily grain)
- Automated incremental loads via EventBridge

**Visualization (Tableau)**
- Portfolio manager: volatility, correlation, performance comparison
- Policy analyst: macro trends, shock analysis, drawdowns
- Data scientist: data quality, pipeline reliability

## Tech Stack

- **Compute:** ECS Fargate, AWS Glue (PySpark)
- **Storage:** S3, RDS PostgreSQL
- **Orchestration:** EventBridge, Lambda
- **Security:** Secrets Manager, IAM
- **Monitoring:** CloudWatch, SNS alerts
- **Visualization:** Tableau
- **Languages:** Python 3.11

## License

Academic project - HSLU Lucerne, January 2025
