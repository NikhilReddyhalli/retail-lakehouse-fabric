## GITHUB SETUP INSTRUCTIONS
## Run these commands on your local machine after downloading the project

## ─── Step 1: Install Git (if not already installed) ───
## Windows: https://git-scm.com/download/win
## Mac:     brew install git

## ─── Step 2: Create a new GitHub repo ───
## 1. Go to https://github.com/new
## 2. Repository name: retail-lakehouse-fabric
## 3. Description: End-to-End Retail Lakehouse on Microsoft Fabric | Medallion Architecture | PySpark | SCD Type-2 | DirectLake
## 4. Set to Public (for portfolio visibility)
## 5. Do NOT initialize with README (we have our own)
## 6. Click "Create repository"

## ─── Step 3: Push code to GitHub ───
cd retail-lakehouse

git init
git add .
git commit -m "feat: initial project setup — Retail Lakehouse Medallion Architecture

- Bronze layer: Raw CSV ingestion to Delta Lake with metadata columns
- Silver layer: PySpark transformations, type casting, data enrichment
- Gold layer: Star schema with fact_sales and 4 dimensions
- SCD Type-2: Customer dimension with full history tracking
- Fabric Pipeline: Full orchestration Bronze → Silver → Gold
- SQL DDL: Gold layer table definitions + analytical queries
- Sample data: 200 customers, 25 products, 8 stores, 5000 transactions"

git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/retail-lakehouse-fabric.git
git push -u origin main

## ─── Step 4: Add GitHub topics (for discoverability) ───
## Go to your repo → click the ⚙️ gear next to "About"
## Add topics: microsoft-fabric, pyspark, delta-lake, data-engineering,
##             medallion-architecture, lakehouse, power-bi, scd-type-2

## ─── Step 5: Add a description ───
## "Production-grade retail data lakehouse built on Microsoft Fabric.
## Implements Medallion Architecture (Bronze/Silver/Gold) with PySpark
## transformations, SCD Type-2 customer dimensions, and DirectLake Power BI reporting."
