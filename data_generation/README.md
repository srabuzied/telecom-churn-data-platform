# Data Generation (SDV Module)

## Overview
This module generates synthetic telecom customer data using SDV (Synthetic Data Vault).
We increased the dataset size from 100,000 records to 1,000,000 records to simulate a realistic production-scale environment for churn analytics.

## Why we did this
- To simulate real telecom customer behavior at scale
- To increase dataset size for better analytics and modeling
- To test pipeline performance on large-scale data
- To feed batch, streaming, and dashboard systems

## Data Scaling
- Original dataset size: 100,000 records
- Final synthetic dataset size: 1,000,000 records
- Method: SDV generative modeling

## Tools Used
- Python
- SDV (Synthetic Data Vault)
- Pandas
- NumPy

## Files
- sdv_generate.py → Script responsible for generating synthetic data

## How to Run
```bash
pip install sdv pandas numpy
python sdv_generate.py
