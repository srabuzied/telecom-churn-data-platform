# Cloud Layer (Google Cloud Storage - GCS)

## Overview
This layer represents the real cloud storage used in the Telecovision Churn Analytics Platform.

The dataset is stored in a Google Cloud Storage bucket named:
**telecom-churn-data**

It follows a Medallion Architecture (Bronze → Silver).

---

## Bucket Structure

### 🥉 Bronze Layer (Raw Data)
- Batch/
- Stream/
- calls/
- churn/
- network/
- usage/
- checkpoints/

This layer contains raw unprocessed data coming from batch and streaming pipelines.

---

### 🥈 Silver Layer (Processed Data)
- Data_From_Batch/
- Data_From_Stream/
- churn_risk/
- customer_care_calls/
- network_events/
- usage_events/

This layer contains cleaned and transformed datasets ready for analytics.

---

## Final Dataset
A consolidated processed dataset is stored as:

- silver_clean_data.csv (539MB)

This represents the final analytical dataset used for dashboards and modeling.

---

## Data Flow

Batch Pipeline → Bronze/Batch → Silver/Data_From_Batch  
Streaming Pipeline → Bronze/Stream → Silver/Data_From_Stream  
Final Merge → silver_clean_data.csv

---

## Cloud Configuration
- Location: US (multi-region)
- Storage Class: Standard
- Access: Not public
- Protection: Soft Delete enabled

---

## Purpose
- Centralized data lake for all pipelines
- Separation of raw and processed data
- Supports analytics, dashboards, and ML models
- Simulates real production GCP architecture

---

## Tools Used
- Google Cloud Storage (GCS)
- Google Cloud Platform (GCP)
