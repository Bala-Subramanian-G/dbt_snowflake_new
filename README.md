# Medallion Architecture-Based Data Pipeline for Snowflake  

This project implements a **Medallion Architecture** in **Snowflake**, using **dbt** for data transformations across multiple layers. The goal is to create a structured, scalable, and analytics-ready data pipeline.  

### **Current Implementation**  

- **Data Ingestion & Raw Storage:**  
  Extracted sales data from **PostgreSQL** and loaded it into Snowflake’s **Landing Layer** using **Informatica**.  

- **Deduplication & Staging:**  
  Applied a **hashing technique** to remove duplicates and stored the cleaned data in the **Stage Layer**.  

- **Bronze Layer (SCD Type 2):**  
  Implemented **Slowly Changing Dimension (SCD) Type 2** using **dbt snapshots**, tracking historical changes while maintaining deduplicated records.  

### **Work in Progress**  

- **Silver Layer (SCD Type 1):**  
  - Schema created in Snowflake.  
  - Pending: Writing dbt models to apply data validation (null checks, count checks, integrity checks) and maintain only the latest records.  

### **Future Plans**  

- **Gold Layer (Analytics-Optimized):**  
  - Implement **dimensional modeling** to structure Silver Layer data for analytics and reporting.  
  - Optimize for **BI tools** and **performance efficiency**.  

This project follows **modular, scalable, and version-controlled** transformation practices using dbt. 
