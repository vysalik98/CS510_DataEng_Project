# CS510_DataEng_Project

### Project Overview
This project is centered around TriMet, a prominent public transportation agency based in Portland, Oregon. TriMet operates an extensive network of buses, light rail, and commuter rail systems. The main objective of this project is to collect and process real-time GPS sensor data, often referred to as "breadcrumbs," from TriMet's buses. We aim to analyze movement patterns and vehicle behavior.

### Technical Design
Data Retrieval: The 5-second interval sensor readings(breadcrumbs) are retrieved from a specified URL, which serves as input for the Kafka Producer module.  
Kafka Producer: This module is responsible for publishing the sensor data to a Kafka topic, allowing further processing and consumption.  
Kafka Consumer: The Kafka Consumer subscribes to the Kafka topic, performs data transformations, validations, and ensures reliability and fault tolerance.  
Data Insertion: Distinct trip information is inserted into the Trip table, and sensor readings go into the Breadcrumb table. A Kafka Producer and Consumer for StopEvents data(contains additional trip information)is also developed, processed, and inserted into the StopEvents table in a Postgres database.  

### Project Phases
Part 1: Kafka infrastructure setup, development of Kafka Producer and Consumer modules for breadcrumb data, setting up automatic  and termination of GCP instances, automating cron jobs, and data writing to a file.  
Part 2: Focus on efficient batch processing of data, Performing Data Validation and Transformation, and writing it to a Postgres database across multiple tables to optimize performance.  
Part 3: Gathering Stop Events Data, implementation of Kafka Producer and Consumer Modules, new Data Validation and Transformation, Loading to Database, perform join operation across different tables, creating views and enabling data visualizations for deeper insights.  

### Key Achievements
* Real-time ingestion and processing of TriMet GPS sensor data.
* Utilization of Confluent Kafka for scalable and fault-tolerant data handling.
* Efficient batch processing to reduce data latency.
* Data analysis using Pandas for actionable insights.
* Loading over one million records into Postgres with Python and SQL.
* Creation of interactive geospatial visualizations using Mapbox for improved decision-making.

### Repository Contents
Kafka Producer and Consumer Code  
SQL Queries for Data Analysis  
Data Validation and Transformation Code  
