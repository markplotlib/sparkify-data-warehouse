# Sparkify Data Warehouse
ETL Pipeline in AWS Redshift and S3

## Project Summary
In this project, I build an **ETL Pipeline** (Extraction, Transformation, Loading)
of a large data set from a fictitious music streaming service named *Sparkify*.
The ETL process flows from Amazon Web Service's (AWS) 
Simple Storage Service (S3) 
into staging tables in **AWS Redshift** (for data warehouses).

I then query the staged data into an analytics table.
This will help *Sparkify's* analytics team learn insight about its customer base.

## File Descriptions

#### create_tables.py
create fact and dimension tables for the star schema in Redshift.

#### sql_queries.py
define SQL statements, which will then be imported into the other files.

#### etl.py
load data from S3 into staging tables on Redshift, and then process that data into analytics tables on Redshift.

### Design Decisions

#### Keyspace Star Schema

The *star* schema is used, with a **fact** table centered around **dimension** tables at its periphery.

**Fact table**: `songplays` -- every occurrence of a song being played is stored here.

**Dimension tables**:

* `users` -- the users of the Sparkify music streaming app

* `songs` -- the songs in Sparkify's music catalog

* `artists` -- the artists who record the catalog's songs

* `time` -- the timestamps of records in songplays, broken down into specific date and time units (year, day, hour, etc.)

### Run instructions
