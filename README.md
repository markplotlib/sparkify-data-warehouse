<img src="./img/redshift.png" alt="Amazon Redshift" width="350" title="Amazon Redshift" align="middle" />

# Sparkify Data Warehouse
ETL Pipeline in AWS Redshift and S3

## Project Summary
In this project, I build an **ETL Pipeline** (Extraction, Transformation, Loading)
of a large data set from a fictitious music streaming service named *Sparkify*.
The ETL process flows from Amazon Web Service's (AWS)
Simple Storage Service (S3)
into staging tables in **AWS Redshift** (for data warehouses).

<img src="./img/etl.jpg" alt="Amazon Redshift" width="500" title="Amazon Redshift" align="middle" />

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

<img alt="Entity Relationship Diagram" src="./img/sparkify_ERD.png" />

**Fact table**: `songplays` -- every occurrence of a song being played is stored here.

**Dimension tables**:

* `users` -- the users of the Sparkify music streaming app

* `songs` -- the songs in Sparkify's music catalog

* `artists` -- the artists who record the catalog's songs

* `time` -- the timestamps of records in songplays, broken down into specific date and time units (year, day, hour, etc.)

### Run Instructions

1. Clone this repository, which will place the 3 `.py` files and the `.cfg` file
 into the same directory.
2. Duplicate the `dwh_template.cfg` file to create a new file named `dwh.cfg`.
 Because this will contain private login credentials, be sure it is added to the
 `.gitignore` file.
3. Fill in the `[CLUSTER]` and `[IAM_ROLE]` attributes from AWS,
 according to the IAM role and Redshift cluster already created. Please consult
 AWS's well-documented instructions as necessary.
4. Run `python create_tables.py` to set up the Redshift data warehouse cluster.
5. Run `python etl.py`. This will copy the 2 large tables from S3 into staging tables.
 This will also populate the smaller dimension tables.
