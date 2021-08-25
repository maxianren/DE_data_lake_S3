# DE_data_lake

Summary of the project A music streaming startup, Sparkify, want to move their data warehouse to a data lake. Their data resides in S3 and the task is to build an ETL pipeline that extracts their data from S3 using Spark, and loads the data back into S3 as a set of dimensional tables for analytics team.

Description of the working folder supporting file

dwh.cfg: credential information to access cloud service

etl.py: connect to cloud server, extract data from S3, transform them into desired form and load data back into S3 as final product

debug.ipynb: code to test if the implementation correspond to expectation and debug

Steps of implementation:

Activate terminal, go to the working directory Type "python etl.py", when the program finish running, the desired dimension table should be created in the objective directory. There will be indication when each table is created.# DE_data_lake_S3
