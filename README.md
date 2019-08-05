Summary:
This project aims to build a ETL to transfer song and log datasets (in json format) to a relational database. The database consists of a fact table and several dimension tables. 

sql_queries.py file contains all SQL queries. The ETL codes are written in the etl.py file. In this file, song_data files and log_data files are processed seperately. All files are processed from a loop. Each file's process is can be found in the process_song_file function and process_log_file function. etl.ipynb is a jupyter notebook that contains experiemental code for etl processes. test.ipynb is a jupyter notebook that can help to visualize success/failure of each injection to a perticular table. 

How to run:
Open a terminal console. 
ls to find the etl.py
execute it by python etl.py
