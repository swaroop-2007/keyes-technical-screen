### HOW TO RUN:

#### Install the dependencies
     pip install -r requirements.txt

#### On line 280, add the path to the sample excel files provided in the zip file.


#### Run the Python script
    python ExcelProcessor.py

### On the execution of the script it will generate: 

#### Raw Files: 
    Saved raw excel file which is being processed in directory raw_files/

#### Output Files:
    Processed parquet files (according to sheets) which are SQL-ready located in the same directory with the sub directory processed_files/timestamp/

    Processed file Metadata: JSON file in the sub directory processed_files/timestamp/

    Logs located in the same directory 



