#### Dependencies:
`pip install psycopg2`

#### Instructions:
In `config.ini`, specify `SYSTEM_TYPE` as either `windows` or `unix`, and `DATA_LOCATION` as an absolute path to a directory containing CSV files. 

If `CREATE_NEW_TABLE` is false, then the data will be concatenated to the table specified in the `EXISTING_TABLE_NAME` field. Otherwise, a new table will be created for each file using the filename.