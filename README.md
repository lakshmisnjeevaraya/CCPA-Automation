# ccpa_automation_test
Automation code for ccpa

1. Execute sNOW Get api and gets the folllowing fields: tasks/sys_id,record type
2. Using sys_id perform get api to mongo DB and gets the cosumner information
3. If data is present then check the Redshift and unload the CSV file to S3
4. Close the task in snow with valid state and close code
