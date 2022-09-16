## Simple JAVA APP for deleting rows in a database

### To start the app :  

1. Compile the project : <br>
<code>mvn clean compile assembly:single</code>

2. Run the app : 
<code>java -jar target\delete-rows-job-1.0.0-jar-with-dependencies.jar guDev guUser guUser operationgu dateTraitement 34 10 5 6</code>
<br>
- arg1 : database
- arg2 : username
- arg3 : password
- arg4 : table name
- arg5 : date column
- arg6 : delete before in hours
- arg7 : limit of rows
- arg8 : delete interval in seconds
- arg9 : stop hour
