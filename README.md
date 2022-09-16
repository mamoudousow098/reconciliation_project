## Simple JAVA APP for deleting rows in a database

### To start the app :  

1. Compile the project : <br>
<code>mvn clean compile assembly:single</code>

2. Run the app : <br>
<code>java -jar target\delete-rows-job-1.0.0-jar-with-dependencies.jar guDev guUser guUser operationgu dateTraitement 34 10 5 6</code>
<br>
- arg1 : database <br>
- arg2 : username <br>
- arg3 : password <br>
- arg4 : table name <br>
- arg5 : date column <br>
- arg6 : delete before in hours <br>
- arg7 : limit of rows <br>
- arg8 : delete interval in seconds <br>
- arg9 : stop hour <br>
