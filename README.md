## Simple JAVA APP for switching to the database according to the time 

### To start the app :  

1. Compile the project : <br>
<code>mvn clean compile assembly:single</code>

2. Run the app : <br>
<code>java -jar target\delete-rows-job-1.0.0-jar-with-dependencies.jar localhost3306/guDev guUser guUser localhost3306/guDev guUser guUser 15 5 2</code>
<br>
- arg1 : host:port/database master <br> 
- arg2 : username master <br>
- arg3 : password master <br>
- arg4 : host:port/database replica <br>
- arg5 : username replica <br>
- arg6 : password replica <br>
- arg7 : numbers of seconds to switch to master <br>
- arg8 : seconds to back up to replica <br>
- arg9 : time to loop <br>
