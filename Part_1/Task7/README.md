# Task 7 - Oakland Crime Stats KML

## Build & Run Instructions

```bash
cd ~/Project5/Part_1/Task7

mkdir -p oaklandcrimestatskml_classes
javac -d oaklandcrimestatskml_classes -cp oaklandcrimestatskml_classes:$CLASSPATH OaklandCrimeStatsKML.java

jar -cvf oaklandcrimestatskml.jar -C oaklandcrimestatskml_classes/ .

# Copy CrimeLatLonXYTabs.txt to HDFS
hdfs dfs -copyFromLocal /home/public/CrimeLatLonXYTabs.txt /user/userID/input/CrimeLatLonXYTabs.txt

hdfs dfs -rm -r /user/userID/output

hadoop jar ~/Project5/Part_1/Task7/oaklandcrimestatskml.jar org.myorg.OaklandCrimeStatsKML \
    /user/userID/input/CrimeLatLonXYTabs.txt /user/userID/output

hdfs dfs -getmerge /user/userID/output ~/Project5/Part_1/Task7/Task7Output
```

Load `Task7Output` in Google Earth Pro to view the placemarks. Be sure to
include a screenshot of the result in the PDF submission.
