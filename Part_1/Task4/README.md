# Task 4 - MinTemperature

## Build & Run Instructions

```bash
cd ~/Project5/Part_1/Task4

mkdir -p mintemperature_classes
javac -d mintemperature_classes -cp mintemperature_classes:$CLASSPATH MinTemperatureMapper.java
javac -d mintemperature_classes -cp mintemperature_classes:$CLASSPATH MinTemperatureReducer.java
javac -d mintemperature_classes -cp mintemperature_classes:$CLASSPATH MinTemperature.java

jar -cvf mintemperature.jar -C mintemperature_classes/ .

# Input file already on HDFS from Task 3. If not:
# hdfs dfs -copyFromLocal /home/public/combinedYears.txt /user/userID/input/combinedYears.txt

hdfs dfs -rm -r /user/userID/output

hadoop jar ~/Project5/Part_1/Task4/mintemperature.jar edu.cmu.andrew.mm6.MinTemperature \
    /user/userID/input/combinedYears.txt /user/userID/output

hdfs dfs -getmerge /user/userID/output ~/Project5/Part_1/Task4/Task4Output
```

The output values are temperatures in Celsius * 10.
