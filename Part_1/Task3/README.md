# Task 3 - MaxTemperature

## Build & Run Instructions

```bash
cd ~/Project5/Part_1/Task3

# Compile. temperature_classes must exist before javac is called.
mkdir -p temperature_classes
javac -d temperature_classes -cp temperature_classes:$CLASSPATH MaxTemperatureMapper.java
javac -d temperature_classes -cp temperature_classes:$CLASSPATH MaxTemperatureReducer.java
javac -d temperature_classes -cp temperature_classes:$CLASSPATH MaxTemperature.java

# Package the jar
jar -cvf temperature.jar -C temperature_classes/ .

# Copy the input to HDFS (only needed once)
hdfs dfs -copyFromLocal /home/public/combinedYears.txt /user/userID/input/combinedYears.txt

# Remove any prior output directory on HDFS
hdfs dfs -rm -r /user/userID/output

# Run the job
hadoop jar ~/Project5/Part_1/Task3/temperature.jar edu.cmu.andrew.mm6.MaxTemperature \
    /user/userID/input/combinedYears.txt /user/userID/output

# Merge output
hdfs dfs -getmerge /user/userID/output ~/Project5/Part_1/Task3/Task3Output
```
