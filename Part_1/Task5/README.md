# Task 5 - Aggravated Assaults + Robberies

## Build & Run Instructions

```bash
cd ~/Project5/Part_1/Task5

mkdir -p aggravatedassaultsplusrobberies_classes
javac -d aggravatedassaultsplusrobberies_classes \
      -cp aggravatedassaultsplusrobberies_classes:$CLASSPATH \
      AggravatedAssaultsPlusRobberies.java

jar -cvf aggrvatedassaultsplusrobberies.jar \
    -C aggravatedassaultsplusrobberies_classes/ .

# Copy P1V.txt into HDFS
hdfs dfs -copyFromLocal /home/public/P1V.txt /user/userID/input/P1V.txt

# Remove any prior output directory
hdfs dfs -rm -r /user/userID/output

hadoop jar ~/Project5/Part_1/Task5/aggrvatedassaultsplusrobberies.jar \
    org.myorg.AggravatedAssaultsPlusRobberies \
    /user/userID/input/P1V.txt /user/userID/output

hdfs dfs -getmerge /user/userID/output ~/Project5/Part_1/Task5/Task5Output
```

The output file contains one line with the combined count.
