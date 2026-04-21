# Task 6 - Oakland Crime Stats

## Build & Run Instructions

```bash
cd ~/Project5/Part_1/Task6

mkdir -p oaklandcrimestats_classes
javac -d oaklandcrimestats_classes -cp oaklandcrimestats_classes:$CLASSPATH OaklandCrimeStats.java

jar -cvf oaklandcrimestats.jar -C oaklandcrimestats_classes/ .

# P1V.txt already on HDFS from Task 5. If not:
# hdfs dfs -copyFromLocal /home/public/P1V.txt /user/userID/input/P1V.txt

hdfs dfs -rm -r /user/userID/output

hadoop jar ~/Project5/Part_1/Task6/oaklandcrimestats.jar org.myorg.OaklandCrimeStats \
    /user/userID/input/P1V.txt /user/userID/output

hdfs dfs -getmerge /user/userID/output ~/Project5/Part_1/Task6/Task6Output
```

`Task6Output` is a single line with the count of aggravated-assault
incidents within 100 m of (1354326.897, 411447.7828) — 3803 Forbes Avenue.
