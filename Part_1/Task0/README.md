# Task 0 - WordCount

## Build & Run Instructions

```bash
cd ~/Project5/Part_1/Task0

# Compile
mkdir -p wordcount_classes
javac -d wordcount_classes -cp wordcount_classes:$CLASSPATH WordCount.java

# Package the jar
jar -cvf wordcount.jar -C wordcount_classes/ .

# Copy input to HDFS (only needed once)
hdfs dfs -mkdir -p /user/userID/input
hdfs dfs -copyFromLocal /home/public/words.txt /user/userID/input/words.txt

# Remove any prior output directory on HDFS
hdfs dfs -rm -r /user/userID/output

# Run the MapReduce job
hadoop jar ~/Project5/Part_1/Task0/wordcount.jar org.myorg.WordCount \
    /user/userID/input/words.txt /user/userID/output

# Merge and copy the output back to local
hdfs dfs -getmerge /user/userID/output ~/Project5/Part_1/Task0/Task0Output
```

The final merged output is written to `Task0Output`.
