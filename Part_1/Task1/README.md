# Task 1 - LetterCounter

## Build & Run Instructions

```bash
cd ~/Project5/Part_1/Task1

# Compile
mkdir -p lettercount_classes
javac -d lettercount_classes -cp lettercount_classes:$CLASSPATH LetterCounter.java

# Package the jar
jar -cvf lettercount.jar -C lettercount_classes/ .

# Ensure words.txt is already in HDFS (from Task 0). If not:
# hdfs dfs -copyFromLocal /home/public/words.txt /user/userID/input/words.txt

# Remove any prior output directory on HDFS
hdfs dfs -rm -r /user/userID/output

# Run the MapReduce job
hadoop jar ~/Project5/Part_1/Task1/lettercount.jar org.myorg.LetterCounter \
    /user/userID/input/words.txt /user/userID/output

# Merge the reducer output and sort by count, descending.
hdfs dfs -getmerge /user/userID/output /tmp/letterCounterMerged
sort -k 2nr /tmp/letterCounterMerged > ~/Project5/Part_1/Task1/Task1Output
```

`Task1Output` will have the letter "e" and its count at the top.
