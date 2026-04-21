# Task 2 - FindPattern

## Build & Run Instructions

```bash
cd ~/Project5/Part_1/Task2

# Compile
mkdir -p findpattern_classes
javac -d findpattern_classes -cp findpattern_classes:$CLASSPATH FindPattern.java

# Package the jar
jar -cvf findpattern.jar -C findpattern_classes/ .

# Remove any prior output directory on HDFS
hdfs dfs -rm -r /user/userID/output

# Run the MapReduce job
hadoop jar ~/Project5/Part_1/Task2/findpattern.jar org.myorg.FindPattern \
    /user/userID/input/words.txt /user/userID/output

# Merge the output back to local
hdfs dfs -getmerge /user/userID/output ~/Project5/Part_1/Task2/Task2Output
```

`Task2Output` will be a list of every word (case preserved) that contains
the substring "fun" (case-insensitive), e.g., "Fungia", "defunct",
"Infundibulata".
