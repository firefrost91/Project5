/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 1, Task 0
 *
 * Purpose:
 *   A classic Hadoop MapReduce program that counts how many times each
 *   whitespace-delimited token (word) occurs in an input text file.
 *
 *   The Mapper reads each line, tokenizes the line on whitespace, and emits
 *   a (word, 1) pair for every token. Hadoop groups the intermediate pairs by
 *   key (the word) and calls the Reducer once per distinct word with the
 *   iterable of 1's. The Reducer sums the 1's and emits (word, totalCount).
 *
 * Source:
 *   Provided in the Project 5 README (package org.myorg). Starter code
 *   from /home/public/WordCount.java as noted in the assignment.
 */
package org.myorg;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

    // Mapper: for each line of the input, emit (word, 1) for every token.
    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Convert the Hadoop Text object to a Java String.
            String line = value.toString();
            // Break the line into whitespace-delimited tokens.
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                // Set the Text 'word' to the next token and emit (word, 1).
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    // Reducer: sum all of the 1's that share the same word key.
    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // Emit (word, totalCount) for this key.
            context.write(key, new IntWritable(sum));
        }
    }

    // Driver: configure and submit the MapReduce job to the cluster.
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // args[0] = HDFS input path, args[1] = HDFS output directory.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }
}
