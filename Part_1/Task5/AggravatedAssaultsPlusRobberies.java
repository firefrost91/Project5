/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 1, Task 5
 *
 * Purpose:
 *   Read the tab-delimited Pittsburgh crime file P1V.txt and compute the
 *   combined total of all aggravated assault and robbery incidents. The
 *   answer is a single integer (e.g., 150) written to the output file.
 *
 *   Schema of P1V.txt (tab delimited):
 *     col 0: X coordinate (State Plane, feet)
 *     col 1: Y coordinate (State Plane, feet)
 *     col 2: time
 *     col 3: street address
 *     col 4: offense type (e.g., "aggravated assault", "Robbery")
 *     col 5: date
 *     col 6: 2000 census tract
 *
 * Strategy:
 *   Mapper: If column 4 equals "aggravated assault" or "Robbery" (matched
 *           case-insensitively for safety), emit ("total", 1). Every record
 *           uses the same key so that exactly one reducer sees all the data.
 *   Reducer: Sum the 1's and emit ("total", count).
 *
 * This task must be completed WITHOUT any external sources per the README.
 */
package org.myorg;

import java.io.IOException;

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

public class AggravatedAssaultsPlusRobberies extends Configured implements Tool {

    // Mapper: for every line whose 5th column is aggravated assault or robbery,
    // emit a single shared key so all records funnel to one reducer invocation.
    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static Text OUT_KEY = new Text("aggravatedAssaultsPlusRobberies");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Split the record on tab characters. The file is tab-delimited.
            String[] fields = line.split("\t");
            // Defensive: skip malformed lines that do not have the offense column.
            if (fields.length < 5) {
                return;
            }
            // Trim the offense text and compare case-insensitively so that "Robbery",
            // "robbery", "ROBBERY", "Aggravated Assault", etc. all match.
            String offense = fields[4].trim().toLowerCase();
            if (offense.equals("aggravated assault") || offense.equals("robbery")) {
                context.write(OUT_KEY, one);
            }
        }
    }

    // Reducer: sum all ones. Because every mapper emits the same key, there
    // is one reduce call and it holds the global total.
    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Driver.
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "aggravatedAssaultsPlusRobberies");
        job.setJarByClass(AggravatedAssaultsPlusRobberies.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        // Using the same key from every mapper guarantees a single reducer task.
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AggravatedAssaultsPlusRobberies(), args);
        System.exit(res);
    }
}
