/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 1, Task 4
 *
 * Purpose:
 *   Driver for the MinTemperature MapReduce job. It wires the mapper and
 *   reducer together, configures the input/output paths, and submits the
 *   job to the cluster. The output, one row per year, will be
 *   (year, minTemperatureOfThatYear) where temperatures are in Celsius * 10.
 *
 * Citations:
 *   - Adapted from MaxTemperature.java in the Project 5 README
 *     (Tom White, "Hadoop: The Definitive Guide", 2011).
 */
package edu.cmu.andrew.mm6;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MinTemperature {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: MinTemperature <input path> <output path>");
            System.exit(-1);
        }

        JobConf conf = new JobConf(MinTemperature.class);
        conf.setJobName("Min temperature");

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(MinTemperatureMapper.class);
        conf.setReducerClass(MinTemperatureReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
    }
}
