/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 1, Task 3
 *
 * Purpose:
 *   Driver for the MaxTemperature MapReduce job. Wires the Mapper and Reducer
 *   together, configures I/O paths from the command line, and submits the
 *   job to the cluster.
 *
 * Source:
 *   Provided in the Project 5 README. Originally from "Hadoop: The Definitive
 *   Guide, Second Edition" by Tom White, 2011.
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

public class MaxTemperature {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        // Build a JobConf with this class as the main class.
        JobConf conf = new JobConf(MaxTemperature.class);
        conf.setJobName("Max temperature");

        // Input/output paths supplied on the command line.
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Specify the Mapper and Reducer implementations.
        conf.setMapperClass(MaxTemperatureMapper.class);
        conf.setReducerClass(MaxTemperatureReducer.class);

        // Output key/value types written by the reducer.
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // Submit and wait for job completion.
        JobClient.runJob(conf);
    }
}
