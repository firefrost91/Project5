/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 1, Task 6
 *
 * Purpose:
 *   Read P1V.txt and count the number of aggravated-assault incidents that
 *   occurred within 100 meters of 3803 Forbes Avenue in Oakland, whose
 *   State Plane (X, Y) coordinate is (1354326.897, 411447.7828).
 *
 *   State Plane coordinates are measured in feet, while the 100-meter
 *   threshold is in meters, so we convert the Euclidean (Pythagorean)
 *   distance from feet to meters (1 foot = 0.3048 meters) before comparing.
 *
 * Strategy:
 *   Mapper: Parse X and Y from the first two tab-delimited columns. If the
 *           5th column equals "aggravated assault" (case-insensitive) AND the
 *           distance from the reference point, converted to meters, is less
 *           than 100, emit (sharedKey, 1).
 *   Reducer: Sum the 1's and emit (sharedKey, total).
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

public class OaklandCrimeStats extends Configured implements Tool {

    // 3803 Forbes Avenue State Plane coordinates, in feet.
    private static final double REF_X = 1354326.897;
    private static final double REF_Y = 411447.7828;

    // Conversion factor: one foot equals 0.3048 meters.
    private static final double FEET_TO_METERS = 0.3048;

    // Threshold: 100 meters.
    private static final double THRESHOLD_METERS = 100.0;

    // Mapper.
    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static Text OUT_KEY = new Text("aggravatedAssaultsWithin100m");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Split on tab characters.
            String[] fields = line.split("\t");
            // Skip malformed rows.
            if (fields.length < 5) {
                return;
            }

            // Only aggravated-assault rows matter.
            String offense = fields[4].trim().toLowerCase();
            if (!offense.equals("aggravated assault")) {
                return;
            }

            // Parse the (X, Y) State Plane coordinates (in feet). Skip rows whose
            // coordinate fields are not numeric.
            double x;
            double y;
            try {
                x = Double.parseDouble(fields[0].trim());
                y = Double.parseDouble(fields[1].trim());
            } catch (NumberFormatException e) {
                return;
            }

            // Pythagorean distance in feet.
            double dx = x - REF_X;
            double dy = y - REF_Y;
            double distanceFeet = Math.sqrt(dx * dx + dy * dy);

            // Convert to meters and test.
            double distanceMeters = distanceFeet * FEET_TO_METERS;
            if (distanceMeters < THRESHOLD_METERS) {
                context.write(OUT_KEY, one);
            }
        }
    }

    // Reducer.
    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "oaklandCrimeStats");
        job.setJarByClass(OaklandCrimeStats.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        // One reducer since every mapper emits the same key.
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
        int res = ToolRunner.run(new Configuration(), new OaklandCrimeStats(), args);
        System.exit(res);
    }
}
