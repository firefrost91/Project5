/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 1, Task 4
 *
 * Purpose:
 *   Mapper for the MinTemperature MapReduce job. Identical logic to the
 *   MaxTemperature mapper: parse the year from columns 15-18, parse the
 *   temperature (Celsius * 10) from columns 87-92 (with a sign character),
 *   and emit (year, airTemperature) for readings with a good quality code.
 *
 *   The minimum is computed in the reducer, so the mapper output is the
 *   same as for the max temperature job.
 *
 * Citations:
 *   - Adapted from the MaxTemperatureMapper in the Project 5 README (Tom White,
 *     "Hadoop: The Definitive Guide", 2011).
 */
package edu.cmu.andrew.mm6;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MinTemperatureMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, IntWritable> {

    // NCDC sentinel that indicates a missing temperature.
    private static final int MISSING = 9999;

    public void map(LongWritable key, Text value,
                    OutputCollector<Text, IntWritable> output,
                    Reporter reporter) throws IOException {

        String line = value.toString();

        // Year is in columns 15-18.
        String year = line.substring(15, 19);

        // Temperature starts at column 87 with a sign character.
        int airTemperature;
        if (line.charAt(87) == '+') {
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }

        // Quality code at column 92 must be one of the acceptable values.
        String quality = line.substring(92, 93);
        if (airTemperature != MISSING && quality.matches("[01459]")) {
            output.collect(new Text(year), new IntWritable(airTemperature));
        }
    }
}
