/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 1, Task 4
 *
 * Purpose:
 *   Reducer for the MinTemperature MapReduce job. For each year key it
 *   receives an iterator of air temperatures (Celsius * 10) and emits the
 *   minimum as (year, minTemp).
 *
 * Citations:
 *   - Adapted from MaxTemperatureReducer in the Project 5 README
 *     (Tom White, "Hadoop: The Definitive Guide", 2011).
 */
package edu.cmu.andrew.mm6;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MinTemperatureReducer extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> output,
                       Reporter reporter) throws IOException {

        // Start the running minimum at the largest possible int so the first
        // iterator value always replaces it.
        int minValue = Integer.MAX_VALUE;
        while (values.hasNext()) {
            minValue = Math.min(minValue, values.next().get());
        }
        // Emit (year, minTemp).
        output.collect(key, new IntWritable(minValue));
    }
}
