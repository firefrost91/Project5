/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 1, Task 3
 *
 * Purpose:
 *   Reducer for the MaxTemperature MapReduce job. For each year it receives
 *   an iterable of air temperatures (Celsius * 10) and emits (year, maxTemp).
 *
 * Source:
 *   Provided in the Project 5 README. Originally from "Hadoop: The Definitive
 *   Guide, Second Edition" by Tom White, 2011.
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

public class MaxTemperatureReducer extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> output,
                       Reporter reporter) throws IOException {

        // Walk the iterator of temperatures for this key (year) and keep the max.
        int maxValue = Integer.MIN_VALUE;
        while (values.hasNext()) {
            maxValue = Math.max(maxValue, values.next().get());
        }
        // Emit (year, maxTemp).
        output.collect(key, new IntWritable(maxValue));
    }
}
