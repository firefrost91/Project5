/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 1, Task 3
 *
 * Purpose:
 *   Mapper for the MaxTemperature MapReduce job. Parses weather readings
 *   from the NCDC combinedYears.txt file. The mapper pulls the year from
 *   positions 15-18 and the air temperature (Celsius * 10) from positions
 *   87-92. Only readings with a non-missing value and an acceptable quality
 *   code [01459] are emitted.
 *
 *   Output intermediate pair: (year, airTemperature)
 *
 * Source:
 *   Provided in the Project 5 README. Originally from "Hadoop: The Definitive
 *   Guide, Second Edition" by Tom White, 2011.
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

public class MaxTemperatureMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, IntWritable> {

    // Sentinel value used by NCDC to mean "no reading".
    private static final int MISSING = 9999;

    public void map(LongWritable key, Text value,
                    OutputCollector<Text, IntWritable> output,
                    Reporter reporter) throws IOException {

        // Convert the Hadoop Text object into a Java String.
        String line = value.toString();

        // Year is at a fixed position within the line (columns 15-18).
        String year = line.substring(15, 19);

        // Temperature is at positions 87-92. The first character is the sign.
        // parseInt() cannot handle a leading '+', so strip it when present.
        int airTemperature;
        if (line.charAt(87) == '+') {
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }

        // A quality code of 0, 1, 4, 5, or 9 is acceptable per NCDC docs.
        String quality = line.substring(92, 93);
        if (airTemperature != MISSING && quality.matches("[01459]")) {
            // Emit (year, airTemperature). Reducer will see an Iterable of temps per year.
            output.collect(new Text(year), new IntWritable(airTemperature));
        }
    }
}
