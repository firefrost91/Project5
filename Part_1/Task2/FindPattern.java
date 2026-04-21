/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 1, Task 2
 *
 * Purpose:
 *   A Hadoop MapReduce program, modeled after WordCount.java, that scans
 *   words.txt and outputs every word containing the substring "fun"
 *   (case-insensitive). Example matches: "afunction", "Fungia", "defunct".
 *
 *   Mapper: For every token in every line, lowercase a copy of the token
 *           and check whether it contains "fun". If so, emit the original
 *           (case-preserved) token as the key with a NullWritable value.
 *   Reducer: A single NullWritable value per key means the reducer simply
 *            writes the matched word with no count.
 *
 * Citations:
 *   - Derived from WordCount.java provided in the Project 5 README.
 */
package org.myorg;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class FindPattern extends Configured implements Tool {

    // Mapper: emit the original word whenever its lowercased form contains "fun".
    public static class MapClass extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            // Call nextToken() exactly once per iteration to avoid an infinite loop.
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                // Case-insensitive substring check via toLowerCase(). The emitted key
                // preserves the original case so, e.g., "Fungia" appears as "Fungia".
                if (token.toLowerCase().contains("fun")) {
                    word.set(token);
                    context.write(word, NullWritable.get());
                }
            }
        }
    }

    // Reducer: each matching word may be emitted multiple times. The reducer
    // writes the key once, eliminating duplicates naturally since the output
    // format defaults to key-then-value (the NullWritable is printed as nothing).
    public static class ReduceClass extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            // Writing the key a single time per reducer invocation removes duplicates.
            context.write(key, NullWritable.get());
        }
    }

    // Driver: configure and run the job.
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "findpattern");
        job.setJarByClass(FindPattern.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FindPattern(), args);
        System.exit(res);
    }
}
