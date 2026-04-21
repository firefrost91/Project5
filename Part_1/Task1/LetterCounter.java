/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 1, Task 1
 *
 * Purpose:
 *   A Hadoop MapReduce program derived from WordCount.java that counts the
 *   total number of occurrences of each individual letter (A-Z, a-z) in the
 *   input file words.txt. The task description notes that the search IS
 *   case-sensitive ("e" is the most frequent letter).
 *
 *   Mapper: For every token in every line, walk the token's characters and
 *           emit (letter, 1) for each alphabetic character.
 *   Reducer: Sums the 1's per letter, emitting (letter, totalCount).
 *
 *   After running the job, merge the reducer output and pipe it through the
 *   standard Linux sort utility (sort -k 2nr) so the most frequent letter
 *   appears first.
 *
 * Citations:
 *   - Based on WordCount.java provided in the Project 5 README.
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

public class LetterCounter extends Configured implements Tool {

    // Mapper: emit (letter, 1) for each alphabetic character in every token.
    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text letter = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Convert the input line to a Java String so we can tokenize it.
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            // The README warns: without nextToken(), the loop is infinite. We call it once per iteration.
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                // Walk each character of the token. Only alphabetic characters are counted.
                for (int i = 0; i < token.length(); i++) {
                    char c = token.charAt(i);
                    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
                        letter.set(String.valueOf(c));
                        context.write(letter, one);
                    }
                }
            }
        }
    }

    // Reducer: sum the 1's for each distinct letter key.
    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Driver: configure and run the job.
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "lettercount");
        job.setJarByClass(LetterCounter.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LetterCounter(), args);
        System.exit(res);
    }
}
