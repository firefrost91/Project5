/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 1, Task 7
 *
 * Purpose:
 *   Read CrimeLatLonXYTabs.txt, find every aggravated-assault incident that
 *   occurred within 100 meters of 3803 Forbes Avenue in Oakland, and produce
 *   a single KML file that can be opened in Google Earth. Each qualifying
 *   crime becomes a <Placemark> with its latitude/longitude coordinates.
 *
 *   Schema (tab delimited):
 *     col 0: X (State Plane, feet)
 *     col 1: Y (State Plane, feet)
 *     col 2: time
 *     col 3: street address
 *     col 4: offense type
 *     col 5: date
 *     col 6: 2000 census tract
 *     col 7: latitude
 *     col 8: longitude
 *
 * Strategy:
 *   Mapper: Parse X, Y, offense, latitude, longitude. If it is an
 *           aggravated assault within 100 m of 3803 Forbes, emit a single
 *           shared key with the value "lat,lon,street,date".
 *   Reducer: Write the KML header, one <Placemark> per incident, and the
 *           KML footer as output records. Using a single reducer ensures
 *           the output is one well-formed KML file when merged.
 *
 * This task must be completed WITHOUT any external sources per the README.
 */
package org.myorg;

import java.io.IOException;

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

public class OaklandCrimeStatsKML extends Configured implements Tool {

    // Reference point — 3803 Forbes Avenue, State Plane, in feet.
    private static final double REF_X = 1354326.897;
    private static final double REF_Y = 411447.7828;
    private static final double FEET_TO_METERS = 0.3048;
    private static final double THRESHOLD_METERS = 100.0;

    // Mapper: emit (sharedKey, "lat,lon,address,date") for each qualifying crime.
    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
        private final static Text OUT_KEY = new Text("oakland");
        private Text out = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            // Must have X, Y, offense (col 4), latitude (col 7), longitude (col 8).
            if (fields.length < 9) {
                return;
            }

            String offense = fields[4].trim().toLowerCase();
            if (!offense.equals("aggravated assault")) {
                return;
            }

            double x;
            double y;
            try {
                x = Double.parseDouble(fields[0].trim());
                y = Double.parseDouble(fields[1].trim());
            } catch (NumberFormatException e) {
                return;
            }

            double dx = x - REF_X;
            double dy = y - REF_Y;
            double distanceMeters = Math.sqrt(dx * dx + dy * dy) * FEET_TO_METERS;
            if (distanceMeters >= THRESHOLD_METERS) {
                return;
            }

            // Latitude and longitude come out of the file as strings; validate
            // they're numeric before emitting so the KML never contains junk.
            String lat = fields[7].trim();
            String lon = fields[8].trim();
            try {
                Double.parseDouble(lat);
                Double.parseDouble(lon);
            } catch (NumberFormatException e) {
                return;
            }

            // Pipe-separated to avoid clashes with commas or tabs in the street address.
            String street = fields[3].trim();
            String date = fields[5].trim();
            out.set(lat + "|" + lon + "|" + street + "|" + date);
            context.write(OUT_KEY, out);
        }
    }

    // Reducer: emit KML header, one Placemark per incident, and the footer.
    // The output format writes "key\tvalue\n" but for KML we want just the
    // XML lines, so we emit NullWritable keys and put the XML in the value.
    public static class ReduceClass extends Reducer<Text, Text, NullWritable, Text> {
        private Text out = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // KML header.
            out.set("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            context.write(NullWritable.get(), out);
            out.set("<kml xmlns=\"http://www.opengis.net/kml/2.2\">");
            context.write(NullWritable.get(), out);
            out.set("  <Document>");
            context.write(NullWritable.get(), out);

            int idx = 0;
            for (Text v : values) {
                String[] parts = v.toString().split("\\|", -1);
                // lat|lon|street|date
                String lat = parts[0];
                String lon = parts[1];
                String street = parts.length > 2 ? parts[2] : "";
                String date = parts.length > 3 ? parts[3] : "";
                idx++;

                out.set("    <Placemark>");
                context.write(NullWritable.get(), out);
                out.set("      <name>Aggravated Assault " + idx + "</name>");
                context.write(NullWritable.get(), out);
                out.set("      <description>" + xmlEscape(street) + " on " + xmlEscape(date) + "</description>");
                context.write(NullWritable.get(), out);
                out.set("      <Point>");
                context.write(NullWritable.get(), out);
                // KML coordinates are longitude,latitude,altitude — NOT lat,lon.
                out.set("        <coordinates>" + lon + "," + lat + ",0</coordinates>");
                context.write(NullWritable.get(), out);
                out.set("      </Point>");
                context.write(NullWritable.get(), out);
                out.set("    </Placemark>");
                context.write(NullWritable.get(), out);
            }

            // KML footer.
            out.set("  </Document>");
            context.write(NullWritable.get(), out);
            out.set("</kml>");
            context.write(NullWritable.get(), out);
        }

        // Minimal XML escaping for the fields we embed in the description.
        private static String xmlEscape(String s) {
            if (s == null) return "";
            return s.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace("\"", "&quot;")
                    .replace("'", "&apos;");
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "oaklandCrimeStatsKML");
        job.setJarByClass(OaklandCrimeStatsKML.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        // Single reducer so the resulting part-r-00000 is a single, valid KML.
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OaklandCrimeStatsKML(), args);
        System.exit(res);
    }
}
