/**
 * Author: Student
 * Course: 95-702 Distributed Systems for Information Systems Management
 * Project 5, Part 2 – Spark Analytics over "All's Well That Ends Well"
 *
 * Purpose:
 *   A single Java Spark program that reads Shakespeare's "All's Well That
 *   Ends Well" as an RDD of lines and prints six analytics (Tasks 0-5)
 *   plus a final interactive lookup (Task 6). Every task's result is
 *   printed with System.out.println so it appears alongside the Spark INFO
 *   log lines in the IntelliJ Run window.
 *
 *   Task 0: Number of lines          — JavaRDD.count()
 *   Task 1: Number of words          — split on "[^a-zA-Z]+", flatMap, filter, count
 *   Task 2: Number of distinct words — .distinct().count() on the words RDD
 *   Task 3: Number of symbols        — split on "" and flatMap, then count
 *   Task 4: Number of distinct symbols — .distinct().count() on the symbols RDD
 *   Task 5: Number of distinct letters — filter symbols to a-zA-Z, then distinct/count
 *   Task 6: Interactive — prompt for a word and print every line that contains
 *           the word as a substring (case-sensitive).
 *
 * Citations:
 *   - Lab 9 (CMU-Heinz-95702/lab9-MapReduceAndSpark) for IntelliJ Spark setup.
 *   - The Spark Java API docs (org.apache.spark.api.java.*) for JavaRDD, flatMap, filter.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class ShakespeareAnalytics {

    public static void main(String[] args) {

        // Configure a local Spark context. "local[*]" uses all available cores
        // on the local machine. The application name shows up in Spark logs.
        SparkConf conf = new SparkConf()
                .setAppName("ShakespeareAnalytics")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("INFO");

        // The input file lives alongside the project working directory
        // (configured in IntelliJ's Run configuration).
        String inputPath = "AllsWellThatEndsWell.txt";
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Cache the lines RDD because multiple tasks read it.
        lines.cache();

        // -------------- Task 0 : number of lines --------------
        // count() returns the total number of elements in the RDD.
        long numLines = lines.count();
        System.out.println("Task 0 - Number of lines: " + numLines);

        // -------------- Task 1 : number of words --------------
        // Split each line on one-or-more non-alphabetic characters, flatten
        // the Arrays into a single RDD<String>, drop empty strings (which
        // appear when a line starts or ends with a delimiter), then count.
        FlatMapFunction<String, String> splitWords =
                (String line) -> Arrays.asList(line.split("[^a-zA-Z]+")).iterator();
        Function<String, Boolean> nonEmpty = (String s) -> !s.isEmpty();

        JavaRDD<String> words = lines.flatMap(splitWords).filter(nonEmpty);
        // Cache because Task 2 reuses the words RDD.
        words.cache();
        long numWords = words.count();
        System.out.println("Task 1 - Number of words: " + numWords);

        // -------------- Task 2 : distinct words --------------
        // distinct() deduplicates the RDD; count() returns the distinct count.
        long numDistinctWords = words.distinct().count();
        System.out.println("Task 2 - Number of distinct words: " + numDistinctWords);

        // -------------- Task 3 : number of symbols --------------
        // split("") breaks a line into its individual characters (as 1-char
        // strings). Per the README, Java's split("") does NOT emit a trailing
        // newline when the line ends with one, but it DOES yield the empty
        // line's newline if we read such lines — matching the sample count of 51.
        FlatMapFunction<String, String> splitChars =
                (String line) -> Arrays.asList(line.split("")).iterator();
        JavaRDD<String> symbols = lines.flatMap(splitChars);
        symbols.cache();
        long numSymbols = symbols.count();
        System.out.println("Task 3 - Number of symbols: " + numSymbols);

        // -------------- Task 4 : distinct symbols --------------
        long numDistinctSymbols = symbols.distinct().count();
        System.out.println("Task 4 - Number of distinct symbols: " + numDistinctSymbols);

        // -------------- Task 5 : distinct letters (case-sensitive) --------------
        // A "letter" here is any single ASCII alphabetic character. We filter
        // the symbols RDD to only those strings and then take distinct().
        Function<String, Boolean> isLetter = (String s) ->
                s.length() == 1
                        && ((s.charAt(0) >= 'a' && s.charAt(0) <= 'z')
                            || (s.charAt(0) >= 'A' && s.charAt(0) <= 'Z'));
        long numDistinctLetters = symbols.filter(isLetter).distinct().count();
        System.out.println("Task 5 - Number of distinct letters: " + numDistinctLetters);

        // -------------- Task 6 : interactive line search --------------
        // Prompt for a search term and print every line that contains the
        // term as a substring. The comparison is CASE-SENSITIVE, so "love"
        // in "cloven" matches but "Love" does not.
        Scanner in = new Scanner(System.in);
        System.out.print("Task 6 - Enter a word to search for (case-sensitive): ");
        String needle = in.nextLine();

        // Filter the cached lines RDD by substring containment.
        final String target = needle;
        JavaRDD<String> matches = lines.filter((String ln) -> ln.contains(target));

        // collect() pulls the matches back to the driver so we can print them.
        // For a file of this size this is safe.
        List<String> matchingLines = matches.collect();
        System.out.println("Task 6 - Lines containing \"" + needle + "\" ("
                + matchingLines.size() + " matches):");
        for (String ln : matchingLines) {
            System.out.println(ln);
        }

        in.close();
        sc.close();
    }
}
