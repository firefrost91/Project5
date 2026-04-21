# Part 2 - Spark ShakespeareAnalytics

Run inside IntelliJ with a JDK 8 compiler (Spark + JDK 17 has known issues;
see Lab 9). Add Spark as a Maven/Gradle dependency.

## Steps
1. Download `AllsWellThatEndsWell.txt` to the project working directory
   (http://www.andrew.cmu.edu/course/95-702/homework/data/SparkDataFiles/AllsWellThatEndsWell.txt).
2. Configure IntelliJ's Run configuration so that the working directory is
   the one containing `AllsWellThatEndsWell.txt`.
3. Run `ShakespeareAnalytics` from within IntelliJ.
4. When prompted, enter `love` at the Task 6 prompt. The output window will
   include the counts from Tasks 0-5 and every line containing the substring
   "love" (case-sensitive), interspersed with Spark INFO log lines.
5. Copy-and-paste the entire IntelliJ Run window output into the PDF
   submission (Info lines + Task output + Task 6 matches).
