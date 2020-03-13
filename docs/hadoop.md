# Assignment 2: Hadoop

**Question 1: What happens when you run the Hadoop commands in the first part of the tutorial?**
1. **hdfs dfs -put:** copies the local files (e.g. input files) to the containter.
2. **hdfs dfs -get:** copies the files from the containter to the local system (e.g. result files).
3. **hdfs dfs -mkdir:** creates a new directory in the containter.
4. **hdfs dfs -ls:** lists files in the container directory.

** Question 2: How do you use mapreduce to count the number of lines/words/characters/… in the Complete Shakespeare?**

For the assignment, I am counting different character types (i.e. uppercase letters, lowercase letters, numbers and special characters).

The way I do it is by using the same StringTokenizer as in the WordCount example, but then I split this string into characters and classify them by using regular expression matching. Each classified character then emits "one" for a respective class.

Then I use the exact same combiner as in the WordCount example to combine the counts together.

** Question 3: Does Romeo or Juliet appear more often in the plays? Can you answer this question making only one pass over the corpus?**

Yes, it is possible to determine this by making only one pass. I have found that:

* __Romeo__ appears in the corpus 313 times
* __Juliet__ appears in the corpus 206 times

Romeo appears in the corpus more.