import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * This program that uses Spark to read The Tempest and perform various calculations, including:
 * Number of lines
 * Number of words
 * Number of distinct words
 * Number of symbols
 * Number of distinct symbols
 * Number of distinct letters
 * It also provides user interaction to let user search for word.
 *
 * Author: Ruidi Chang
 * andrew id: ruidic
 */
public class TempestAnalytics {
    public static void tempest(String fileName, String SearchTerm) {

        // Create a SparkConf object
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Project5Part2");
        // JavaSparkContext
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        // RDD
        JavaRDD<String> input = sparkContext.textFile(fileName);

        // Task 0
        // Map: number of lines
        JavaRDD<String> Lines = input.flatMap(content -> Arrays.asList(content.split("\n")));
        // Reduce: count
        long count0 = Lines.count();

        // Task 1
        // Map: number of words
        JavaRDD<String> Words = input.flatMap(content -> Arrays.asList(content.split("[^a-zA-Z]+"))).filter(word -> (!word.isEmpty()));
        // Reduce: count
        long count1 = Words.count();

        // Task 2
        // Map: Number of distinct words
        JavaRDD<String> DistinctWords = Words.distinct();
        // Reduce: count
        long count2 = DistinctWords.count();

        // Task 3
        // Map: number of symbols
        JavaRDD<String> Symbol = input.flatMap(content -> Arrays.asList(content.split("")));
        // Reduce: count
        long count3 = Symbol.count();

        // Task 4
        // Map: Number of distinct symbol
        JavaRDD<String> DistinctSymbol = Symbol.distinct();
        // Reduce: count
        long count4 = DistinctSymbol.count();

        //Task5
        // Map: number of distinct letter
        JavaRDD<String> DistinctLetters = Symbol.filter(l -> l.matches("^[a-zA-Z]+")).distinct();
        // Reduce: count
        long count5 = DistinctLetters.count();

        // Task6
        // Map: search word
        JavaRDD<String> Result = Lines.filter(l -> l.contains(SearchTerm));
        // Reduce: collect
        List<String> result = Result.collect();

        System.out.println("Number of lines: " + count0);
        System.out.println("Number of words: " + count1);
        System.out.println("Number of distinct words: " + count2);
        System.out.println("Number of symbols: " + count3);
        System.out.println("Number of distinct symbols: " + count4);
        System.out.println("Number of distinct letters: " + count5);
        System.out.println("lines that contains " + SearchTerm);
        for (String l : result) {
            System.out.println(l);
        }
    }
}