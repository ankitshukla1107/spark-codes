package rdd.practice.sparkDataProcessing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AnagramCheck {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Anagram-Checker").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile(
				"E:\\code\\git-2018\\spark-codes\\spark-data-processing\\src\\main\\java\\rdd\\practice\\sparkDataProcessing\\anagram-inputs",
				1);
		//System.out.println(lines.collect());
		List<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();
		JavaPairRDD<String, String> rdd = lines.flatMapToPair(word->{
			char[] wordChars = word.toCharArray(); 
            Arrays.sort(wordChars); 
            String sortedWord = new String(wordChars); 
            results.add(new Tuple2<String,String>(sortedWord, word));
            return (Iterator<Tuple2<String, String>>) results;
		});
		
		List<Tuple2<String, String>> anagramList = rdd.collect();
		for (Tuple2<String, String> tuple2 : anagramList) {
			System.out.println(tuple2._1+"-"+tuple2._2);
		}
		sc.close();
	}

}
