package rdd.practice.sparkDataProcessing;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class JavaWordCount {

	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) throws Exception {
		// Create a Java Spark Context.
		JavaSparkContext sc = SparkContextCreator.getJavaSparkContext();
		// Load our input data.
		JavaRDD<String> lines = sc
				.textFile("E:\\code\\git-2018\\spark-codes\\spark-data-processing\\src\\main\\java\\rdd\\practice\\sparkDataProcessing\\inputdata");

/*		// Split up into words.
		JavaRDD<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterator<String> call(String s) {
						return Arrays.asList(SPACE.split(s)).iterator();
					}
				});

		JavaPairRDD<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<>(s, 1);
					}
				});

		JavaPairRDD<String, Integer> counts = pairs
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});
*/
		
		JavaPairRDD<String, Integer> counts = lines.flatMap(e->Arrays.asList(e.split(" ")).iterator()).mapToPair(x->new Tuple2<>(x,1)).reduceByKey((x,y)->x+y);
		List<Tuple2<String, Integer>> lst = counts.collect();
		for (Tuple2<String, Integer> tuple2 : lst) {
			System.out.println("word: " + tuple2._1() + " count: "
					+ tuple2._2());
		}
		System.out.println("word count saved successfully...");
	}

}
