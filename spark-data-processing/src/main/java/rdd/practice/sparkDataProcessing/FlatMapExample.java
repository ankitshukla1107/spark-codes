package rdd.practice.sparkDataProcessing;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class FlatMapExample {
	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = SparkContextCreator.getJavaSparkContext();

		// Parallelized with 2 partitions
		JavaRDD<String> rddX = sc.parallelize(
				Arrays.asList("spark rdd example", "sample example"), 2);

		// map operation will return List of Array in following case
		JavaRDD<String[]> rddY = rddX.map(e -> e.split(" "));
		List<String[]> listUsingMap = rddY.collect();

		// flatMap operation will return list of String in following case
		/*JavaRDD<String> rddY2 = rddX.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s){
				return Arrays.asList(s.split(" ")).iterator();
			}
			
		});*/
		JavaRDD<String> rddY2 = rddX.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
		List<String> listUsingFlatMap = rddY2.collect();
		for (String currentWord : listUsingFlatMap) {
			System.out.println(currentWord);
		}
		sc.close();
	}
}
