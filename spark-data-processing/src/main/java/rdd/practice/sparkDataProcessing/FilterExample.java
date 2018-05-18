package rdd.practice.sparkDataProcessing;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class FilterExample {
	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = SparkContextCreator.getJavaSparkContext();

		// Filter Predicate
		Function<Integer, Boolean> filterPredicate = e -> e % 2 == 0;

		// Parallelized with 2 partitions
		JavaRDD<Integer> rddX = sc.parallelize(
				Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);

		// filter operation will return List of Array in following case
		JavaRDD<Integer> rddY = rddX.filter(e->e%2==0);
		List<Integer> filteredList = rddY.collect();
		for (Integer currentElement : filteredList) {
			System.out.println(currentElement);
		}
		sc.close();
	}
}
