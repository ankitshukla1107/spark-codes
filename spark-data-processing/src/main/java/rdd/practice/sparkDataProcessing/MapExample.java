package rdd.practice.sparkDataProcessing;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MapExample {
    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = SparkContextCreator.getJavaSparkContext();
        
        // Parallelized with 2 partitions
        JavaRDD<String> x = sc.parallelize(
                Arrays.asList("spark", "rdd", "example", "sample", "example"),
                2);
        
        // Word Count Map Example
        JavaRDD<Tuple2<String, Integer>> y1 = x.map(e -> new Tuple2<>(e, 1));
        List<Tuple2<String, Integer>> list1 = y1.collect();
        
        for (Tuple2<String, Integer> tuple2 : list1) {
			System.out.println(tuple2._1()+"---"+tuple2._2());
		}
        
        // Another example of making tuple with string and it's length
        JavaRDD<Tuple2<String, Integer>> y2 = x.map(e -> new Tuple2<>(e, e.length()));
        List<Tuple2<String, Integer>> list2 = y2.collect();
        for (Tuple2<String, Integer> tuple2 : list2) {
			System.out.println(tuple2._1()+"---"+tuple2._2());
		}
        sc.close();
    }
}
