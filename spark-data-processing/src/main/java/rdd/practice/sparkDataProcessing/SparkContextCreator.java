package rdd.practice.sparkDataProcessing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContextCreator {

	private static JavaSparkContext javaSparkContext = null;

	public static JavaSparkContext getJavaSparkContext() {
		SparkConf sparkConf = new SparkConf().setAppName("sampleSparkApp").setMaster(
				"local[2]");
		javaSparkContext = new JavaSparkContext(sparkConf);
		return javaSparkContext;
	}
}
