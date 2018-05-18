package rdd.practice.sparkDataProcessing;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONtoRDD {

	public static void main(String[] args) {
		// configure spark
		SparkSession spark = SparkSession.builder()
				.appName("Spark Example - Read JSON to RDD").master("local[2]")
				.getOrCreate();

		System.out.println(spark.sparkContext().appName());
		// read list to RDD
		String jsonPath = "E:/code/personal code/sparkDataProcessing/src/main/java/com/practice/sparkDataProcessing/employees.json";
		JavaRDD<Row> items = spark.read().json(jsonPath).toJavaRDD();

		items.foreach(item -> {
			System.out.println(item);
		});
	}
}