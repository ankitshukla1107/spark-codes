package rdd.practice.sparkDataProcessing;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.ImmutableList;

import scala.Tuple2;

public class ClassTopper {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("find-class-topper")
				.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// prepare list of objects
		List<Student> studentList = ImmutableList.of(
				new Student("Ankit","Physics", 80),
				new Student("Ankit", "Chemistry", 70),
				new Student("Ankit", "Maths", 100),
				new Student("Arpit","Physics", 85), 
				new Student("Arpit", "Chemistry", 65),
				new Student("Arpit", "Maths", 80));
		
		JavaRDD<Student> studentRDD = sc.parallelize(studentList);
		
		JavaPairRDD<String, Integer> pairRDD = studentRDD.mapToPair(e -> new Tuple2(e.name, e.marks));
		
		JavaPairRDD<String, Integer> nameAndMarksRDD = pairRDD.reduceByKey((x, y) -> x + y).sortByKey(true);
		
		System.out.println(nameAndMarksRDD.first());
		
		sc.close();
	}

}

class Student implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8430735762088325181L;
	String name;
	String subject;
	int marks;

	public Student(String name, String subject, int marks) {
		super();
		this.name = name;
		this.subject = subject;
		this.marks = marks;
	}
}
