package dataframe.practice.sparkDataProcessing;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import dataframe.practice.sparkDataProcessing.JavaSparkSQLExample.Person;

public class ClassTopper {

	public static class Student implements Serializable {
		private String name;
		private String subject;
		private int marks;

		public String getSubject() {
			return subject;
		}

		public void setSubject(String subject) {
			this.subject = subject;
		}

		public int getMarks() {
			return marks;
		}

		public void setMarks(int marks) {
			this.marks = marks;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("find-class-topper").master("local[2]").getOrCreate();

		// Create an RDD of Student objects from a text file
		JavaRDD<Student> studentRDD = spark.read().textFile(
				"E:\\\\code\\\\git-2018\\\\spark-codes\\\\spark-data-processing\\\\src\\\\main\\\\java\\\\dataframe\\\\practice\\\\sparkDataProcessing\\\\studentdata")
				.javaRDD().map(line -> {
					String[] parts = line.split(",");
					Student student = new Student();
					student.setName(parts[0]);
					student.setSubject(parts[1]);
					student.setMarks(Integer.parseInt(parts[2]));
					return student;
				});

		// Apply a schema to an RDD of JavaBeans to get a DataFrame
		Dataset<Row> studentDF = spark.createDataFrame(studentRDD, Student.class);

		// Register the DataFrame as a temporary view
		studentDF.createOrReplaceTempView("student");

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> studentWiseTotalMarksDF = spark.sql(
				"SELECT name, sum(marks) as totalMarks from student GROUP BY name ORDER BY totalMarks DESC limit 1");
		studentWiseTotalMarksDF.show();

		spark.close();
	}

}
