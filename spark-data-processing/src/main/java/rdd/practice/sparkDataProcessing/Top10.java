package rdd.practice.sparkDataProcessing;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class Top10 {

    private static final String DATA_TOP_FILE_NAME = "E:\\code\\git-2018\\spark-codes\\spark-data-processing\\src\\main\\java\\rdd\\practice\\sparkDataProcessing\\tweets-count.txt";

    public static void main(String[] args) throws Exception {

        String inputPath = DATA_TOP_FILE_NAME;

        final JavaSparkContext ctx = SparkContextCreator.getJavaSparkContext();

        // STEP-3: create an RDD for input
        // input record format:
        //  <string-key><,><integer-value>,

        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        // STEP-4: create (K, V) pairs
        // Note: the assumption is that all K's are unique
        // PairFunction<T, K, V>
        // T => Tuple2<K, V>

        JavaPairRDD<String,Integer> pairs = lines.mapToPair((String s) -> {
            String[] tokens = s.split(",");
            System.out.println("Complete word -- " + s);
            return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
        });

        // When a lazy collection is collected, the values are called into the list and
        // can be printed in the standard output console. This can't be done in big parallel
        // processing because it kills the master node.
        List<Tuple2<String,Integer>> debug1 = pairs.collect();
        for (Tuple2<String,Integer> t2 : debug1) {
            System.out.println("key="+t2._1 + "\t value= " + t2._2);
        }

    
        // STEP-5: create a local top-10

        JavaRDD<SortedMap<Integer, String>> partitions = pairs.mapPartitions((Iterator<Tuple2<String,Integer>> iter) -> {
            SortedMap<Integer, String> top10 = new TreeMap<>();
            while (iter.hasNext()) {
                Tuple2<String,Integer> tuple = iter.next();
                top10.put(tuple._2, tuple._1);
                // keep only top N
                if (top10.size() > 10) {
                    top10.remove(top10.firstKey());
                }
            }
            return Collections.singletonList(top10).iterator();
        });

        // STEP-6: find a final top-10
        SortedMap<Integer, String> finaltop10 = new TreeMap<>();

        List<SortedMap<Integer, String>> alltop10 = partitions.collect();

        for (SortedMap<Integer, String> localtop10 : alltop10) {
          //System.out.println(tuple._1 + ": " + tuple._2);
          // weight/count = tuple._1
          // catname/URL = tuple._2
          for (Map.Entry<Integer, String> entry : localtop10.entrySet()) {
              //   System.out.println(entry.getKey() + "--" + entry.getValue());
              finaltop10.put(entry.getKey(), entry.getValue());
              // keep only top 10 
              if (finaltop10.size() > 10) {
                 finaltop10.remove(finaltop10.firstKey());
              }
          }
      }
    
      // STEP_7: emit final top-10
      for (Map.Entry<Integer, String> entry : finaltop10.entrySet()) {
         System.out.println(entry.getKey() + "--" + entry.getValue());
      }
      ctx.close();
      System.exit(0);
   }
}
