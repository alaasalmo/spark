import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class  mapreduce {

      private  static  final  JavaSparkContext sc  = new  JavaSparkContext(new  SparkConf().setAppName("SparkJdbcDs").setMaster("local"));
       public  static  void  main(String[] args) {

 

      //1. Read Test file
      JavaRDD<String> lines = sc .textFile("mapReduce.txt");

      //2. Map each line to multiple words
      JavaRDD<String> words = lines.flatMap( new  FlatMapFunction<String, String>() {   
                              public  Iterable<String> call(String line)  
                              {  return  Arrays.asList(line.split(" "));    }});

      // 3. Map: Take the data as words
      // Turn the words into (word, 1) pairs

     JavaPairRDD<String, Integer> maps = words.mapToPair( new  PairFunction<String, String, Integer>() { 
                                               public  Tuple2<String, Integer> call(String w) 
                                               {  return  new  Tuple2<String, Integer>(w, 1);    }});

       

     // 4. Reduce: Add the same words together
     // Group up and add the pairs by key to produce counts
     JavaPairRDD<String, Integer> reduces = maps.reduceByKey( new  Function2<Integer, Integer, Integer>() {  
                                             public  Integer call(Integer i1, Integer i2) 
                                             {      return  i1 + i2;    }});

 

      // 5. Display the Map result
       maps.foreach( new  VoidFunction<Tuple2<String, Integer>> () {
                                                                  public  void  call(Tuple2<String, Integer> tuple) throws  Exception {
                                                                                           System.out .println("[" + tuple._1() + "," + tuple._2() + "]");

       }});

 

      // 6. Display the Reduce result

      reduces.foreach( new  VoidFunction<Tuple2<String, Integer>> () {
                                                                  public  void  call(Tuple2<String, Integer> tuple) throws  Exception {
                                                                                          System.out .println("[" + tuple._1() + "," + tuple._2() + "]");

        }});

      System.out .println("Completed");

    }

}
