import java.util.ArrayList; 
import java.util.Arrays; 
import java.util.List; 
import java.util.Map; 
import java.util.Map.Entry; 

import scala.Tuple2;  
import org.apache.commons.lang.StringUtils; 
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD; 
import org.apache.spark.api.java.JavaSparkContext; 
import org.apache.spark.api.java.function.Function; 
import org.apache.spark.api.java.function.PairFunction; 
 
 
public final class KeyValueMapFilter { 
 
 
public static void main(String[] args) throws Exception { 
 		
  
     System.setProperty("hadoop.home.dir", "C:\\spark\\winutils\\");
	 SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local");
	 JavaSparkContext sc = new JavaSparkContext(conf); 
     JavaRDD<String> input = sc.textFile("C:\\spark\\data\\selfishgiant.txt"); 
     PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() { 
       @Override 
       public Tuple2<String, String> call(String x) { 
         return new Tuple2(x.split(" ")[0], x); 
       } 
     }; 
     Function<Tuple2<String, String>, Boolean> longWordFilter = new Function<Tuple2<String, String>, Boolean>() { 
       @Override 
       public Boolean call(Tuple2<String, String> input) { 
         return (input._2().length() < 100); 
       } 
     }; 
     JavaPairRDD<String, String> rdd = input.mapToPair(keyData); 
     //System.out.println("rdd"+rdd.collect());
     JavaPairRDD<String, String> result = rdd.filter(longWordFilter); 
     Map<String, String> resultMap = result.collectAsMap(); 
     for (Entry<String, String> entry : resultMap.entrySet()) { 
       System.out.println(entry.getKey() + ":" + entry.getValue()); 
     } 
 	} 
 } 
