import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.*;

public final class UBERStudent20200962 {
	public static void main(String[] args) throws Exception {
		SparkSession spark = SparkSession
			.builder()
			.appName("UBERStudent20200962")
			.getOrCreate();

		String[] weekDays = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
   
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();


		PairFunction<String, String, String> pf = new PairFunction<String, String, String>(){
			public Tuple2<String, String> call (String s){
				String[] data = s.toString().split(",");
		    String region = data[0];
		    String date = data[1];
		   	String vehicles = data[2];
		    String trips = data[3];

				Date day = new Date(date);
				String dayStr = weekDays[day.getDay()];
				return new Tuple2(region + "," + dayStr , trips + "," + vehicles);
			}
		};
		JavaPairRDD<String, String> tuples = lines.mapToPair(pf);

		Function2<String, String, String> f2 = new Function2<String, String, String>(){
			public String call (String x, String y){
				String[] dataX = x.toString().split(",");
				String[] dataY = y.toString().split(",");
		
				int trips = Integer.parseInt(dataX[0]) + Integer.parseInt(dataY[0]);
				int vehicles = Integer.parseInt(dataX[1]) + Integer.parseInt(dataY[1]);
				return trips + "," + vehicles;
			}
		};
		JavaPairRDD<String, String> counts = tuples.reduceByKey(f2);

		counts.saveAsTextFile(args[1]);
	}
}
