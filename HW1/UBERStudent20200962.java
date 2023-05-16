import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20200962 
{

	public static class UBERStudentMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text map_key = new Text();
		private Text map_value = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] data = value.toString().split(",");
		    	String region = data[0];
		    	String date = data[1];
		   	String vehicles = data[2];
		    	String trips = data[3];
		
		 	String[] weekDays = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
           	    	Date day = new Date(date);
		    	String dayStr = weekDays[day.getDay()];
			
		    	map_key.set(region + "," + dayStr);
		    	map_value.set(trips + "," + vehicles);
		    	context.write(map_key, map_value);
		}
	}

	public static class UBERStudentReducer extends Reducer<Text, Text, Text, Text> 
	{
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int vehicles = 0;
			int trips = 0;
			for(Text val : values){
				String[] data = val.toString().split(",");
				trips += Integer.parseInt(data[0]);
				vehicles += Integer.parseInt(data[1]);
			}
			result.set(trips + "," + vehicles);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: UBERStudent20200962 <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBER");
		job.setJarByClass(UBERStudent20200962.class);
		job.setMapperClass(UBERStudentMapper.class);
		job.setCombinerClass(UBERStudentReducer.class);
		job.setReducerClass(UBERStudentReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
