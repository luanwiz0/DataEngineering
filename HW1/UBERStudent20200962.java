import java.io.IOException;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.TextStyle;

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
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			while(itr.hasMoreTokens())
			{
				String local = itr.nextToken();
				String[] dateString = itr.nextToken().split("/");
				LocalDate date = LocalDate.of(Integer.parseInt(dateString[2]), 
						Integer.parseInt(dateString[0]), Integer.parseInt(dateString[1]));
				DayOfWeek dayOfWeek = date.getDayOfWeek();
				String day = dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US).toUpperCase();

				map_key.set(local + "," + day);
				map_value.set(itr.nextToken() + "," + itr.nextToken());
				context.write(map_key, map_value);
			}
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
				vehicles += Integer.parseInt(data[0]);
				trips += Integer.parseInt(data[1]);
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
