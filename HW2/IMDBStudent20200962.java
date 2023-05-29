import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20200962
{
	public static class Movie{
       		public String title;
        	public double rating;

        	public Movie(String title, double rating){
        	        this.title = title;
        	        this.rating = rating;
        	}
	}

	public static class MovieComparator implements Comparator<Movie>{
		public int compare(Movie x, Movie y){
               		if(x.rating > y.rating) return 1;
               		else if(x.rating < y.rating) return -1;
                	return 0;
        	}	
	}
	
	public static void insertMovie(PriorityQueue q, String title, double rating, int topK){
		Movie head = (Movie) q.peek();
		if(q.size() < topK || head.rating < rating){
			Movie movie = new Movie(title, rating);
			q.add(movie);
			if(q.size() > topK) q.remove();
		}
	}
	
	public static class IMDBMapper extends Mapper<Object, Text, Text, Text>
	{
		boolean movieFile = true;
		Text outputKey = new Text();
		Text outputValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{	
			String[] data = value.toString().split("::");

			if( movieFile ) {
				String id = data[0];
				String title = data[1];
				String genreStr = data[2];
				StringTokenizer itr = new StringTokenizer(genreStr, "|");
				
				boolean isFantasy = false;
				while(itr.hasMoreTokens()){
					if(itr.nextToken().equals("Fantasy"))
						isFantasy = true;
				}
				
				if(isFantasy){
					outputKey.set(id);
					outputValue.set("title:" + title);
					context.write(outputKey, outputValue);
				}
			}
			else {  // rating file
				String id = data[1];
                                String rating = data[2];
		
				outputKey.set(id);
				outputValue.set("rating:" + rating);
				context.write(outputKey, outputValue);
			}
		}
		
		protected void setup(Context context) throws IOException, InterruptedException
		{
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
	
			if ( filename.indexOf( "movies.dat" ) != -1 ) movieFile = true;
			else movieFile = false;
		}
	}

	public static class IMDBReducer extends Reducer<Text,Text,Text,DoubleWritable>
	{
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;
		Text outputKey = new Text();
		DoubleWritable outputValue = new DoubleWritable();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			String title = "";
			double rating = 0;
			int count = 0;

			for (Text val : values) {
				if(val.toString().split(":")[0].equals("title")){
					title = val.toString();
				}
				else {	
					rating += Integer.parseInt(val.toString().split(":")[1]);
					count++;
				}
			}
			
			rating = rating / (double) count;
			insertMovie(queue, title, rating, topK);
		}

		public void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Movie>(topK , comp);
		}

		public void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0){
				Movie movie = (Movie) queue.remove();
				outputKey.set(movie.title);
				outputValue.set(movie.rating);
				context.write(outputKey, outputValue);
			}
		}	
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3){
			System.err.println("Usage: IMDBStudent20200962 <in> <out> <topK>");
			System.exit(2);
		}

		int topK = Integer.parseInt(otherArgs[2]);
		conf.setInt("topK", topK);
		Job job = new Job(conf, "IMDB");
		job.setJarByClass(IMDBStudent20200962.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
