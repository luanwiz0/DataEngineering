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
	public static class MovieData{
       		public String title;
        	public int rating;

        	public MovieData(String title, int rating){
        	        this.title = title;
        	        this.rating = rating;
        	}
	
	        public String toString(){
	                return title + " " + rating;
	        }
	}

	public static class DoubleString implements WritableComparable
	{
		String joinKey = new String();
		String tableName = new String();

		public DoubleString() {}
		public DoubleString( String _joinKey, String _tableName )
		{
			joinKey = _joinKey;
			tableName = _tableName;
		}

		public void readFields(DataInput in) throws IOException
		{
			joinKey = in.readUTF();
			tableName = in.readUTF();
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(joinKey);
			out.writeUTF(tableName);
		}

		public int compareTo(Object o1)
		{
			DoubleString o = (DoubleString) o1;
			int ret = joinKey.compareTo( o.joinKey );
			if (ret!=0) return ret;
			return tableName.compareTo( o.tableName);
		}

		public String toString() { return joinKey + " " + tableName; }
	}

	public static class CompositeKeyComparator extends WritableComparator 
	{
		protected CompositeKeyComparator() {
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
			
			int result = k1.joinKey.compareTo(k2.joinKey);
			if(0 == result) {
				result = k1.tableName.compareTo(k2.tableName);
			}
			return result;
		}	
	}

	public static class FirstPartitioner extends Partitioner<DoubleString, Text>
	{
		public int getPartition(DoubleString key, Text value, int numPartition)
		{
			return key.joinKey.hashCode()%numPartition;
		}
	}

	public static class FirstGroupingComparator extends WritableComparator 
	{
		protected FirstGroupingComparator(){
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;

			return k1.joinKey.compareTo(k2.joinKey);
		}
	}

	public static class MovieComparator implements Comparator<MovieData>{
		public int compare(MovieData x, MovieData y){
               		if(x.rating > y.rating) return 1;
               		else if(x.rating < y.rating) return -1;
                	return 0;
        	}	
	}
	
	public static void insertMovie(PriorityQueue q, String title, int rating, int topK){
		MovieData head = (MovieData) q.peek();
		if(q.size() < topK || head.rating < rating){
			MovieData movie = new MovieData(title, rating);
			q.add(movie);
			if(q.size() > topK) q.remove();
		}
	}
	
	public static class IMDBMapper extends Mapper<Object, Text, DoubleString, Text>
	{
		boolean movieFile = true;
		Text outputValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{	
			String[] data = value.toString().split("::");

			if( movieFile ) {
				String id = data[0];
				String title = data[1];
				String genreStr = data[2];
				DoubleString joinKey = new DoubleString(id, "M");
				
				boolean isFantasy = false;
				for(String genre : genreStr.split("|")){
					if(genre.equals("Fantasy")){
						isFantasy = true;
						break;
					}
				}
				if(isFantasy){
					outputValue.set("title:" + title);
					context.write(joinKey, outputValue);
				}
			}
			else {  // rating file
				String id = data[1];
                                String rating = data[2];
				DoubleString joinKey = new DoubleString(id, "R");
		
				outputValue.set("rating:" + rating);
				context.write(joinKey, outputValue);
			}
		}
		
		protected void setup(Context context) throws IOException, InterruptedException
		{
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
	
			if ( filename.indexOf( "movies.dat" ) != -1 ) movieFile = true;
			else movieFile = false;
		}
	}

	public static class IMDBReducer extends Reducer<DoubleString,Text,Text,Text>
	{
		private PriorityQueue<MovieData> queue;
		private Comparator<MovieData> comp = new MovieComparator();
		private int topK;

		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			String title = "";
			int rating = 0;
			int count = 0;

			for (Text val : values) {
				if(val.toString().split(":")[1].equals("title")){
					title = val.toString();
				}
				else {	
					rating += Integer.parseInt(val.toString().split(":")[1]);
					count++;
				}
			}
			
			rating = rating / count;
			insertMovie(queue, title, rating, topK);
		}

		public void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<MovieData>(topK, comp);
		}

		public void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0){
				MovieData movieData = (MovieData) queue.remove();
				context.write(new Text(movieData.title), new Text(Integer.toString(movieData.rating)));
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

		int topK = Integer.valueOf(otherArgs[2]);
		conf.setInt("topK", topK);
		Job job = new Job(conf, "IMDB");
		job.setJarByClass(IMDBStudent20200962.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(DoubleString.class);
		job.setMapOutputValueClass(Text.class);
	
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
