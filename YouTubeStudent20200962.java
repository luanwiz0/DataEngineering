public class YouTubeStudent20200962{

	public class Youtube {
		public String category;
		public double rating;

		public Youtube(String category, double rating){
			this.category = category;
			this.rating = rating;
		}

		public String toString(){
			return category + " " + rating;
		}
	}

	public static class youtubeComparator implements Comparator<Youtube>{
		public int compare(Youtube x, Yotube y){
			if(x.rating > y.rating) return 1;
			else if(x.rating < y.rating) return -1;
			return 0;
		}
	}

	public static void insertYoutube(PriorityQueue q, String category, double rating, int topK){
		Youtube  head = (Youtube) q.peek();
		if ( q.size() < topK || head.rating < rating ){
			Youtube y = new Emp(category, rating);
			q.add(y);
			if( q.size() > topK ) q.remove();
		}
	}

	public static class TopKMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split("|");
			String category = data[3];
			double rating = Double.parseDouble(data[6]);
			context.write(new Text(category), new DoubleWritable(rating));
		}
	}

	public static class TopKReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		private PriorityQueue<Emp> queue ;
		private Comparator<Emp> comp = new EmpComparator();
		private int topK;

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
			double rating = 0;
			int count = 0;
			
			for(DoubleWritable val : values){
				rating += val.get();
				count++;
			}
			rating = rating / (double) count;
			insertYoutube(queue, key.toString(), rating, topK);
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Youtube>( topK , comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while( queue.size() != 0 ) {
				Youtube y = (Youtube) queue.remove();
				context.write(new Text(y.category), new DoubleWritable(y.rating));
			}
		}
	}
			
	
}
