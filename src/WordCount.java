import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount
{
	

	public static class PhraseMapper extends
			Mapper<LongWritable, Text, Text, IntWritable>
	{
		private Text sentence = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException
		{

			String sString = value.toString();

			if (sString.length() > 0 && sString.charAt(0) == 'Q')
			{
				sentence.set(sString.substring(1));
				context.write(sentence, new IntWritable(1));
			}

		}
	}

	public static class WordMapper extends
			Mapper<LongWritable, Text, Text, IntWritable>
	{

		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException
		{

			String sString = value.toString();
			
			if (sString.length() > 0 && sString.charAt(0) == 'Q')
			{
				sString = sString.substring(1);

				String words[] = sString.split(" ");

				for (String wString : words)
				{
					word.set(wString);
					context.write(word, new IntWritable(1));
				}
			}

		}
	}
	
	public static class SubPhraseMapper extends
		Mapper<LongWritable, Text, Text, IntWritable>
	{
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
		{
			String sString = value.toString();
			int chunks = 2;
			if (sString.length() > 0 && sString.charAt(0) == 'Q')
			{
				sString = sString.substring(1);
				
				String words[] = sString.split(" ");
				String wString = "";
				for(int i = 0; i < (words.length - chunks + 1); i++)
				{
					wString = "";
					
					for(int j = 0; j < chunks; j++)
						wString = wString + (j == 0 ? words[i+j] : " " + words[i+j]);

					word.set(wString);
					context.write(word, new IntWritable(1));
				}
	
			}
		}
	}
	
	
	
	
	
	public static class PhraseReducer extends
			Reducer<Text, IntWritable, Text, IntWritable>
	{

		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			Iterator<IntWritable> itr = value.iterator();

			while (itr.hasNext())
			{
				sum += itr.next().get();
			}

			context.write(key, new IntWritable(sum));

		}

	}

	public static void main(String[] args) throws Exception
	{
		

	
		
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Phrase Count");
		
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(SubPhraseMapper.class);
		job.setReducerClass(PhraseReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/user/theoden/memtracker"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/user/theoden/memtracker-out"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
