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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank
{

	public static class PageRankMapper extends
			Mapper<LongWritable, Text, Text, Text>
	{

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException
		{

			LongWritable pivot = new LongWritable();
			float pivotRank = 0f;
			float newRank = 0f;
			String line = value.toString();
			int numTokens;

			/*
			 * The string being parsed by the mapper is of the format
			 * 
			 * ArticleID : ArticleRank : Article1, Article2, Article3,...
			 * 
			 * First, the article ID and rank are tokenized using a tokenizer.
			 * Then, a second tokenizer is used to tokenize the list of
			 * articles.
			 */
			StringTokenizer tokenizer = new StringTokenizer(line, ":");
			numTokens = tokenizer.countTokens();
			pivot = new LongWritable(new Long(tokenizer.nextToken()));
			pivotRank = new Float(tokenizer.nextToken());

			/* The new rank is computed */
			if (numTokens == 0)
			{
				newRank = pivotRank;
			}
			else
			{
				newRank = pivotRank / numTokens;
			}

			line = tokenizer.nextToken();
			tokenizer = new StringTokenizer(line, ",");
			String emitString = ":" + pivotRank + ":";

			while (tokenizer.hasMoreTokens())
			{
				String article = tokenizer.nextToken();
				emitString = emitString + article + ",";

				/*
				 * Each article is emitted as a string of format article, rank
				 * 
				 * the rank of each individual rank is computed as a function of
				 * the total number of articles that also spring from its parent
				 * and the rank of the parent.
				 */
				context.write(new Text(article),
						new Text(Float.toString(newRank)));

			}

			/*
			 * A total list of all articles that are pointed to by an article is
			 * also emitted as a string of format
			 * 
			 * ArticleId : ArticleRank : Article1, Article2, Article3,..
			 * 
			 * the rank of the parent article, ArticleRank is computed in the
			 * Reducer.
			 */
			context.write(new Text(pivot.toString()), new Text(emitString));

		}
	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text>
	{

		/*
		 * 
		 * Reduce function
		 */
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{

			/*
			 * The reducer computes the rank of each parent article based on the
			 * ranks of the articles that spring from it.
			 * 
			 * The string could be either of the format
			 * 
			 * ArticleID : ArticleRank : Article1, Article2, Article3,.. or
			 * 
			 * ArticleName, ArticleRank
			 * 
			 * A regular expression tester is used to distinguish between the
			 * two strings.
			 */
			float sumOfRanks = 0f;
			String listOfArticles = "";
			for (Text value : values)
			{
				String line = value.toString();

				if (line.matches("(.+, \\d+)"))
				{
					String tokens[] = line.split(",");
					sumOfRanks += new Float(tokens[1]);

				}

				else if (line.matches("(.+:\\d:)(.+ \\d)*"))
				{
					listOfArticles = line.split(":")[2];

				}

			}

			/*
			 * A new string of format
			 * 
			 * ArticleID : NewArticleRank : Article1, Article2, Article3,...
			 * 
			 * is emitted, where NewArticleRank is computed as a sum of the
			 * individual ranks of each of the articles that spring from the
			 * article.
			 */
			String emitString = ":" + sumOfRanks + ":" + listOfArticles;

			context.write(key, new Text(emitString));

		}

	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Page Rank");

		job.setJarByClass(PageRank.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(
				"/user/theoden/wiki-pagelink-parsed.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/user/theoden/wiki-pagelink-parsed-out.txt"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
