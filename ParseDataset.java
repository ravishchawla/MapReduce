import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class ParseDataset
{

	public static class ParseMapper extends
			Mapper<LongWritable, Text, LongWritable, Text>
	{

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException
		{

			/*
			 * The dataset is in format of an SQL Insertion statement, and each
			 * article and its links is in format
			 * 
			 * (articleID, articleNameSpace, link)
			 * 
			 * A pattern matcher is used to find specific pairs that match this
			 * pattern. Those pairs are then further parsed to get the article
			 * name and rank, and they are emitted as a string of format
			 * 
			 * ArticleID:ArticleRank: Article1, Article2, Article3,...
			 */
			String line = value.toString();
			Pattern pattern = Pattern.compile("\\(([^,]+[,]){2}[^,]+\\)");
			Matcher matcher = pattern.matcher(line);
			String pivot_id;
			String id;
			String[] tokens;
			String emitString = "";

			String pair;

			matcher.find();
			pair = matcher.group();

			pair = pair.substring(1, pair.length() - 1);
			tokens = pair.split(",");

			pivot_id = tokens[0];

			emitString += tokens[2];
			emitString += " 0,";

			/*
			 * For each pair, a pivot is used to find all articles that have the
			 * same parent. each pair is then split based on a delimeter, and a
			 * new emit string is generated and written to contex.
			 */
			while (matcher.find() /* and pivot does not change */)
			{

				pair = matcher.group();
				pair = pair.substring(1, pair.length() - 1);
				tokens = pair.split(",");

				id = tokens[0];

				if (!id.equals(pivot_id))
				{
					emitString = ":0:"
							+ emitString.substring(0, emitString.length() - 1);
					context.write(key, new Text(emitString));

					pivot_id = id;
					emitString = "";
				}

				String article = "";

				article = tokens[2];
				emitString += article;
				emitString += ",";

			}

		}
	}

	public static class ParseReducer extends
			Reducer<LongWritable, Text, LongWritable, Text>
	{

		public void reduce(LongWritable key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException
		{

			/*
			 * The reducer simply appends the list of articles that correspond
			 * to the same key to each other, to emit a string that contains the
			 * id, its rank, and all the articles that spring from it.
			 */

			Iterator<Text> itr = value.iterator();

			String baseString = itr.next().toString();

			while (itr.hasNext())
			{
				baseString += itr.toString().split(":")[2];

			}

			context.write(key, new Text(baseString));

		}

	}

	public static void main(String[] args) throws Exception
	{

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Parse Dataset");

		job.setJarByClass(ParseDataset.class);
		job.setMapperClass(ParseMapper.class);
		job.setReducerClass(ParseReducer.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(
				"/user/theoden/wiki-pagelink.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/user/theoden/wiki-pagelink-parsed.txt"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
