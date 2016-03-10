package mapreduce;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.text.wikipedia.XmlInputFormat;

import mapreduce.WikiGreatestContributor.GreatestContributor;

public class WikiWordCount {

	public static class WordCount extends
	Mapper<Object, Text, Text, IntWritable> {

		private static final String START_DOC = "<text xml:space=\"preserve\">";
		private static final String END_DOC = "</text>";
		private static final Pattern TITLE = Pattern
				.compile("<title>(.*)<\\/title>");
		private static final Pattern WORD = Pattern
				.compile("[a-zA-Z0-9_-");

		final IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get and parse XML data
			String articleXML = value.toString();
			
			// Count words in title and article
			wordCount(getTitle(articleXML), context);
			wordCount(getDocument(articleXML), context);
		}
		
		/* Hypothesis: the 100 most common words are English words with Latin characters */
		private void wordCount(String content, Context context) throws IOException, InterruptedException {
			Matcher wordMatcher = WORD.matcher(content);
		
			while(wordMatcher.find()) {
				Text wordMatched = new Text(wordMatcher.group());
	
				context.write(wordMatched, one);
			}
		}

		private static String getDocument(String xml) {
			int start = xml.indexOf(START_DOC) + START_DOC.length();
			int end = xml.indexOf(END_DOC, start);
			return start < end ? xml.substring(start, end) : "";
		}

		private static String getTitle(CharSequence xml) {
			Matcher m = TITLE.matcher(xml);
			return m.find() ? m.group(1) : "";
		}

	}

	public static class WordCountReducer extends
	Reducer<Text, IntWritable, Text, IntWritable> {

		TreeMap<Text, Integer> words = new TreeMap<>();
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int totalCount = 0;
			
			for(IntWritable count : values) {	
				totalCount += count.get();
			}

			context.write(key, new IntWritable(totalCount));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		Job job = Job
				.getInstance(conf, "KM-WikiWordCount");
		job.setJarByClass(WikiFirstTitleLetterDocumentLengthSum.class);

		// Input / Mapper
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapperClass(GreatestContributor.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setCombinerClass(WordCountReducer.class);
		
		// Output / Reducer
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(WordCountReducer.class);
		job.setNumReduceTasks(4);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
}
	
}
