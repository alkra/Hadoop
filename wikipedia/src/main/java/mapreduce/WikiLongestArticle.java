package mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.text.wikipedia.XmlInputFormat;

public class WikiLongestArticle {
	
	public static class TextArrayWritable extends ArrayWritable {
		public TextArrayWritable () {
			super(Text.class);
		}
	}
	
	public static class LongestArticle extends
			Mapper<Object, Text, IntWritable, TextArrayWritable> {

		private static final String START_DOC = "<text xml:space=\"preserve\">";
		private static final String END_DOC = "</text>";
		private static final Pattern TITLE = Pattern
				.compile("<title>(.*)<\\/title>");
		
		final IntWritable zero = new IntWritable(0);
		final TextArrayWritable resultArray = new TextArrayWritable();			
		final Text[] result = new Text[2];
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get and parse XML data
			String articleXML = value.toString();

			String title = getTitle(articleXML);
			String document = getDocument(articleXML);

			// Send to reducer an array [article_title, article_length]
			result[0] = new Text(title);
			result[1] = new Text(String.valueOf(document.length()));

			resultArray.set(result);
			context.write(zero, resultArray);
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

	public static class LongestArticleReducer extends
			Reducer<IntWritable, TextArrayWritable, Text, IntWritable> {

		// Maximal-length article properties
		int maxLength = 0;
		Text titleLongest = new Text();
		
		public void reduce(IntWritable key, Iterable<TextArrayWritable> values,
				Context context) throws IOException, InterruptedException {
			
			for(TextArrayWritable articleArray : values) {
				Writable[] article = articleArray.get();
				
				Text lengthText = (Text) article[1];
				int length = Integer.parseInt(lengthText.toString());
				
				// Update maximum
				if (length > maxLength) {
					titleLongest = (Text) article[0];
					maxLength = length;
				}
			}
			context.write(titleLongest, new IntWritable(maxLength));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		Job job = Job
				.getInstance(conf, "WikiLongestArticle");
		job.setJarByClass(WikiFirstTitleLetterDocumentLengthSum.class);

		// Input / Mapper
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapperClass(LongestArticle.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TextArrayWritable.class);

		// Output / Reducer
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(LongestArticleReducer.class);
		job.setNumReduceTasks(4);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}