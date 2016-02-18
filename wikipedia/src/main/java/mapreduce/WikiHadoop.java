package mapreduce;

import java.io.IOException;
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

public class WikiHadoop {
	
	public static class Hadoop extends
			Mapper<Object, Text, IntWritable, IntWritable> {

		private static final String START_DOC = "<text xml:space=\"preserve\">";
		private static final String END_DOC = "</text>";
		private static final Pattern TITLE = Pattern
				.compile("<title>(.*)<\\/title>");
		
		final IntWritable zero = new IntWritable(0);
		final String hadoop = new String("hadoop");
		final String Hadoop = new String("Hadoop");
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get and parse XML data
			String articleXML = value.toString();

			String title = getTitle(articleXML);
			String document = getDocument(articleXML);

			if(title.indexOf(Hadoop) >= 0
					|| document.indexOf(Hadoop) >= 0
					|| title.indexOf(hadoop) >= 0
					|| document.indexOf(hadoop) >= 0) {
				context.write(zero, zero);
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

	public static class HadoopReducer extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		final IntWritable zero = new IntWritable(0);
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			int count = 0;
			
			for(@SuppressWarnings("unused") IntWritable unit : values) {
				count++;
			}
			
			context.write(zero, new IntWritable(count));
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
		job.setMapperClass(Hadoop.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Output / Reducer
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(HadoopReducer.class);
		job.setNumReduceTasks(4);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}