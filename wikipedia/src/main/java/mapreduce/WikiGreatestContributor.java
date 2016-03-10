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

public class WikiGreatestContributor {
	
	public static class GreatestContributor extends
			Mapper<Object, Text, Text, IntWritable> {

		private static final String START_PAGE = "<page>";
		private static final String END_PAGE = "</page>";
		private static final Pattern USERNAME = Pattern
				.compile("<contributor><username>(.*)<\\/username><\\/contributor>");
		private static final Pattern IP = Pattern
				.compile("<contributor><ip>(.*)<\\/ip><\\/contributor>");
		
		final IntWritable one = new IntWritable(1);
		final Text contributor = new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get and parse XML data
			String articleXML = value.toString();

			String contributorId = getContributor(articleXML);

			contributor.set(contributorId);
			context.write(contributor, one);
		}

		private static String getPage(String xml) {
			int start = xml.indexOf(START_PAGE) + START_PAGE.length();
			int end = xml.indexOf(END_PAGE, start);
			return start < end ? xml.substring(start, end) : "";
		}

		private static String getContributor(CharSequence xml) {
			String result = "";
			
			Matcher userNameMatcher = USERNAME.matcher(xml);
			if(userNameMatcher.find()) {
				result = userNameMatcher.group(1);
			}
			/*else {
				Matcher ipMatcher = IP.matcher(xml);
				if(ipMatcher.find()) {
					result = ipMatcher.group(1);
				}
			}*/
				
			return result;
		}

	}

	public static class GreatestContributorReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		// Maximal-length article properties
		int maxContribution = 0;
		Text contributor = new Text();
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			int contributions = 0;
			
			for(@SuppressWarnings("unused") IntWritable unitContribution : values) {	
				contributions++;
			}
				
			// Update maximum
			if (contributions > maxContribution) {
				contributor = key;
				maxContribution = contributions;
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(contributor, new IntWritable(maxContribution));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		Job job = Job
				.getInstance(conf, "KM-WikiGreatestContributor");
		job.setJarByClass(WikiFirstTitleLetterDocumentLengthSum.class);

		// Input / Mapper
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapperClass(GreatestContributor.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Output / Reducer
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(GreatestContributorReducer.class);
		job.setNumReduceTasks(4);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}