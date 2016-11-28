package wikilinks;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Wikilinks {

	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String pg = value.toString(), texttag = "", title = "", lnk;

			title = pg.substring(pg.indexOf("<title>") + 7,
					pg.indexOf("</title>"));
			title = title.replace(" ", "_");
			context.write(new Text(title), new Text("!"));

			int begPos = pg.indexOf("<text");
			int endPos = pg.indexOf("</text>");
			if (begPos != -1 && endPos != -1)
				texttag = pg.substring(begPos, endPos);
			
			Pattern pattern = Pattern.compile("\\[\\[(.*?)(\\||\\]\\])");
			Matcher matcher = pattern.matcher(texttag);
			while (matcher.find()) {

				String match = matcher.group(1);

				if (match != null && !match.isEmpty()) {
					if (match.contains("|")) {
						lnk = match.substring(0, match.indexOf("|")).replace(
								" ", "_");
						if (!lnk.equals(title))
							context.write(new Text(lnk), new Text(title));
					} else {
						lnk = match.replace(" ", "_");
						if (!lnk.equals(title))
							context.write(new Text(lnk), new Text(title));
					}
				}
			}
		}
	};

	public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws InterruptedException, IOException {
			String svalues = value.toString();
			String[] array = svalues.split("\t");
			context.write(new Text(array[0]), new Text(array[1]));
		}
	};

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Set<String> set = new HashSet<String>();
			for (Text val : values) {
				set.add(val.toString());
			}
			if (set.contains("!")) {
				Iterator i = set.iterator();
				while (i.hasNext()) {
					String val = (String) i.next();
					if (!val.equals("!"))
						context.write(new Text(val), key);
					else {
						context.write(key, new Text("!"));
					}
				}
			}
		}
	};

	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			StringBuilder temp = new StringBuilder();
			if (key.toString().equals("!")) {
				while (iter.hasNext()) {
					context.write(new Text(iter.next()), new Text(""));
				}
			} else {
				while (iter.hasNext()) {
					String val = iter.next().toString();
					if (!val.equals("!"))
						temp.append(val + "\t");
				}
				if (temp.length() > 0)
					temp.deleteCharAt(temp.length() - 1);
				context.write(key, new Text(temp.toString()));
			}
		}
	};

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set(XMLInputFormat.START_TAG_KEY, "<page>");
		conf.set(XMLInputFormat.END_TAG_KEY, "</page>");

		Job job = Job.getInstance(conf, "wikilinks1");
		job.setJarByClass(Wikilinks.class);
		job.setInputFormatClass(XMLInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp"));

		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "wikilinks2");
		job2.setJarByClass(Wikilinks.class);

		FileInputFormat.addInputPath(job2, new Path(args[1] + "/temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/graph"));

		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}