import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorCombiner;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorReducer;

// create the user-preference vector from the raw rating file: [uid iid rating]
public class UserVector {

	public static class Map extends MapReduceBase implements
			Mapper<Object, Text, Text, Text> {
		private final Text userID = new Text();
		private final Text itemID = new Text();

		public void map(Object key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] ss = value.toString().split(",");
			if (ss.length != 3)
				return;
			userID.set( ss[0] );
			itemID.set( ss[1] + "," + ss[2] );
			output.collect(userID, itemID);
		}

	}

	// It may not be necessary since the rating in the data file is ordered.
	public static class Combine extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private final Text ratingList = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer list = new StringBuffer();
			while (values.hasNext()) {
				list.append(values.next().toString() + ";"); // [itemID,rating;itemID,rating;]
			}
			ratingList.set(list.toString());
			output.collect(key, ratingList);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private final Text ratingList = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer list = new StringBuffer();
			while (values.hasNext()) {
				list.append(values.next().toString()); // [itemID,rating;itemID,rating;]
			}
			ratingList.set(list.toString());
			output.collect(key, ratingList);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("format: UserVector [rating dir] [output dir]");
			System.exit(-1);
		}
		JobConf conf = new JobConf(UserVector.class);
		conf.setJobName("UserVector");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combine.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}