import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

// create the co-occurrence matrix from the user vector file: [uid\tiid,rating;iid2,rating;]
public class COMatrix {

	public static class Mapper extends
			org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text> 
	{
		private final Text itemID = new Text();
		private final Text itemID2 = new Text();

		@Override
		// Input Data:
		// UserID \t ItemID,Pref;ItemID,Pref;ItemID,Pref;
		// Emit Data:
		// EVERY co-currence pair (itemID1,itemID2)
		public void map(Object key, Text value, Context context) throws IOException 
		{
			String[] ss = value.toString().split("\t");
			if (ss.length != 2) 
			{
				System.out.println("The line does not fit the format and skip: "+ value.toString());
				return;
			}
			String[] list = ss[1].split(";");
			for (int i = 0; i < list.length - 1; ++i) 
			{
				for (int j = i + 1; j < list.length; ++j) 
				{
					if (list[i] == "" || list[j] == ""
							|| list[i].split(",").length != 2
							|| list[j].split(",").length != 2)
						break;

					itemID.set(list[i].split(",")[0]);
					itemID2.set(list[j].split(",")[0]);
					try {
						context.write(itemID, itemID2);
						context.write(itemID2, itemID);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	// It may not be necessary since the rating in the data file is ordered.
	// ERROR here.
	public static class Combine extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private final Text ratingList = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer list = new StringBuffer();
			while (values.hasNext()) {
				if (values.next().toString() != "")
					list.append(values.next().toString()); // [itemID
															// itemID,itemID2,]
			}
			ratingList.set(list.toString());
			output.collect(key, ratingList);
		}
	}

	// Output Data:
	// Store the 
	public static class Reduce extends
			TableReducer<Text, Text, ImmutableBytesWritable> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		// Iterator or Iterable ???
				throws IOException, InterruptedException
		{
			SortedMap<String, Integer> ratingCnt = new TreeMap<String, Integer>();
			StringBuffer list = new StringBuffer();
			// String item = new String();
			// int itemCnt = 0;
			System.out.println(key);
			// Calculate the times every item occur with item(key)
			for (Text value : values) {
				String tmp = value.toString();

				if (tmp.isEmpty())
					continue;

				if (!ratingCnt.containsKey(tmp))
					ratingCnt.put(tmp, 1);
				else
					ratingCnt.put(tmp, ratingCnt.get(tmp) + 1);
			}

			// construct String list according to Map "ratingCnt"
			for (Entry<String, Integer> entry : ratingCnt.entrySet()) {
				list.append(entry.getKey() + "," + entry.getValue() + ";");
			}
			ratingCnt.clear();
			if (list.length() == 0)
				return;
			ImmutableBytesWritable outputkey = new ImmutableBytesWritable(
					Bytes.toBytes(key.toString()));
			Put put = new Put(Bytes.toBytes(key.toString()));

			put.add(Bytes.toBytes("list"), Bytes.toBytes(""),
					Bytes.toBytes(list.toString()));
			try {
				context.write(outputkey, put);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		
		if (args.length == 0) {
			System.err.println("Format: CM2HBase [user vector dir] [co-occurance output dir]");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "calculate Co-ocurrance matrix");
		job.setJarByClass(COMatrix.class);

		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		TableMapReduceUtil.initTableReducerJob("comatrix", Reduce.class, job);

		boolean jobSucceeded = job.waitForCompletion(true);
		if (jobSucceeded) {
			System.out.println("Job succeeded");
		} else {
			System.out.println("job failed");
		}

	}
}