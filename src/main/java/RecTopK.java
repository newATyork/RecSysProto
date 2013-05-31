import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;


// recommend topk results for users who have ratings
public class RecTopK {

	public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text> {
		private final Text itemID = new Text();
		private final Text itemID2 = new Text();
		private HTable comatrixTable;

		public void setup(Context context) throws IOException {
			comatrixTable = new HTable(HBaseConfiguration.create(), "comatrix");
		}	

		@Override
		
		// Input Data:
		// UserID \t itemID,Pref;itemID,Pref;itemID,Pref;
		public void map(Object key, Text value, Context context) 
				throws IOException {
			String[] ss = value.toString().split("\t");
			if(ss.length != 2) {
				//The line does not fit the format and will be skipped;
				return;
			}
			String[] list = ss[1].split(";");
			//  HashSet stores existing songs
			HashSet<Integer> ratedSongs = new HashSet<Integer>();
			for(String pair :list)
			{
				String[] splited = pair.split(",");
				if(splited.length != 2 || splited[0].length() == 0) 
					continue;
				ratedSongs.add(Integer.parseInt(splited[0]));
			}
			
			//  randomly generate 2*n songs
			int len = 2 * ratedSongs.size();
			HashSet<Integer> songsToRec = new HashSet<Integer>();
			while( len != 0 )
			{
				int rec = (int)(Math.random()*498);
				if( !ratedSongs.contains(rec) )
				{
					songsToRec.add(rec); // 498 songs currently in the dataset
					--len;
				}
			}
			
			HashMap<Integer,Integer> prefMap = new HashMap<Integer,Integer>();
			
			String[] strlist = ss[1].split("[,;]");
			for( int i = 0; i < strlist.length; i = i + 2 )
			{
				int tmpItemID = Integer.parseInt( strlist[i] );
				int tmpItemPref = Integer.parseInt( strlist[i+1] );
				prefMap.put( tmpItemID, tmpItemPref );
			}
			
			//  a heap
			PriorityQueue<HeapNode> recHeap = new PriorityQueue<HeapNode>( 40, new Comparator<HeapNode>(){
				//compare word alphabetic order and first docID
				public int compare( HeapNode a, HeapNode b )
				{
					//[Word, DocFrequency, [DocIDs, Frequencies], [DocIDs, Frequencies], [DocIDs, Frequencies]...]
					int score_a = a.score;
					int score_b = b.score;
					if( score_a > score_b )
						return 1;
					else 
						return -1;
				}
			});
			
			//  calculate the score of every song in songToRec
			for(Integer songid: songsToRec)
			{
				String pairsList = null;
				Get get = new Get( songid.toString().getBytes() );
				Result r = comatrixTable.get(get);
				for(KeyValue kv : r.raw())
				{
					pairsList = new String(kv.getValue());
		            System.out.println( pairsList );
		        }
				if(r.raw().length == 0) 
					continue;
				
//				String pairsList = r.raw()[0].getValue().toString();
				String[] coPairs = pairsList.split("[,;]");
//				int[] list2 = new int[coPairs.length];
//				for( int i = 0; i < coPairs.length; ++i )
//					list2[i] = Integer.parseInt( coPairs[i] );
				
				int score = 0;
				for( int i = 0; i < coPairs.length; i = i + 2 )
				{
					int tmpItemID = Integer.parseInt( coPairs[i] );
					if( !prefMap.containsKey( tmpItemID ) )
						continue;
					int tmpItemCocur = Integer.parseInt( coPairs[i+1] );
					int tmpScore = prefMap.get(tmpItemID);
					score += tmpItemCocur * tmpScore;
				}
				System.out.println(score);
				if( recHeap.size() < 40 )	
				{
					HeapNode node = new HeapNode( songid, score );
					recHeap.add(node);
				}
				else if( score > recHeap.peek().score  )
				{
					HeapNode node = new HeapNode( songid, score );
					recHeap.poll();
					recHeap.add(node);
				}
			}
			while( recHeap.size() != 0 )
			{
				HeapNode node = recHeap.poll();
				Text emitKey = new Text( ss[0] );
				Text emitVal = new Text( node.score.toString() );
				try {
					
					context.write( emitKey, emitVal );
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	
	//Maybe we can save the topK results into HBase, but it may be overhead
	public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable>{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		//Iterator or Iterable ???
				throws IOException, InterruptedException 
		{
			SortedMap<String, Integer> ratingCnt = new TreeMap<String, Integer>();
			StringBuffer list = new StringBuffer();
			//String item = new String();
			//int itemCnt = 0;
			System.out.println(key);
			for(Text value : values){
				String tmp = value.toString();
//				if(tmp.isEmpty()) continue;
//
//				if(!ratingCnt.containsKey(tmp))
//					ratingCnt.put(tmp, 1);
//				else
//					ratingCnt.put(tmp, ratingCnt.get(tmp) + 1);
				list.append( value.toString() + "," );
			}
//
//			for(Entry<String, Integer> entry : ratingCnt.entrySet()){
//				list.append(entry.getKey() + "," + entry.getValue() + ";");
//			}
//			ratingCnt.clear();
			
			if(list.length() == 0) return;
			
			ImmutableBytesWritable outputkey = new ImmutableBytesWritable(Bytes.toBytes(key.toString()));
			Put put = new Put(Bytes.toBytes(key.toString()));

			put.add(Bytes.toBytes("recitem"), Bytes.toBytes(""), Bytes.toBytes(list.toString()));
			try {
				context.write(outputkey, put);
			} catch (IOException e) {e.printStackTrace();}
			catch (InterruptedException e) { e.printStackTrace();}
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length == 0){
			System.err.println("Format: Topk");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "get topk recommendation resuls");
		job.setJarByClass(RecTopK.class);

		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		TableMapReduceUtil.initTableReducerJob("recresult", Reduce.class, job);

		boolean jobSucceeded = job.waitForCompletion(true);
		if (jobSucceeded) {
			System.out.println("Job succeeded");
		} else {
			System.out.println("job failed");
		}

	}
}

class HeapNode
{
	public Integer itemID;
	public Integer score;
	
	public HeapNode( int id, int score )
	{
		this.itemID = id;
		this.score = score;
	}
}