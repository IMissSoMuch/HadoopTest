package hadoop.test.pagerank;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hadoop.test.example.HdfsDAO;
import hadoop.test.pagerank.AdjacencyMatrix.AdjacencyMatrixMapper;
import hadoop.test.pagerank.AdjacencyMatrix.AdjacencyMatrixReducer;

public class PageRank {
	
	public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		private String flag;
		private static int nums = 4;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
			System.out.println(value.toString());
			String[] tokens = PageRankJob.DELIMITER.split(value.toString());
			if(flag.equals("tmp1")){
				String row = value.toString().substring(0, 1);
				String[] vals = PageRankJob.DELIMITER.split(value.toString().substring(2));
				for(int i=0;i<vals.length;i++){
					Text k = new Text(String.valueOf(i+1));
					Text v = new Text(String.valueOf("A:"+(row)+","+vals[i]));
					context.write(k, v);
				}
			}else if(flag.equals("pr")){
				for(int i=1;i<=nums;i++){
					Text k = new Text(String.valueOf(i));
					Text v = new Text(String.valueOf("B:"+tokens[0]+","+tokens[1]));
					context.write(k, v);
				}
			}
		}
		
	}
	
	public static class PageRankReducer extends Reducer<Text,Text,Text,Text>{
		
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			Map<Integer,Float> mapA = new HashMap<Integer,Float>();
			Map<Integer,Float> mapB = new HashMap<Integer,Float>();
			float pr = 0f;
			
			for(Text line:values){
				System.out.println(line);
				String vals = line.toString();
				if(vals.startsWith("A:")){
					String[] tokenA = PageRankJob.DELIMITER.split(vals.substring(2));
					mapA.put(Integer.parseInt(tokenA[0]), Float.parseFloat(tokenA[1]));
				}
				if(vals.startsWith("B:")){
					String[] tokenB = PageRankJob.DELIMITER.split(vals.substring(2));
					mapB.put(Integer.parseInt(tokenB[0]), Float.parseFloat(tokenB[1]));
				}
			}
			Iterator iterA = mapA.keySet().iterator();
			while(iterA.hasNext()){
				int idx = (int) iterA.next();
				float A = mapA.get(idx);
				float B = mapB.get(idx);
				pr += A*B;
			}
			context.write(key, new Text(PageRankJob.scaleFloat(pr)));
		}
	}
	
	public static void run(Map<String,String>path) throws Exception{
		JobConf conf = PageRankJob.config();
		String input = path.get("tmp1");
		String output = path.get("tmp2");
		String pr = path.get("input_pr");
		
		HdfsDAO hdfs = new HdfsDAO(PageRankJob.HDFS,conf);
	    hdfs.rmr(output);
	    
	    Job job = new Job(conf);
	    job.setJarByClass(PageRank.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setMapperClass(PageRankMapper.class);
	    //job.setCombinerClass(AdjacencyMatrixReducer.class);
	    job.setReducerClass(PageRankReducer.class);
	    
	    //job.setInputFormatClass(TextInputFormat.class);
	    //job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.setInputPaths(job, new Path(input),new Path(pr));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    job.waitForCompletion(true);
	    
	    hdfs.rmr(pr);
	    hdfs.rename(output,pr);
	}
}
