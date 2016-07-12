package hadoop.test.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import hadoop.test.example.WordCount.IntSumReducer;
import hadoop.test.example.WordCount.TokenizerMapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class matrixMultiply {
	private static final String HDFS = "hdfs://125.216.242.37:9000";
	private static final Pattern DELIMITER = Pattern.compile("[\t,]");
	
	public static class MatrixMapper extends Mapper<LongWritable,Text,Text,Text>{
		private String flag;
		private int rowNum = 2;
		private int colNum = 2;
		private int rowIndexA = 1;
		private int rowIndexB = 1;
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getName();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	      String[] tokens = DELIMITER.split(value.toString());
	      if(flag.equals("m1")){
	    	  for(int i=1;i<=rowNum;i++){
	    		  Text k = new Text(rowIndexA+","+i);
	    		  for(int j=1;j<= tokens.length;j++){
	    			  Text v = new Text("A:"+j+","+tokens[j-1]);
	    			  context.write(k, v);
	    			  System.out.println(k.toString()+"  "+v.toString());
	    		  }
	    	  }
	    	  rowIndexA++;
	      }
	      else if(flag.equals("m2")){
	    	  for(int i=1;i<=tokens.length;i++){
	    		  for(int j=1;j<= colNum;j++){
	    			  Text k = new Text(i+","+j);
	    			  Text v = new Text("B:"+rowIndexB+","+tokens[j-1]);
	    			  context.write(k, v);
	    			  System.out.println(k.toString()+"  "+v.toString());
	    		  }
	    	  }
	    	  rowIndexB++;
	      }
	    }
	} 
	
	
	public static class MatrixReducer extends Reducer<Text,Text,Text,IntWritable> {
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			Map<String,String> mapA = new HashMap<String,String>();
			Map<String,String> mapB = new HashMap<String,String>();
			
			System.out.print(key.toString()+"  ");
			
			for(Text line:values){
				String val = line.toString();
				System.out.print("("+val+")");
				if(val.startsWith("A:")){
					String[] kv = DELIMITER.split(val.substring(2));
					mapA.put(kv[0], kv[1]);
					
				}else if(val.startsWith("B:")){
					String[] kv = DELIMITER.split(val.substring(2));
					mapB.put(kv[0], kv[1]);
				}
			}
			
			int result = 0;
			Iterator<String> iter = mapA.keySet().iterator();
			while(iter.hasNext()){
				String mapk = iter.next();
				result += Integer.parseInt(mapA.get(mapk))*Integer.parseInt(mapB.get(mapk));
			}
			context.write(key,new IntWritable(result));
			System.out.println();
		}
	} 
	
	public static void run(Map<String,String> path) throws Exception{
		Configuration conf = new Configuration();
		//Job job = Job.getInstance(conf, "matrix multiply");
	    //job.setJarByClass(matrixMultiply.class);
	    String input = path.get("input");
	    String input1 = path.get("input1");
	    String input2 = path.get("input2");
	    String output = path.get("output");
	    
	    HdfsDAO hdfs = new HdfsDAO(HDFS,conf);
	    hdfs.rmr(input);
	    hdfs.mkdir(input);
	    hdfs.copyFile(path.get("m1"), input1);
	    hdfs.copyFile(path.get("m2"), input2);
	    
	    Job job = Job.getInstance(conf, "matrix multiply");
	    job.setJarByClass(matrixMultiply.class);
	    
	    job.setMapperClass(MatrixMapper.class);
	    //job.setCombinerClass(MatrixReducer.class);
	    job.setReducerClass(MatrixReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //job.setInputFormatClass(TextInputFormat.class);
	    //job.setOutputFormatClass(TextOuputFormat.class);
	    FileInputFormat.setInputPaths(job, new Path(input1),new Path(input2));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args){
		Map<String,String> path = new HashMap<String,String>();
		path.put("m1", "data/m1.csv");
		path.put("m2", "data/m2.csv");
		path.put("input", HDFS+"/user/root/test/matrix");
		path.put("input1", HDFS+"/user/root/test/matrix/m1");
		path.put("input2", HDFS+"/user/root/test/matrix/m2");
		path.put("output", HDFS+"/user/root/test/matrix/output");
		try{
			run(path);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
