package hadoop.test.mr.kpi;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//import hadoop.test.mr.kpi.KPIPV;
//import hadoop.test.mr.kpi.KPIPV.KPIPVMapper;
//import hadoop.test.mr.kpi.KPIPV.KPIPVReducer;


public class KPIPV {
	public static class KPIPVMapper 
		extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
	      
		@Override
		public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
		  WebKPI kpi = WebKPI.filterPVs(value.toString());
	      if(kpi.isValid()){
	        word.set(kpi.getRequest());
	        context.write(word, one);
	      }
	    }
	}
	  
	public static class KPIPVReducer 
	       extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable result = new IntWritable();

	    @Override
		public void reduce(Text key, Iterable<IntWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String input = "hdfs://125.216.242.37:9000/user/root/test/weblog";
	    String ouput = "hdfs://125.216.242.37:9000/user/root/test/pv";
//	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//	    if (otherArgs.length < 2) {
//	      System.err.println("Usage: wordcount <in> [<in>...] <out>");
//	      System.exit(2);
//	    }
	    Job job = Job.getInstance(conf, "KPIPV");
	    job.setJarByClass(KPIPV.class);
	    job.setMapperClass(KPIPVMapper.class);
	    job.setCombinerClass(KPIPVReducer.class);
	    job.setReducerClass(KPIPVReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
//	    for (int i = 0; i < otherArgs.length - 1; ++i) {
//	      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//	    }
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job,new Path(ouput));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
