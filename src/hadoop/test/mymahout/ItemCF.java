package hadoop.test.mymahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

import hadoop.test.example.HdfsDAO;

public class ItemCF {
	private static final String HDFS = "hdfs://125.216.242.37:9000";

	public static JobConf config() {
        JobConf conf = new JobConf(ItemCF.class);
        conf.setJobName("ItemCFHadoop");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
	
    public static void main(String[] args) throws Exception {
        String localFile = "data/item.csv";
        String inPath = HDFS + "/user/root/test/userCF";
        String inFile = inPath + "/item.csv";
        String outPath = HDFS + "/user/root/test/userCF/result/";
        String outFile = outPath + "/part-r-00000";
        String tmpPath = HDFS + "/tmp/" + "rec";//System.currentTimeMillis();

        JobConf conf = config();
        HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
        hdfs.rmr(inPath);
        hdfs.mkdir(inPath);
        hdfs.copyFile(localFile, inPath);
        hdfs.ls(inPath);
        hdfs.cat(inFile);

        StringBuilder sb = new StringBuilder();
        sb.append("-i ").append(inPath);
        sb.append(" -o ").append(outPath);
        //sb.append(" --booleanData true");
        sb.append(" --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.EuclideanDistanceSimilarity");
        sb.append(" --tempDir ").append(tmpPath);
        args = sb.toString().split(" ");

        RecommenderJob job = new RecommenderJob();
        
        job.setConf(conf);
        ToolRunner.run(conf, job, args);
        //job.run(args);

        //hdfs.cat(outFile);
    }

    
}
