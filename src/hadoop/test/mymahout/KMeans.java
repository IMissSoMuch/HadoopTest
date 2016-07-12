package hadoop.test.mymahout;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

import hadoop.test.example.HdfsDAO;

public class KMeans {
	private static final String HDFS = "hdfs://125.216.242.37:9000";

	public static void main(String[] args) throws Exception {
		String localFile = "data/randomData.csv";
		String inPath = HDFS + "/user/root/mix_data";
		String seqFile = inPath + "/seqfile";
		String seeds = inPath + "/seeds";
		String outPath = inPath + "/result/";
		String clusteredPoints = outPath + "/clusteredPoints";

		JobConf conf = config();
		HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
		hdfs.rmr(inPath);
		hdfs.mkdir(inPath);
		hdfs.copyFile(localFile, inPath);
		hdfs.ls(inPath);

		InputDriver.runJob(new Path(inPath), new Path(seqFile), "org.apache.mahout.math.RandomAccessSparseVector");

		int k = 3;
		Path seqFilePath = new Path(seqFile);
		Path clustersSeeds = new Path(seeds);
		DistanceMeasure measure = new EuclideanDistanceMeasure();
		clustersSeeds = RandomSeedGenerator.buildRandom(conf, seqFilePath, clustersSeeds, k, measure);
		KMeansDriver.run(conf, seqFilePath, clustersSeeds, new Path(outPath), 0.01, 10, true, 0.01, false);

		Path outGlobPath = new Path(outPath, "clusters-*-final");
		Path clusteredPointsPath = new Path(clusteredPoints);
		System.out.printf("Dumping out clusters from clusters: %s and clusteredPoints: %s\n", outGlobPath,
				clusteredPointsPath);

		ClusterDumper clusterDumper = new ClusterDumper(outGlobPath, clusteredPointsPath);
		clusterDumper.printClusters(null);
	}

	public static JobConf config() {
		JobConf conf = new JobConf(KMeans.class);
		conf.setJobName("ItemCFHadoop");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}

}
