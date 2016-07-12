package hadoop.test.pagerank;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

public class PageRankJob {
	public static final String HDFS = "hdfs://125.216.242.37:9000";
	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	public static void main(String[] args) {
		Map<String, String> path = new HashMap<String, String>();
		path.put("page", "data/page.csv");// ���ص������ļ�
		path.put("pr", "data/pr.csv");// ���ص������ļ�

		path.put("input", HDFS + "/user/root/test/pagerank");// HDFS��Ŀ¼
		path.put("input_pr", HDFS + "/user/root/test/pagerank/pr");// pr�洢Ŀ
		path.put("tmp1", HDFS + "/user/root/test/pagerank/tmp1");// ��ʱĿ¼,����ڽӾ���
		path.put("tmp2", HDFS + "/user/root/test/pagerank/tmp2");// ��ʱĿ¼,���㵽��PR,����input_pr

		path.put("result", HDFS + "/user/root/test/pagerank/result");// ��������PR

		try {

			AdjacencyMatrix.run(path);
			int iter = 3;
			for (int i = 0; i < iter; i++) {// ����ִ��
				PageRank.run(path);
			}
			Normal.run(path);

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}

	public static JobConf config() {// Hadoop��Ⱥ��Զ��������Ϣ
		JobConf conf = new JobConf(PageRankJob.class);
		conf.setJobName("PageRank");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}

	public static String scaleFloat(float f) {// ����6λС��
		DecimalFormat df = new DecimalFormat("##0.000000");
		return df.format(f);
	}
}
