package hadoop.test.zookeeper;

import java.io.IOException;

import hadoop.test.example.HdfsDAO;

public class Profit {
	public static void main(String[] args) throws Exception{
		profit();
	}
	
	public static void profit() throws Exception{
		int sell = getSell();
		int purchase = getPurchase();
		int other = getOther();
		int profit = sell - purchase - other;
		System.out.printf("profit = sell - purchase - other = %d - %d - %d = %d\n",sell,purchase,other,profit);
	}

	private static int getOther() throws IOException {
		// TODO Auto-generated method stub
		return Other.calcOther(Other.file);
	}

	private static int getPurchase() throws Exception{
		// TODO Auto-generated method stub
		HdfsDAO hdfs = new HdfsDAO(Purchase.HDFS,Purchase.config());
		return Integer.parseInt(hdfs.cats(Purchase.path().get("output")+"/part-r-00000").trim());
	}

	private static int getSell() throws Exception {
		// TODO Auto-generated method stub
		HdfsDAO hdfs = new HdfsDAO(Sell.HDFS,Purchase.config());
		return Integer.parseInt(hdfs.cats(Sell.path().get("output")+"/part-r-00000").trim());
	}
	
}
