package hadoop.test.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDAO {
	static Configuration cfg = HBaseConfiguration.create();
	public static void create(String tablename,String columnFamily) throws Exception{
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if(admin.tableExists(tablename)){
			System.out.println("table Exists!");
			System.exit(0);
		}
		else {
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		}
	}
	
	public static void put(String tablename,String row, String columnFamily,
			String column,String data) throws Exception{
		HTable table = new HTable(cfg,tablename);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put '"+row+"','"+columnFamily+":"+column+"','"+data+"'");
		
	}
	
	public static void get(String tablename,String row) throws Exception{
		HTable table = new HTable(cfg,tablename);
		Get g = new Get(Bytes.toBytes(row));
		Result result = table.get(g);
		System.out.println("Get: "+result);
		
	}
	
	public static void scan(String tablename) throws Exception{
		HTable table = new HTable(cfg,tablename);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		System.out.println("Scan: "+rs);
		
	}
	
	public static boolean delete(String tablename)throws Exception{
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if(admin.tableExists(tablename)){
			try{
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
			}catch(Exception e){
				e.printStackTrace();
				return false;
			}
		}
		return true;
		
	}
	
	public static void main(String[] args){
		String tablename = "test";
		String columnFamily = "cf";
		try{
			//HBaseDAO.create(tablename, columnFamily);
			HBaseDAO.put(tablename, "row1", columnFamily, "c11", "data");
			HBaseDAO.get(tablename, "row1");
			HBaseDAO.scan(tablename);
			if(true == HBaseDAO.delete(tablename))
				System.out.println("Delete table:"+tablename+"success!");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
}
