package hadoop.test.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

public class HdfsDAO {
	private static final String HDFS="hdfs://125.216.242.37:9000/";
	private String hdfsPath;
	private Configuration conf;
	public HdfsDAO(Configuration conf){
		this(HDFS,conf);
	}
	public HdfsDAO(String hdfs,Configuration conf){
		this.hdfsPath = hdfs;
		this.conf = conf;
	}
	public void ls(String folder) throws Exception{
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		FileStatus[] list = fs.listStatus(path);
		System.out.println("ls: "+folder);
		System.out.println("==========================================================");
		for(FileStatus f:list){
			System.out.printf("name: %s,folder: %s,size: %d\n", f.getPath(),f.isDir(),f.getLen());
		}
		System.out.println("==========================================================");
		fs.close();
	}
	
	public void mkdir(String folder) throws Exception{
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		if(!fs.exists(path)){
			fs.mkdirs(path);
			System.out.println("Create: "+folder);
		}
		fs.close();
	}
	
	public void rmr(String folder) throws Exception{
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		fs.deleteOnExit(path);
		fs.close();
	}
	
	public void copyFile(String local,String remote) throws Exception{
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		fs.copyFromLocalFile(new Path(local), new Path(remote));
		System.out.print("copy from: "+local+" to "+remote);
		fs.close();
	}
	
	public void download(String remote,String local) throws Exception{
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		fs.copyToLocalFile(new Path(remote), new Path(local));
		System.out.print("download from: "+remote+" to "+local);
		fs.close();
	}
	
	public void cat(String remoteFile) throws Exception{
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		FSDataInputStream is = null;
		System.out.print("cat: "+remoteFile);
		try{
			is = fs.open(new Path(remoteFile));
			IOUtils.copyBytes(is, System.out, 4096,false);
		}finally{
			IOUtils.closeStream(is);
		}
		fs.close();
	}
	
	public String cats(String remoteFile) throws Exception{
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		FSDataInputStream is = null;
		String res = "";
		BufferedReader br = null;
		System.out.print("cat: "+remoteFile);
		try{
			is = fs.open(new Path(remoteFile));
			br = new BufferedReader(new InputStreamReader(is));
			String line = null;
			while((line = br.readLine())!=null)
				res += line;
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			if(null!=br){
				try{
					br.close();
				}catch(IOException e){
					e.printStackTrace();
				}
			}
		}
		fs.close();
		return res;
	}
	
	public void createFile(String file,String content) throws Exception{
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		byte[] buff = content.getBytes();
		FSDataOutputStream os =null;
		try{
			os = fs.create(new Path(file));
			os.write(buff,0,buff.length);
			System.out.println("Create: "+file);
		}finally{
			if(os!=null)
				os.close();
		}
		fs.close();
	}
	
	public void rename(String file1, String file2) throws IOException {
		// TODO Auto-generated method stub
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		fs.rename(new Path(file1), new Path(file2));
		fs.close();
	}
	
	public static void main(String[] args){
		Configuration conf = new Configuration();
		HdfsDAO hdfs = new HdfsDAO(conf);
		try{
			//hdfs.mkdir("/user/one");
			hdfs.ls("/user");
			//hdfs.cat("/user/root/test/input/README.txt");
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
}
