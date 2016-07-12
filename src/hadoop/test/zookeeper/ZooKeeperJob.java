package hadoop.test.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperJob {
	public final static String QUEUE = "/queue";
	public final static String PROFIT = "/queue/profit";
	public final static String PURCHASE = "/queue/purchase";
	public final static String SELL = "/queue/sell";
	public final static String OTHER = "/queue/other";
	
	public static void main(String[] args) throws Exception{
		//args[0] = "1";
		if(args.length == 0)
			System.out.println("Please start a task:");
		else
			doAction(Integer.parseInt(args[0]));
	}

	private static void doAction(int client) throws Exception {
		// TODO Auto-generated method stub
		String host1 = "125.216.242.37:2182";
		String host2 = "125.216.242.37:2183";
		String host3 = "125.216.242.37:2184";
		
		ZooKeeper zk = null;
		switch(client){
		case 1 : 
			zk = connection(host1);
			initQueue(zk);
			doPurchase(zk);
			break;
		case 2 : 
			zk = connection(host2);
			initQueue(zk);
			doSell(zk);
			break;
		case 3 : 
			zk = connection(host3);
			initQueue(zk);
			doOther(zk);
			break;
		}
	}

	private static ZooKeeper connection(String host) throws IOException {
		// TODO Auto-generated method stub
		ZooKeeper zk = new ZooKeeper(host, 60000,new Watcher(){
			public void process(WatchedEvent event){
				if(event.getType() == Event.EventType.NodeCreated && event.getPath().equals(PROFIT))
					System.out.println("Queue has Completed");
			}
		}) ;
		return zk;
	}
	
	private static void initQueue(ZooKeeper zk) throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("Watch=>"+PROFIT);
		zk.exists(PROFIT, true);
		if(zk.exists(QUEUE, false)==null){
			System.out.println("create "+QUEUE);
			zk.create(QUEUE, QUEUE.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		else{
			System.out.println(QUEUE+" is exist!");
		}
	}
	
	public static void doPurchase(ZooKeeper zk) throws ClassNotFoundException, KeeperException, InterruptedException, Exception{
		if(zk.exists(PURCHASE, false)==null){
			Purchase.run(Purchase.path());
			System.out.println("create "+PURCHASE);
			zk.create(PURCHASE, PURCHASE.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}else
			System.out.println(PURCHASE+ " is exist!");
		isComplete(zk);
	} 
	
	public static void doSell(ZooKeeper zk) throws ClassNotFoundException, InterruptedException, Exception{
		if(zk.exists(SELL, false)==null){
			Sell.run(Sell.path());
			System.out.println("create "+PURCHASE);
			zk.create(SELL, SELL.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}else
			System.out.println(SELL+ " is exist!");
		isComplete(zk);
	} 
	

	public static void doOther(ZooKeeper zk) throws Exception{
		if(zk.exists(OTHER, false)==null){
			Other.calcOther(Other.file);
			System.out.println("create "+OTHER);
			zk.create(OTHER, OTHER.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}else
			System.out.println(OTHER+ " is exist!");
		isComplete(zk);
	} 
	
	private static void isComplete(ZooKeeper zk) throws Exception {
		// TODO Auto-generated method stub
		int size = 3;
		List<String> children = zk.getChildren(QUEUE, true);
		int length = children.size();
		
		System.out.println("Queue Complete "+ length+"/"+size);
		if(length >= size){
			System.out.println("create "+PROFIT);
			Profit.profit();
			zk.create(PROFIT, PROFIT.getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			for(String child :children)
				zk.delete(QUEUE+"/"+child,-1);
		}
	}
}
