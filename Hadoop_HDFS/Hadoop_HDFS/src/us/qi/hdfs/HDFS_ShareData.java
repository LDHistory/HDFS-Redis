package us.qi.hdfs;

import java.util.concurrent.ConcurrentHashMap;

public class HDFS_ShareData {
	
	private ConcurrentHashMap<String, String> map;
	
	public synchronized void clearMap() {
		this.map = null;
	}
	
	public synchronized ConcurrentHashMap<String, String> getMap(){
		while (this.map == null) {
				try {
					wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
		ConcurrentHashMap<String, String> hashmap = map;

		clearMap();
		notifyAll();
		
		return hashmap;
	}
	
	public synchronized void setMap(ConcurrentHashMap<String, String> RedisKV) {
		while (this.map != null) {
			try {
				wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		this.map = RedisKV;
		notifyAll();
	}

}
