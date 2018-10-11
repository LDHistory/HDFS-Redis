/** 
 * Copyright (c) 2010 Jonathan Leibiusky

* Permission is hereby granted, free of charge, to any person
* obtaining a copy of this software and associated documentation
* files (the "Software"), to deal in the Software without
* restriction, including without limitation the rights to use,
* copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the
* Software is furnished to do so, subject to the following
* conditions:
* 
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
* OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
* HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
* WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
* OTHER DEALINGS IN THE SOFTWARE.
*/
package us.qi.hdfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

/** 
 * Jedis can be transaction with Redis.
 * */
public class HDFS_Jedis extends Thread {
	private final static Logger logger = Logger.getLogger("us.qi.hdfs");
	
	private JedisPool pool;
	private HDFS_ShareData share;
	private Jedis jedis;
	
	private String key;
	private int start;
	private int last;
	private long count;
	private boolean jobFlag;
	private double rediscount;
	
	private int watingReadTime = 100000;
	private double divideCount = 100000.0;
	
	public HDFS_Jedis() {}
	
	/** 
	 * Initialize a Jedis configuration.
	 * Jedis pool manages a connection setting
	 * like a host name and port number, connecting time.
	 * */
	public HDFS_Jedis(HDFS_ShareData share, String key) {	
		this.pool = new JedisPool(new JedisPoolConfig(), "localhost", 6379, watingReadTime);
		this.jedis = pool.getResource();
		
		this.key = key;
		this.share = share;
	}
	
	/** 
	 * Save a key and value in Reids.
	 * It should change a source code to confirm a redis type.
	 * Because Key and value pairs must be stored according to the table like a Hashmap, String, Lists etc.
	 * @param uri Hadoop cluster NameNode URI
	 * */
	public void setSortedSet(HashMap<String, String> redisKV) throws IOException {
		StringBuilder sb = new StringBuilder();

		Pipeline pipeline = jedis.pipelined();
		
		Set<Entry<String, String>> KVset = redisKV.entrySet();
		
		if (KVset == null) {
			logger.warning("There is not value in KVset!");
			
			return;
		}
		
		logger.info("Starting insert a data to Redis...");
		
		double a = System.currentTimeMillis();
		/** Key and value pairs will be saved in Redis.*/
		for (Entry<String, String> tuple : KVset) {
			sb.append(tuple.getKey());
	
			pipeline.zadd(sb.substring(0, 14), Double.parseDouble(sb.substring(15)),
					tuple.getValue());
			
			sb.setLength(0);
		}

		pipeline.sync();
		
		double b = System.currentTimeMillis();
		double result = (b - a) / 1000.0;
		logger.info("Writing to Redis time : " + result);
		
		KVset.clear();
		
		logger.info("Successfully put a key and value to Redis.");
	}
	
	/** Close a Jedis and pool connection.*/
	public void closePool() {
		jedis.close();
		pool.close();
	}
	
	/** Generate a temporary data for testing.*/
	public void test() {
		try (Jedis jedis = pool.getResource()){
			Pipeline p = jedis.pipelined();
			
			String key = "d:air:1:rawaqi";
			
			for (int i = 0; i < divideCount; i++)
				p.zadd(key, i, String.valueOf(i));
			
			p.sync();
			
			pool.close();
		}
	}
	
	/** 
	 * Initialize a counting value and writing thread flag.
	 * And get the number of Redis Sorted Sets rows.
	 *  */
	public void getRedisValueCount() {
		this.start = 0;
		this.last = 0;
		this.jobFlag = true;
		
		this.count =  jedis.zlexcount(key, "-", "+");
		
		rediscount = Math.ceil(this.count / divideCount);
	}
	
	/** 
	 * Return a rediscount.
	 * @return rediscount the number of loop counting.
	 * */
	public double getRedisCount() {
		return rediscount;
	}
	
	/** 
	 * Receive a value of key.
	 * If there are a lot of Data about 2,000,000 in Redis,
	 * Jedis will occur a time out error.
	 * So Data should search by dividing range.
	 * @param count the number of remaining redis rows.
	 * @return map  key and value pairs from redis.
	 * */
	public synchronized void getSortedMap(long count){
		
		Set<Tuple> redisKV;
		ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String,String>();
		double a = System.currentTimeMillis();
		
		if (count >= divideCount) {
			
			last += divideCount;
			
			redisKV = jedis.zrangeWithScores(key, start, last - 1);
			
			for (Tuple tuple : redisKV) {
				map.put(key + ":" + tuple.getScore(), tuple.getElement());
			}
			
			start += divideCount;
			this.count -= divideCount;
			share.setMap(map);
		} else {
			last += count;
			
			redisKV = jedis.zrangeWithScores(key, start, last);
			
			if (redisKV.isEmpty()) {
				logger.info("Searched Data is empty! Finish Searcing process!");

				jobFlag = false;
				
				pool.close();
			}
			
			for (Tuple tuple : redisKV) {
				map.put(key + ":" + tuple.getScore(), tuple.getElement());
			}
			
			logger.info("Finish Searching process!");
			logger.info("Redis Search Finish..." + "Result size : " + map.size());
			
			double b = System.currentTimeMillis();
			double result = (b - a) / 1000.0;
			logger.info("Reading time : " + result);
			
			share.setMap(map);
			
			jobFlag = false;
			pool.close();
		}
	}
	
	/** Reset values.*/
	public void resetValue() {
		start = 0;
		last = 0;
		count = 0;
	}

	@Override
	public void run() {
		getRedisValueCount();
		
		double start = System.currentTimeMillis();
		
		while (this.jobFlag) {
			getSortedMap(this.count);
		}
		
		resetValue();
		
		double endtime = System.currentTimeMillis();
		double result = (endtime - start) / 1000.0;
		logger.info("Jedis Time" + result);
	}
}

/** 
 * Not use.
 * */
	
///** 
// * Save a key and value in Reids.
// * It should change a source code to confirm a redis type.
// * Because Key and value pairs must be stored according to the table like a Hashmap, String, Lists etc.
// * @param uri Hadoop cluster NameNode URI
// * */
//	public void setSortedSet(HDFS_SequenceFile sequence, String uri) throws IOException {
//		StringBuilder sb = new StringBuilder();
//
//		try (Jedis jedis = pool.getResource()){
//			Pipeline pipeline = jedis.pipelined();
//		
//			Set<Entry<String, String>> KVset = sequence.getSeq(uri).entrySet();
//			
//			Set<Entry<String, String>> KVset = redisKV.entrySet();
//			
//			if (KVset == null) {
//				logger.warning("There is not value in KVset!");
//				
//				return;
//			}
//			
//			logger.info("Starting insert a data to Redis...");
//			
//			double a = System.currentTimeMillis();
//			/** Key and value pairs will be saved in Redis.*/
//			for (Entry<String, String> tuple : KVset) {
//				sb.append(tuple.getKey());
//		
//				pipeline.zadd(sb.substring(0, 14), Double.parseDouble(sb.substring(15)),
//						tuple.getValue());
//				
//				sb.setLength(0);
//			}
//
//			pipeline.sync();
//			
//			double b = System.currentTimeMillis();
//			double result = (b - a) / 1000.0;
//			logger.info("Writing to Redis time : " + result);
//			
//			KVset.clear();
//			pool.close();
//			
//			logger.info("Successfully put a key and value to Redis.");
//		} finally {
//			if (jedis != null)
//				jedis.close();
//		}
//	}
//	
//	
///** 
// * Receive a value of key.
// * If there are a lot of Data about 2,000,000 in Redis,
// * Jedis will occur a time out error.
// * So Data should search by dividing range.
// * @return map Stored a key and value pairs from redis.
// * @throws IOException 
// * */
//	public HashMap<String, String> getSortedSet() {			
//	
//		try (Jedis jedis = pool.getResource()) {
//			
//			/** Get a Redis key size.*/
//			long count = jedis.zlexcount(key, "-", "+");
//			int start = 0;
//			int last = 0;
//			
//			Set<Tuple> redisKV;
//			
//			/** 
//			 * If there are a lot of Data about 2,000,000 in Redis,
//			 * Jedis will occur a time out error.
//			 * So Data should search by dividing range.
//			 * */
//			double a = System.currentTimeMillis();
//			
//			while (true) {
//				if (count >= 100000) {
//					last += 100000;
//					
//					logger.info("Redis Search Staring..." + "start : " + start + " last : " + last + " count : " + count );
//					redisKV = jedis.zrangeWithScores(key, start, last - 1);
//					
//					for (Tuple tuple : redisKV) {
//						map.put(key + ":" + tuple.getScore(), tuple.getElement());
//					}
//					
//					start += 100000;
//					count -= 100000;
//					
//					redisKV.clear();
//					
//				} else {
//					last += count;
//					
//					logger.info("Redis Search Staring..." + "start : " + start + " last : " + last + " count : " + count );
//					redisKV = jedis.zrangeWithScores(key, start, last);
//					
//					if (redisKV.isEmpty()) {
//						logger.info("Searched Data is empty! Finish Searcing process!");
//						logger.info("Redis Search Finish..." + "Result size : " + map.size());
//						
//						double b = System.currentTimeMillis();
//						double result = (b - a) / 1000.0;
//						System.out.println("Reading time : " + result);
//						
//						pool.close();
//						return map;
//					}
//					
//					for (Tuple tuple : redisKV) {
//						map.put(key + ":" + tuple.getScore(), tuple.getElement());
//					}
//					
//					redisKV.clear();
//					logger.info("Finish Searching process!");
//					logger.info("Redis Search Finish..." + "Result size : " + map.size());
//					
//					double b = System.currentTimeMillis();
//					double result = (b - a) / 1000.0;
//					System.out.println("Reading time : " + result);
//					
//					pool.close();
//					return map;
//				}
//			}
//		}
//	}
//	
//		
//	public static void main(String args[]) throws IOException {
//		
//		HDFS_ShareData s = new HDFS_ShareData();
//		HDFS_Jedis j = new HDFS_Jedis(s, "a");
//		j.test();
//		HDFS_Configuration config = new HDFS_Configuration();
//		Configuration conf = config.get_conf();
//		
//		HDFS_Jedis jedis = new HDFS_Jedis("d:air:1:rawaqi");
//		HDFS_SequenceFile sequence = new HDFS_SequenceFile(conf,
//				"hdfs://slave02:9000/user/hadoop/test.seq");
//		
//		logger.info("Start!");
//		double start = System.currentTimeMillis();
//		
//		/** Write Sequence File*/
//		sequence.writeSeq(jedis.getSortedSet());
//		jedis.clearHashMap();
//		
//		/** Put data to Redis*/
//		jedis.setSortedSet(conf, sequence, "hdfs://slave02:9000/user/hadoop/test.seq");
//		
//		double endtime = System.currentTimeMillis();
//		double result = (endtime - start) / 1000.0;
//		System.out.println(result);
//	}
