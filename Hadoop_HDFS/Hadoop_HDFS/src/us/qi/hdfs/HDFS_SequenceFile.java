/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package us.qi.hdfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

public class HDFS_SequenceFile extends Thread {
	private final Logger logger = Logger.getLogger("us.qi.hdfs");
	
	private Configuration conf;
	
	private HDFS_ShareData share;
	private SequenceFile.Writer writer;
	private HDFS_Jedis jedis;
	
	private Text key;
	private Text value;
	private FileSystem fs;
	private Path path;
	
	private String result;
	
	public HDFS_SequenceFile() {}
	
	/**
	 * Constructor to setting Hadoop config and uri.
	 * @param conf Hadoop Configuration
	 * @param uri Hadoop cluster NameNode URI
	 * @throws IOException 
	 */
	public HDFS_SequenceFile(Configuration conf, HDFS_ShareData share, HDFS_Jedis jedis,
			String uri) throws IOException {
		this.conf = conf;
		this.path = new Path(uri);
		this.fs = FileSystem.get(conf);
		
		this.share = share;
		this.jedis = jedis;
		
		key = new Text();
		value = new Text();
	}
	
	/** 
	 * This method can read a sequence File.
	 * @param uri Hadoop cluster NameNode URI
	 * @return result whether sequence File is successfully read.
	 * */
	public String readSeq(String uri) throws IOException {
		
		key = new Text();
		value = new Text();
		
		if (fs.exists(path)) {
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));

			while (reader.next(key, value)) {
				System.out.println(key + "\t" + value);
			}

			reader.close();
			fs.close();
			
			result = "Successfully reads a sequence File.";
			
		} else {
			result  = "Unsuccessfully reads a sequence File.";
			logger.warning(path + " is not exists.");
		}
		return result;
	}
	
	/** 
	 * This method can get a key and value of Sequence File.
	 * It must be called before writing a key and value to Redis.
	 * @param uri Hadoop cluster NameNode URI
	 * @return setKV A collection of key and value pair stored in in HDFS.
	 * */
	public void getSeq(String uri) throws IOException{	
		key = new Text();
		value = new Text();
		
		if (fs.exists(path)) {
			
			HashMap<String, String> setKV = new HashMap<String, String>();
			
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
			
			double a = System.currentTimeMillis();
			
			while (reader.next(key, value)) {
				setKV.put(key.toString(), value.toString());
			}
			
			double b = System.currentTimeMillis();
			double result = (b - a) / 1000.0;
			logger.info("Reading time : " + result);
			
			reader.close();
			fs.close();
			
			logger.info("Successfully get a Sequence File.");
			
			HashMap<String, String> tempKV = new HashMap<String, String>();
			
			for (Entry<String, String> tuple : setKV.entrySet()) {
				tempKV.put(tuple.getKey(), tuple.getValue());
				
				if (tempKV.size() >= 1000000) {
					jedis.setSortedSet(tempKV);
					tempKV.clear();
				}
			}
			
			if (!tempKV.isEmpty()) {
				jedis.setSortedSet(tempKV);
				tempKV.clear();
			}
			
			jedis.closePool();
			
		} else {
			logger.warning(path + " is not exists.");
		}
	}

	/** 
	 * This creates a SequenceFile writer.
	 * SequenceFile be compressed as a Block.
	 * */
	public void setWriter() {
		try {
			if (!fs.exists(path)) {
				writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
						SequenceFile.Writer.keyClass(key.getClass()), SequenceFile.Writer.valueClass(value.getClass()),
						SequenceFile.Writer.compression(CompressionType.BLOCK));
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/** 
	 * This writes a data to SequenceFile writer.
	 * Also writer creates a snapshot for data.
	 * It is handled in parallel on the Thread.
	 * @param redisKV  key and value pairs from redis.
	 * */
	public synchronized void writeSeq(ConcurrentHashMap<String, String> redisKV) {
		
		for (Map.Entry<String, String> entry : redisKV.entrySet()) {
			try {
				writer.append(new Text(entry.getKey()), new Text(entry.getValue()));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/** 
	 * It closes a writer and Hadoop filesystem.
	 * Snapshot be flushed to Sequence File
	 * into the HDFS when writer.close is called.
	 * */
	public void closeWriter() {
		try {
			writer.close();
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		setWriter();
		
		double start = System.currentTimeMillis();
		
		double count  = jedis.getRedisCount();
		ConcurrentHashMap<String, String> hash = new ConcurrentHashMap<String, String>();
		
		for (int i = 0; i <= count - 1; i++) {
			hash = share.getMap();
			writeSeq(hash);
			hash.clear();
		}
		
		closeWriter();
		
		double endtime = System.currentTimeMillis();
		double result = (endtime - start) / 1000.0;
		logger.info("HDFS Time" + result);
	}
}

/** 
 * Not use.
 * */

///** 
// * This method can get a key and value of Sequence File.
// * It must be called before writing a key and value to Redis.
// * @param uri Hadoop cluster NameNode URI
// * @return setKV A collection of key and value pair stored in in HDFS.
// * */
//public HashMap<String, String> getSeq(String uri) throws IOException{
//	key = new Text();
//	value = new Text();
//	
//	if (fs.exists(path)) {
//		
//		HashMap<String, String> setKV = new HashMap<String, String>();
//		
//		SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
//		
//		double a = System.currentTimeMillis();
//		
//		while (reader.next(key, value)) {
//			setKV.put(key.toString(), value.toString());
//		}
//		
//		double b = System.currentTimeMillis();
//		double result = (b - a) / 1000.0;
//		System.out.println("Reading time : " + result);
//		
//		reader.close();
//		fs.close();
//		
//		logger.info("Successfully get a Sequence File.");
//		
//		HashMap<String, String> tempKV = new HashMap<String, String>();
//		
//		for (Entry<String, String> tuple : setKV.entrySet()) {
//			tempKV.put(tuple.getKey(), tuple.getValue());
//			
//			if (tempKV.size() >= 1000000) {
//				jedis.setSortedSet(tempKV);
//				tempKV.clear();
//			}
//		}
//		
//		if (!tempKV.isEmpty()) {
//			jedis.setSortedSet(tempKV);
//			tempKV.clear();
//		}
//		
//		jedis.closePool();
//		
//		return setKV;
//		
//	} else {
//		logger.warning(path + " is not exists.");
//	}
//	
//	return null;
//}
//
///** 
// * This method can write a sequence File.
// * @param reidsKV Collection of redis key and value
// * @return result whether sequence File is successfully created.
// * */
//public void writeSeq(HashMap<String, String> redisKV) throws IOException {
//	key = new Text();
//	value = new Text();
//		
//	if (!fs.exists(path)) {
//		
//		SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
//				SequenceFile.Writer.keyClass(key.getClass()), SequenceFile.Writer.valueClass(value.getClass()),
//				SequenceFile.Writer.compression(CompressionType.BLOCK));
//		
//		/** 
//		 * Hadoop File value must be serialized.
//		 * So recived key and value must convert to Hadoop Serialization type like Text, IntWritable, LongWritable,
//		 * when sequence file is wrote.
//		 * */
//		
//		double a = System.currentTimeMillis();
//		
//		for (Map.Entry<String, String> entry : redisKV.entrySet()) {
//			try {
//				writer.append(new Text(entry.getKey()), new Text(entry.getValue()));
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//		
//		double b = System.currentTimeMillis();
//		double result = (b - a) / 1000.0;
//		System.out.println("Writing time : " + result);
//		
//		redisKV.clear();
//		writer.close();
//		fs.close();
//		
//		logger.info("Successfully creates a sequence File.");
//	
//	} else {
//		result = "Unsuccessfully creates a sequence File.";
//		logger.warning(path + " already exists.");
//	}
//}
