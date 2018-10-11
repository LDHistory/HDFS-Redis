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
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;

public class HDFS_Thread {
	private final static Logger logger = Logger.getLogger("us.qi.hdfs");
	
	private static String URI = "hdfs://slave02:9000/user/hadoop/test.seq";
	private static String KEY = "d:air:1:rawaqi";
	
	public static void main(String args[]) throws IOException {

		HDFS_Configuration config = new HDFS_Configuration();
		HDFS_ShareData share = new HDFS_ShareData();
		
		Configuration conf = config.get_conf();
		
		HDFS_Jedis jedis = new HDFS_Jedis(share, KEY);
		HDFS_SequenceFile sequence = new HDFS_SequenceFile(conf, share, 
				jedis,URI);
		
		logger.info("Start!");

		/** Write Sequence FIle*/
//		jedis.start();
//		sequence.start();
		
		/** Read Sequence File*/
//		sequence.readSeq("hdfs://slave02:9000/user/hadoop/test.seq");
		
		/** Put data to Redis*/
		sequence.getSeq(URI);
	}
}
