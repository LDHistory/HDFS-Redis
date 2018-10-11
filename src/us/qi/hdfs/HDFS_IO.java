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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class HDFS_IO{
	private final Logger logger = Logger.getLogger("us.qi.hdfs");
	
	/**
	 * Directory search for Hadoop.
	 * @param conf Hadoop Configuration
	 * @param uri Hadoop cluster NameNode URI
	 * @param args Directory name on Hadoop
	 */
	public void HDFS_Search(Configuration conf, String uri, String args) throws FileNotFoundException, IOException {
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		Path paths = new Path(args);
		
		FileStatus[] status = fs.listStatus(paths);
		Path[] listPaths = FileUtil.stat2Paths(status);
		
		logger.info("File searching...");
		for (Path p : listPaths) {
			System.out.println(p);
		}
		
		fs.close();
	}
	
	/**
	 * Directory be created in Hadoop.
	 * @param conf Hadoop Configuration
	 * @param uri Hadoop cluster NameNode URI
	 * @param dir File path to be generated
	 * @throws IOException 
	 */
	public void mkdir(Configuration conf, String uri, String dir) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		Path path = new Path(dir);
		if (fs.exists(path)) {
			logger.warning(dir + " is already exists.");
			
			fs.close();
			return;
		}
		
		fs.mkdirs(path);
		logger.info(path + " is created.");
		
		fs.close();
	}
	
	/**
	 * File create for Hadoop.
	 * @param conf Hadoop Configuration
	 * @param uri Hadoop cluster NameNode URI
	 * @param file File to be read
	 * @throws IOException 
	 */
	public void HDFS_Read(Configuration conf, String uri) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		/** Check a file ahead before processing order.*/
		Path path = new Path(uri);
		String filename = uri.substring(uri.lastIndexOf('/') + 1, uri.length());
		
		if (!fs.exists(path)) {
			logger.warning(filename + " is not exists.");
			
			fs.close();
			return;
		}
		
		FSDataInputStream in = fs.open(path);
		OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(filename)));
		
		byte[] read = new byte[1024];
		int numBytes = 0;
		
		while ((numBytes = in.read(read)) > 0) {
			out.write(read, 0, numBytes);
		}
		
		in.close();
		out.close();
		fs.close();
	}
	
	/**
	 * File write for Hadoop.
	 * @param conf Hadoop Configuration
	 * @param uri Hadoop cluster NameNode URI
	 * @param dest File path to be generated
	 * @param source Created Data
	 * @throws IOException 
	 */
	public void HDFS_Write(Configuration conf, String uri, String dest, String source) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		/** Get the filename without the file path.*/
		String fileName = source.substring(source.lastIndexOf('/') + 1, source.length());
		
		/** Check File is right.*/
		if (dest.charAt(dest.length() - 1) != '/') {
			dest = dest + "/" + fileName;
		} else {
			dest = dest + fileName;
		}
		
		/** Cheak a file is already exists.*/
		Path path = new Path(dest);
		if (fs.exists(path)) {
			logger.warning(dest + " is already exists.");
			
			fs.close();
			return;
		}
		
		InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));
		
		FSDataOutputStream os = fs.create(path, new Progressable() {
			
			@Override
			public void progress() {
				// TODO Auto-generated method stub
				System.out.print("*");
			}
		});
		
		/** Copy Bytes to HDFS.*/
		byte[] buffer = new byte[1024];
		int valueBytes = 0;
		
		while ((valueBytes = in.read(buffer)) > 0) {
			os.write(buffer, 0, valueBytes);
		}
		
		logger.info(dest + " is created.");
		
		in.close();
		os.close();
		fs.close();
	}
	
	/**
	 * File delete for Hadoop.
	 * @param conf Hadoop Configuration
	 * @param uri Hadoop cluster NameNode URI
	 * @param file File to be deleted
	 * @throws IOException 
	 */
	public void HDFS_Delete(Configuration conf, String uri, String file) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		/** Setting a path for hadoop dir.*/
		StringBuilder sb = new StringBuilder();
		
		sb.append(uri);
		sb.append("user/");
		sb.append("hadoop/");
		sb.append(file);
		
		Path path = new Path(sb.toString());
		if (!fs.exists(path)) {
			logger.warning(path + " does not exists.");
			
			fs.close();
			return;
		}
		
		fs.delete(new Path(sb.toString()), true);
		
		logger.info("Successfully delete a File.");
		
		fs.close();
	}
	
//	public static void main(String args[]) throws Exception {
//		/** Object */
//		HDFS_Configuration conf = new HDFS_Configuration();
//		HDFS_IO hdfs = new HDFS_IO();
//		
//		/** Value */
//		String uri = "hdfs://slave02:9000/";
//		Configuration configuration = conf.get_conf();
//		String file = "test.seq";
//		
//		hdfs.HDFS_Delete(configuration, uri, file);
//		
//		hdfs.HDFS_Write(configuration, uri, args[0], args[1]);
//		hdfs.HDFS_Search(configuration, uri, args[0]);
//	}
}
