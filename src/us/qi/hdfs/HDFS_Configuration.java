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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Setting for Hadoop Configuration.
 * Configuration Obejct can get a Hadoop information such as hdfs-site.xml, core-site.xml etc.
 */
public class HDFS_Configuration {

	private Configuration configuration;
	
	/** Create a Configuration Object and add resource for my hadoop server setting.*/
	public HDFS_Configuration() {
		configuration = new Configuration();
		
		configuration.addResource(new Path("/home/hadoop/hadoop-2.8.4/etc/hadoop/core-site.xml"));
		configuration.addResource(new Path("/home/hadoop/hadoop-2.8.4/etc/hadoop/hdfs-site.xml"));
	}
	
	/**
	 * Return Hadoop configuration. 
	 * @return configuration Hadoop configuration property
	 * */
	public Configuration get_conf() {
		return this.configuration;
	}
}
