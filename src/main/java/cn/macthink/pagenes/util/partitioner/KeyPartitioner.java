/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-6
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.util.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * KeyPartitioner
 * 
 * @author Macthink
 */
public class KeyPartitioner extends Partitioner<IntWritable, Writable> {

	@Override
	public int getPartition(IntWritable key, Writable value, int numPartitions) {
		return key.get() % numPartitions;
	}

}
