/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-5
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.util.partitionsort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * PartitionSortKeyPairPartitioner
 * 
 * @author Macthink
 */
public class PartitionSortKeyPairPartitioner extends Partitioner<PartitionSortKeyPair, Writable> {

	@Override
	public int getPartition(PartitionSortKeyPair key, Writable value, int numPartitions) {
		return key.getPartitionKey().get() % numPartitions;
	}

}
