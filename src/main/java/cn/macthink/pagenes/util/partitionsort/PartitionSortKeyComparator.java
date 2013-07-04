/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-6
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.util.partitionsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * PartitionSortKeyComparator
 * 
 * @author Macthink
 */
public class PartitionSortKeyComparator extends WritableComparator {

	public PartitionSortKeyComparator() {
		super(PartitionSortKeyPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		return ((PartitionSortKeyPair) a).getPartitionKey().compareTo(((PartitionSortKeyPair) b).getPartitionKey());
	}

}
