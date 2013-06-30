/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-5
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.util.partitionsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * PartitionSortKeyPairComparator
 * 
 * @author Macthink
 */
public class PartitionSortKeyPairComparator extends WritableComparator {

	public PartitionSortKeyPairComparator() {
		super(PartitionSortKeyPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		PartitionSortKeyPair o1 = (PartitionSortKeyPair) a;
		PartitionSortKeyPair o2 = (PartitionSortKeyPair) b;
		if (!o1.getPartitionKey().equals(o2.getPartitionKey())) {
			return o1.getPartitionKey().compareTo(o2.getPartitionKey());
		} else {
			return o2.getSortKey().compareTo(o1.getSortKey());
		}
	}

}
