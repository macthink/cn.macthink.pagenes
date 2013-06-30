/**
 * Project:cn.macthink.pagenes
 * File Created at 2013年6月30日
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.step2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.iterator.ClusterWritable;

import cn.macthink.pagenes.model.Cluster;
import cn.macthink.pagenes.util.PAgenesConfigKeys;

/**
 * PartitionMapper
 * 
 * @author Macthink
 */
public class PartitionMapper extends Mapper<NullWritable, ClusterWritable, IntWritable, ClusterWritable> {

	// 将使用的处理机数目（处理机即Mapper or Reducer）
	private int processorNum;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		processorNum = context.getConfiguration().getInt(PAgenesConfigKeys.PROCESSOR_NUM_KEY, 1);
	}

	/**
	 * 计算输入的簇应该划分到哪一个处理机中去
	 */
	@Override
	protected void map(NullWritable key, ClusterWritable value, Context context) throws IOException,
			InterruptedException {
		Cluster cluster = (Cluster) value.getValue();
		context.write(new IntWritable(PartitionPolicy.getPartitionNum(cluster, processorNum)), value);
	}
}
