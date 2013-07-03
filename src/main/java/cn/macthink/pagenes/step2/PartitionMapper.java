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

import cn.macthink.pagenes.model.PAgenesCluster;
import cn.macthink.pagenes.util.PAgenesConfigKeys;

/**
 * PartitionMapper
 * 
 * @author Macthink
 */
public class PartitionMapper extends Mapper<NullWritable, PAgenesCluster, IntWritable, PAgenesCluster> {

	/**
	 * 将使用的处理机数目（处理机即Mapper or Reducer）
	 */
	private int processorNum;

	/**
	 * setup
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		processorNum = context.getConfiguration().getInt(PAgenesConfigKeys.PROCESSOR_NUM_KEY, 2);
	}

	/**
	 * map:计算输入的簇应该划分到哪一个处理机中去
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 * @param key
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(NullWritable key, PAgenesCluster value, Context context) throws IOException,
			InterruptedException {
		context.write(new IntWritable(PartitionPolicy.getPartitionNum(value, processorNum)), value);
	}
}
