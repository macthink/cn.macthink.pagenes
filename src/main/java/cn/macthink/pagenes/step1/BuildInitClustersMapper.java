/**
 * Project:cn.macthink.pagenes
 * File Created at 2013-6-28
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.step1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

import cn.macthink.pagenes.model.PAgenesCluster;

/**
 * GenerateInitClustersMapper
 * 
 * @author Macthink
 */
public class BuildInitClustersMapper extends
		Mapper<WritableComparable<?>, VectorWritable, NullWritable, PAgenesCluster> {

	/**
	 * map
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 * @param key
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(WritableComparable<?> key, VectorWritable value, Context context) throws IOException,
			InterruptedException {
		NamedVector point = (NamedVector) value.get();
		PAgenesCluster cluster = new PAgenesCluster(point);
		context.write(NullWritable.get(), cluster);
	}

}
