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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

import cn.macthink.pagenes.model.PAgenesCluster;

/**
 * GenerateInitClustersMapper
 * 
 * @author Macthink
 */
public class BuildInitClustersMapper extends Mapper<Text, VectorWritable, NullWritable, ClusterWritable> {

	/**
	 * clusterId
	 */
	private static int clusterId = 0;

	/**
	 * clusterWritable
	 */
	private ClusterWritable clusterWritable;

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
		clusterWritable = new ClusterWritable();
	}

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
	protected void map(Text key, VectorWritable value, Context context) throws IOException, InterruptedException {
		NamedVector vector = (NamedVector) value.get();
		PAgenesCluster cluster = new PAgenesCluster(vector, ++clusterId, new CosineDistanceMeasure());
		clusterWritable.setValue(cluster);
		context.write(NullWritable.get(), clusterWritable);
	}

}
