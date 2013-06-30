/**
 * Project:cn.macthink.pagenes
 * File Created at 2013-6-28
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.preprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * GenerateInitClustersMapper
 * 
 * @author Macthink
 */
public class GenerateInitClustersMapper extends Mapper<Text, VectorWritable, NullWritable, ClusterWritable> {

	/**
	 * Map函数 Input:(docName,docVector); Output:(NullWritable,ClusterWritable).
	 * 
	 * @param key
	 *            输入文档名称
	 * @param value
	 *            输入文档的向量表示
	 * @param context
	 */
	@Override
	protected void map(Text key, VectorWritable value, Context context) throws IOException, InterruptedException {
		Vector vector = value.get();
		Cluster cluster = new DistanceMeasureCluster(vector);
		ClusterWritable clusterWritable = new ClusterWritable(cluster);
		context.write(NullWritable.get(), clusterWritable);

	}

}
