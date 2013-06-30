/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-16
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.step3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;

import cn.macthink.pagenes.ClusterDistanceWritable;
import cn.macthink.pagenes.util.PAgenesConfigKeys;
import cn.macthink.pagenes.util.partitionsort.PartitionSortKeyPair;

/**
 * MergeClustersReducer
 * 
 * @author Macthink
 */
public class MergeClustersReducer extends
		Reducer<PartitionSortKeyPair, ClusterDistanceWritable, NullWritable, ClusterWritable> {

	/**
	 * 类别间距离阈值
	 */
	private double threshold;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		threshold = Double.parseDouble(context.getConfiguration().get(PAgenesConfigKeys.DISTANCE_THRESHOLD_KEY));
	}

	@Override
	protected void reduce(PartitionSortKeyPair key, Iterable<ClusterDistanceWritable> values, Context context)
			throws IOException, InterruptedException {
		// 从小到大遍历，如果小于距离阈值则合并类别
		Set<String> processedSet = new HashSet<String>();
		Iterator<ClusterDistanceWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			ClusterDistanceWritable clusterDistanceWritable = iterator.next();
			// 判断是否与其他类别合并过
			if (processedSet.contains(clusterDistanceWritable.get().getSource().getId())) {
				continue;
			}

			// 当前类别标识为已处理
			processedSet.add(clusterDistanceWritable.get().getSource().getId());

			// 尚未与其他类别合并过
			double distance = clusterDistanceWritable.get().getDistance();
			Cluster newCluster = new Cluster(clusterDistanceWritable.get().getSource());
			if (distance >= 0.0d && distance < distanceThreshold) {
				// 合并类别
				newCluster.combineSamplePoints(clusterDistanceWritable.get().getTarget());
				ClusterWritable clusterWritable = new ClusterWritable(newCluster);
				context.write(NullWritable.get(), clusterWritable);

				// 另一类别也标识为已处理
				processedSet.add(clusterDistanceWritable.get().getTarget().getId());
			} else {
				ClusterWritable clusterWritable = new ClusterWritable(newCluster);
				context.write(NullWritable.get(), clusterWritable);
			}
		}
	}

}
