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

import cn.macthink.pagenes.model.PAgenesCluster;
import cn.macthink.pagenes.model.PAgenesClusterDistance;
import cn.macthink.pagenes.util.PAgenesConfigKeys;
import cn.macthink.pagenes.util.partitionsort.PartitionSortKeyPair;

/**
 * MergeClustersReducer
 * 
 * @author Macthink
 */
public class MergeClustersReducer extends
		Reducer<PartitionSortKeyPair, PAgenesClusterDistance, NullWritable, PAgenesCluster> {

	/**
	 * 类别间距离阈值
	 */
	private double distanceThreshold;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		distanceThreshold = Double
				.parseDouble(context.getConfiguration().get(PAgenesConfigKeys.DISTANCE_THRESHOLD_KEY));
	}

	@Override
	protected void reduce(PartitionSortKeyPair key, Iterable<PAgenesClusterDistance> values, Context context)
			throws IOException, InterruptedException {
		// 从小到大遍历，如果小于距离阈值则合并类别
		Set<String> processedSet = new HashSet<String>();
		Iterator<PAgenesClusterDistance> iterator = values.iterator();
		while (iterator.hasNext()) {
			PAgenesClusterDistance clusterDistance = iterator.next();
			// 判断是否与其他类别合并过
			if (processedSet.contains(clusterDistance.getSource().getId())) {
				continue;
			}
			// 当前类别标识为已处理
			processedSet.add(clusterDistance.getSource().getId());
			// 尚未与其他类别合并过
			double distance = clusterDistance.getDistance();
			PAgenesCluster newCluster = new PAgenesCluster((PAgenesCluster) clusterDistance.getSource());
			if (distance >= 0.0d && distance < distanceThreshold) {
				// 合并类别
				newCluster.combine((PAgenesCluster) clusterDistance.getTarget());
				context.write(NullWritable.get(), newCluster);
				// 另一类别也标识为已处理
				processedSet.add(clusterDistance.getTarget().getId());
			} else {
				context.write(NullWritable.get(), newCluster);
			}
		}
	}

}
