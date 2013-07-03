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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.DistanceMeasure;

import cn.macthink.pagenes.model.PAgenesCluster;
import cn.macthink.pagenes.model.PAgenesClusterDistance;
import cn.macthink.pagenes.util.PAgenesConfigKeys;
import cn.macthink.pagenes.util.partitionsort.PartitionSortKeyPair;

import com.google.common.collect.Lists;

/**
 * CalculateClustersDistanceReducer
 * 
 * @author Macthink
 */
public class ComputeClustersDistanceReducer extends
		Reducer<IntWritable, PAgenesCluster, PartitionSortKeyPair, PAgenesClusterDistance> {

	/**
	 * 最后一个类别自己到自己的距离（理论上应该为零，这里特殊处理，设置一个最大的距离，防止排序时将其排在前面）
	 */
	private static double lastClusterSelfDistance = Double.MAX_VALUE;

	/**
	 * 距离度量
	 */
	private DistanceMeasure distanceMeasure;

	/**
	 * setup
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		distanceMeasure = ClassUtils.instantiateAs(
				context.getConfiguration().get(PAgenesConfigKeys.DISTANCE_MEASURE_KEY), DistanceMeasure.class);
	}

	/**
	 * reduce
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable,
	 *      org.apache.hadoop.mapreduce.Reducer.Context)
	 * @param key
	 * @param values
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(IntWritable key, Iterable<PAgenesCluster> values, Context context) throws IOException,
			InterruptedException {

		// 将本分区的values组成一个List
		List<PAgenesCluster> clusterList = Lists.newArrayList();
		Iterator<PAgenesCluster> iterator = values.iterator();
		while (iterator.hasNext()) {
			// 需要克隆一个新实例，否则总是得到相同的实例
			clusterList.add(WritableUtils.clone(iterator.next(), context.getConfiguration()));
		}

		// 将List转换成数组，方便按矩阵读取
		PAgenesCluster[] clusters = new PAgenesCluster[clusterList.size()];
		clusterList.toArray(clusters);

		// 生成类别间距离，相当于将簇间的距离矩阵的上三角部分降成一维的
		int length = clusters.length;
		PartitionSortKeyPair partitionSortKeyPair = new PartitionSortKeyPair();
		PAgenesClusterDistance clusterDistance = new PAgenesClusterDistance();
		for (int i = 0; i < (length - 1); i++) {
			PAgenesCluster currentCluster = clusters[i];
			// 寻找最近簇
			PAgenesCluster nearestCluster = clusters[i + 1];
			double nearestDistance = distanceMeasure.distance(currentCluster.getCenter(), nearestCluster.getCenter());
			for (int j = i + 2; j < length; j++) {
				PAgenesCluster tempCluster = clusters[j];
				double tempDistance = distanceMeasure.distance(currentCluster.getCenter(), tempCluster.getCenter());
				if (nearestDistance > tempDistance) {
					nearestCluster = tempCluster;
					nearestDistance = tempDistance;
				}
			}
			partitionSortKeyPair = new PartitionSortKeyPair(new DoubleWritable(nearestDistance), key);
			clusterDistance = new PAgenesClusterDistance(currentCluster, nearestCluster, nearestDistance);
			context.write(partitionSortKeyPair, clusterDistance);
		}

		// 上述生成距离的方法将最后一个类别排除，需另外加入特殊处理
		partitionSortKeyPair = new PartitionSortKeyPair(new DoubleWritable(lastClusterSelfDistance), key);
		PAgenesCluster lastCluster = clusters[clusters.length - 1];
		clusterDistance = new PAgenesClusterDistance(lastCluster, lastCluster, lastClusterSelfDistance);
		context.write(partitionSortKeyPair, clusterDistance);
	}

}
