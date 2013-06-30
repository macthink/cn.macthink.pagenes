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
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.DistanceMeasure;

import cn.macthink.pagenes.ClusterDistance;
import cn.macthink.pagenes.ClusterDistanceWritable;
import cn.macthink.pagenes.util.PAgenesConfigKeys;
import cn.macthink.pagenes.util.partitionsort.PartitionSortKeyPair;

import com.google.common.collect.Lists;

/**
 * CalculateClustersDistanceReducer
 * 
 * @author Macthink
 */
public class ComputeClustersDistanceReducer extends
		Reducer<IntWritable, ClusterWritable, PartitionSortKeyPair, ClusterDistanceWritable> {

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
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(IntWritable key, Iterable<ClusterWritable> values, Context context) throws IOException,
			InterruptedException {

		// 将所有的values组成一个List
		List<ClusterWritable> clusterWritableList = Lists.newArrayList();
		Iterator<ClusterWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			// 需要克隆一个新实例，否则总是得到相同的实例
			clusterWritableList.add(WritableUtils.clone(iterator.next(), context.getConfiguration()));
		}

		// 将List转换成数组，方便按矩阵读取
		ClusterWritable[] clusterWritables = new ClusterWritable[clusterWritableList.size()];
		clusterWritableList.toArray(clusterWritables);

		// 生成类别间距离
		int length = clusterWritables.length;
		for (int i = 0; i < (length - 1); i++) {
			ClusterWritable minClusterWritable = clusterWritables[i + 1];
			double minDistance = distanceMeasure.distance(clusterWritables[i].getValue().getCenter(),
					clusterWritables[i + 1].getValue().getCenter());
			for (int j = i + 2; j < length; j++) {
				double distance = distanceMeasure.distance(clusterWritables[i].getValue().getCenter(),
						clusterWritables[j].getValue().getCenter());
				if (minDistance > distance) {
					minClusterWritable = clusterWritables[j];
					minDistance = distance;
				}
			}
			PartitionSortKeyPair partitionSortKeyPair = new PartitionSortKeyPair(new DoubleWritable(minDistance), key);
			ClusterDistance clusterDistance = new ClusterDistance(clusterWritables[i].getValue(),
					minClusterWritable.getValue(), minDistance);
			ClusterDistanceWritable clusterDistanceWritable = new ClusterDistanceWritable(clusterDistance);
			context.write(partitionSortKeyPair, clusterDistanceWritable);
		}

		// 上述生成距离的方法将最后一个类别排除，需另外加入特殊处理
		PartitionSortKeyPair partitionSortKeyPair = new PartitionSortKeyPair(
				new DoubleWritable(lastClusterSelfDistance), key);
		ClusterDistance clusterDistance = new ClusterDistance(clusterWritables[clusterWritables.length - 1].getValue(),
				clusterWritables[clusterWritables.length - 1].getValue(), lastClusterSelfDistance);
		ClusterDistanceWritable clusterDistanceWritable = new ClusterDistanceWritable(clusterDistance);
		context.write(partitionSortKeyPair, clusterDistanceWritable);
	}

}