/**
 * Project:cn.macthink.pagenes
 * File Created at 2013年6月30日
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.step2;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import cn.macthink.common.util.StatisticUtils;
import cn.macthink.pagenes.model.PAgenesCluster;

import com.google.common.base.Preconditions;

/**
 * PartitionPolicy
 * 
 * @author Macthink
 */
public class PartitionPolicy {

	/**
	 * 获得某个给定簇将被划分的分区编号
	 * 
	 * @param cluster
	 * @param processorNum
	 * @return
	 */
	public static int getPartitionNum(PAgenesCluster cluster, int processorNum) {

		Preconditions.checkNotNull(cluster);

		// 获得类别的质心
		Vector centroid = cluster.getCenter();

		// 分块
		int blockNum = processorNum; // 块数
		int dimensionOfBlock = centroid.size() / processorNum; // 每个块中的维数，最后一块可能比较小
		double[][] blocks = new double[blockNum][dimensionOfBlock];
		double[] eigenvalues = new double[centroid.size()];
		Iterator<Element> iterable = centroid.iterator();
		for (int i = 0; iterable.hasNext(); i++) {
			eigenvalues[i] = iterable.next().get();
		}
		int pointer = 0;
		for (int i = 0; i < blockNum; i++) {
			blocks[i] = Arrays.copyOfRange(eigenvalues, pointer, pointer + dimensionOfBlock);
			pointer += dimensionOfBlock;
		}

		// 统计各块的非零元素和方差
		int nonZeroNums = 0;
		double variances = 0;
		int partitionNum = 1; // 将被划分到的分区编号
		double fitness = 0.0d;
		double maxFitness = 0.0d; // 最大适应度
		for (int i = 0; i < blocks.length; i++) {
			double[] block = blocks[i];
			// 统计非零元素
			nonZeroNums = 0;
			for (int j = 0; j < block.length; j++) {
				if (block[j] != 0) {
					nonZeroNums++;
				}
			}
			// 统计方差
			variances = StatisticUtils.getVariance(block);
			// 计算适应度并比较
			fitness = nonZeroNums / variances;
			if (fitness > maxFitness) {
				maxFitness = fitness;
				partitionNum = i + 1;
			}
		}

		return partitionNum;
	}
}
