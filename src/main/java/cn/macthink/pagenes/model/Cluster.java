/**
 * Project:cn.macthink.pagenes
 * File Created at 2013年6月30日
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.model;

import org.apache.mahout.clustering.iterator.DistanceMeasureCluster;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

/**
 * Cluster
 * 
 * @author Macthink
 */
public class Cluster extends DistanceMeasureCluster {

	/**
	 * Constructor
	 */
	public Cluster() {
		super();
	}

	/**
	 * Constructor
	 * 
	 * @param point
	 * @param clusterId
	 * @param measure
	 */
	public Cluster(Vector point, int clusterId, DistanceMeasure measure) {
		super(point, clusterId, measure);
	}

}
