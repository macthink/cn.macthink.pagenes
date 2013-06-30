/**
 * Project:cn.macthink.pagenes
 * File Created at 2013年6月30日
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes;

import org.apache.mahout.clustering.Cluster;

/**
 * CategoriesDistance
 * 
 * @author Macthink
 */
public class ClusterDistance implements Comparable<ClusterDistance> {

	/**
	 * 源
	 */
	private Cluster source;

	/**
	 * 目标
	 */
	private Cluster target;

	/**
	 * 它们间的距离
	 */
	private double distance;

	/**
	 * 构造函数
	 */
	public ClusterDistance() {
		super();
	}

	/**
	 * 构造函数
	 * 
	 * @param source
	 * @param target
	 * @param distance
	 */
	public ClusterDistance(Cluster source, Cluster target, double distance) {
		super();
		this.source = source;
		this.target = target;
		this.distance = distance;
	}

	@Override
	public int compareTo(ClusterDistance o) {
		return Double.compare(this.distance, o.distance);
	}

	@Override
	public String toString() {
		return "CategoriesDistance [ " + source.getId() + "\t" + target.getId() + "\t" + distance + "]";
	}

	/**
	 * @return the source
	 */
	public Cluster getSource() {
		return source;
	}

	/**
	 * @return the target
	 */
	public Cluster getTarget() {
		return target;
	}

	/**
	 * @return the distance
	 */
	public double getDistance() {
		return distance;
	}

	/**
	 * @param source
	 *            the source to set
	 */
	public void setSource(Cluster source) {
		this.source = source;
	}

	/**
	 * @param target
	 *            the target to set
	 */
	public void setTarget(Cluster target) {
		this.target = target;
	}

	/**
	 * @param distance
	 *            the distance to set
	 */
	public void setDistance(double distance) {
		this.distance = distance;
	}

}
