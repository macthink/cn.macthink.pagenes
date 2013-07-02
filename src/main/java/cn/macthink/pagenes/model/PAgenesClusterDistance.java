/**
 * Project:cn.macthink.pagenes
 * File Created at 2013年6月30日
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.model;

/**
 * CategoriesDistance
 * 
 * @author Macthink
 */
public class PAgenesClusterDistance implements Comparable<PAgenesClusterDistance> {

	/**
	 * 源
	 */
	private PAgenesCluster source;

	/**
	 * 目标
	 */
	private PAgenesCluster target;

	/**
	 * 它们间的距离
	 */
	private double distance;

	/**
	 * 构造函数
	 */
	public PAgenesClusterDistance() {
		super();
	}

	/**
	 * 构造函数
	 * 
	 * @param source
	 * @param target
	 * @param distance
	 */
	public PAgenesClusterDistance(PAgenesCluster source, PAgenesCluster target, double distance) {
		super();
		this.source = source;
		this.target = target;
		this.distance = distance;
	}

	@Override
	public int compareTo(PAgenesClusterDistance o) {
		return Double.compare(this.distance, o.distance);
	}

	@Override
	public String toString() {
		return "CategoriesDistance [ " + source.getId() + "\t" + target.getId() + "\t" + distance + "]";
	}

	/**
	 * @return the source
	 */
	public PAgenesCluster getSource() {
		return source;
	}

	/**
	 * @param source
	 *            the source to set
	 */
	public void setSource(PAgenesCluster source) {
		this.source = source;
	}

	/**
	 * @return the target
	 */
	public PAgenesCluster getTarget() {
		return target;
	}

	/**
	 * @param target
	 *            the target to set
	 */
	public void setTarget(PAgenesCluster target) {
		this.target = target;
	}

	/**
	 * @return the distance
	 */
	public double getDistance() {
		return distance;
	}

	/**
	 * @param distance
	 *            the distance to set
	 */
	public void setDistance(double distance) {
		this.distance = distance;
	}

}
