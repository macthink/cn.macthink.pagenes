/**
 * Project:cn.macthink.pagenes
 * File Created at 2013年6月30日
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * CategoriesDistance
 * 
 * @author Macthink
 */
public class PAgenesClusterDistance implements WritableComparable<PAgenesClusterDistance> {

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
	 * Constructor
	 */
	public PAgenesClusterDistance() {
	}

	/**
	 * Constructor
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
		return "PAgenesClusterDistance [ " + source.getId() + "\t" + target.getId() + "\t" + distance + "]";
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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(getDistance());
		getSource().write(out);
		getTarget().write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		setDistance(in.readDouble());
		setSource(new PAgenesCluster());
		getSource().readFields(in);
		setTarget(new PAgenesCluster());
		getTarget().readFields(in);
	}

}
