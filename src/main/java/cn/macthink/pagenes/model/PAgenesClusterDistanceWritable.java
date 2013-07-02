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
import org.apache.mahout.clustering.iterator.ClusterWritable;

/**
 * ClusterDistanceWritable
 * 
 * @author Macthink
 */
public class PAgenesClusterDistanceWritable implements WritableComparable<PAgenesClusterDistance> {

	/**
	 * ClusterDistance
	 */
	private PAgenesClusterDistance clusterDistance;

	/**
	 * Constructor
	 */
	public PAgenesClusterDistanceWritable() {
		super();
	}

	/**
	 * Constructor
	 * 
	 * @param clusterDistance
	 */
	public PAgenesClusterDistanceWritable(PAgenesClusterDistance clusterDistance) {
		super();
		this.clusterDistance = clusterDistance;
	}

	/**
	 * @return the clusterDistance
	 */
	public PAgenesClusterDistance get() {
		return clusterDistance;
	}

	/**
	 * @param clusterDistance
	 *            the clusterDistance to set
	 */
	public void set(PAgenesClusterDistance clusterDistance) {
		this.clusterDistance = clusterDistance;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(clusterDistance.getDistance());
		ClusterWritable clusterWritable = new ClusterWritable();
		clusterWritable.setValue(clusterDistance.getSource());
		clusterWritable.write(out);
		clusterWritable = new ClusterWritable();
		clusterWritable.setValue(clusterDistance.getTarget());
		clusterWritable.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		clusterDistance = new PAgenesClusterDistance();
		clusterDistance.setDistance(in.readDouble());
		ClusterWritable clusterWritable = new ClusterWritable();
		clusterWritable.readFields(in);
		clusterDistance.setSource((PAgenesCluster) clusterWritable.getValue());
		clusterWritable = new ClusterWritable();
		clusterWritable.readFields(in);
		clusterDistance.setTarget((PAgenesCluster) clusterWritable.getValue());
	}

	@Override
	public int compareTo(PAgenesClusterDistance o) {
		return clusterDistance.compareTo(o);
	}

	@Override
	public int hashCode() {
		return clusterDistance.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return (obj instanceof PAgenesClusterDistanceWritable)
				&& clusterDistance.equals(((PAgenesClusterDistanceWritable) obj).get());
	}

	@Override
	public String toString() {
		return clusterDistance.toString();
	}

}
