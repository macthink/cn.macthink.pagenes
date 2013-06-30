/**
 * Project:cn.macthink.pagenes
 * File Created at 2013年6月30日
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes;

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
public class ClusterDistanceWritable implements WritableComparable<ClusterDistance> {

	/**
	 * ClusterDistance
	 */
	private ClusterDistance clusterDistance;

	/**
	 * Constructor
	 */
	public ClusterDistanceWritable() {
		super();
	}

	/**
	 * Constructor
	 * 
	 * @param clusterDistance
	 */
	public ClusterDistanceWritable(ClusterDistance clusterDistance) {
		super();
		this.clusterDistance = clusterDistance;
	}

	/**
	 * @return the clusterDistance
	 */
	public ClusterDistance get() {
		return clusterDistance;
	}

	/**
	 * @param clusterDistance
	 *            the clusterDistance to set
	 */
	public void set(ClusterDistance clusterDistance) {
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
		clusterDistance = new ClusterDistance();
		clusterDistance.setDistance(in.readDouble());
		ClusterWritable clusterWritable = new ClusterWritable();
		clusterWritable.readFields(in);
		clusterDistance.setSource(clusterWritable.getValue());
		clusterWritable = new ClusterWritable();
		clusterWritable.readFields(in);
		clusterDistance.setTarget(clusterWritable.getValue());
	}

	@Override
	public int compareTo(ClusterDistance o) {
		return clusterDistance.compareTo(o);
	}

	@Override
	public int hashCode() {
		return clusterDistance.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return (obj instanceof ClusterDistanceWritable)
				&& clusterDistance.equals(((ClusterDistanceWritable) obj).get());
	}

	@Override
	public String toString() {
		return clusterDistance.toString();
	}

}
