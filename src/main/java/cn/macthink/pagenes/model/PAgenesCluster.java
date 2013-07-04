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
import java.util.Collection;
import java.util.UUID;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;

import com.google.common.collect.Lists;

/**
 * PAgenesCluster
 * 
 * @author Macthink
 */
public class PAgenesCluster implements Writable {

	/**
	 * the identifier of cluster
	 */
	private String id;

	/**
	 * the sum of points
	 */
	private Vector pointSum;

	/**
	 * the center of cluster equal pointSum / points.size()
	 */
	private Vector center;
	/**
	 * the vector in the cluster
	 */
	private Collection<NamedVector> points;

	/**
	 * Constructor: for (de)serialization as a Writable
	 */
	public PAgenesCluster() {
	}

	/**
	 * Constructor: for build init clusters
	 * 
	 * @param point
	 */
	public PAgenesCluster(NamedVector point) {
		this.id = UUID.randomUUID().toString();
		this.points = Lists.newArrayList();
		this.points.add(point);
		this.pointSum = point;
		this.center = point;
	}

	/**
	 * Constructor: for merge cluster
	 * 
	 * @param other
	 */
	public PAgenesCluster(PAgenesCluster other) {
		this.id = UUID.randomUUID().toString();
		this.points = other.points;
		this.pointSum = other.pointSum;
		this.center = other.center;
	}

	/**
	 * addVector
	 * 
	 * @param point
	 */
	public void addVector(NamedVector point) {
		if (this.points == null) {
			this.points = Lists.newArrayList();
		}
		this.points.add(point);
		this.pointSum.plus(point);
		this.center = this.pointSum;
		this.center.assign(Functions.DIV, points.size());
	}

	/**
	 * combine
	 * 
	 * @param other
	 */
	public void combine(PAgenesCluster other) {
		this.id = UUID.randomUUID().toString();
		if (other.getPoints() != null) {
			if (this.points == null) {
				this.points = other.getPoints();
				this.pointSum = other.pointSum;
				this.center = other.center;
			} else {
				for (NamedVector vector : other.getPoints()) {
					this.points.add(vector);
					this.pointSum.plus(vector);
				}
				this.center = this.pointSum;
				this.center.assign(Functions.DIV, points.size());
			}
		}
	}

	/**
	 * readFields
	 * 
	 * @see org.apache.mahout.clustering.DistanceMeasureCluster#readFields(java.io.DataInput)
	 * @param in
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readUTF();
		pointSum = VectorWritable.readVector(in);
		center = VectorWritable.readVector(in);
		long numPoints = in.readLong();
		points = Lists.newArrayList();
		for (int i = 0; i < numPoints; i++) {
			points.add((NamedVector) VectorWritable.readVector(in));
		}
	}

	/**
	 * write
	 * 
	 * @see org.apache.mahout.clustering.DistanceMeasureCluster#write(java.io.DataOutput)
	 * @param out
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		VectorWritable.writeVector(out, pointSum);
		VectorWritable.writeVector(out, center);
		long numPoints = 0;
		out.writeLong(numPoints);
		if (points != null && points.size() != 0) {
			numPoints = points.size();
			for (NamedVector vector : points) {
				VectorWritable.writeVector(out, vector);
			}
		}
	}

	/**
	 * toString
	 * 
	 * @see java.lang.Object#toString()
	 * @return
	 */
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the pointSum
	 */
	public Vector getPointSum() {
		return pointSum;
	}

	/**
	 * @param pointSum
	 *            the pointSum to set
	 */
	public void setPointSum(Vector pointSum) {
		this.pointSum = pointSum;
	}

	/**
	 * @return the center
	 */
	public Vector getCenter() {
		return center;
	}

	/**
	 * @param center
	 *            the center to set
	 */
	public void setCenter(Vector center) {
		this.center = center;
	}

	/**
	 * @return the points
	 */
	public Collection<NamedVector> getPoints() {
		return points;
	}

	/**
	 * @param points
	 *            the points to set
	 */
	public void setPoints(Collection<NamedVector> points) {
		this.points = points;
	}

}
