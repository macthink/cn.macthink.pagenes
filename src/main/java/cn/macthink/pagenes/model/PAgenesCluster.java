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

import org.apache.mahout.clustering.DistanceMeasureCluster;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.NamedVector;

import com.google.common.collect.Lists;

/**
 * PAgenesCluster
 * 
 * @author Macthink
 */
public class PAgenesCluster extends DistanceMeasureCluster {

	private Collection<String> vectorNames;

	/**
	 * Constructor: for (de)serialization as a Writable
	 */
	public PAgenesCluster() {
	}

	/**
	 * Constructor
	 * 
	 * @param point
	 * @param clusterId
	 * @param measure
	 */
	public PAgenesCluster(NamedVector point, int clusterId, DistanceMeasure measure) {
		super(point, clusterId, measure);
		vectorNames = Lists.newArrayList();
		vectorNames.add(point.getName());
	}

	/**
	 * Constructor
	 * 
	 * @param other
	 */
	public PAgenesCluster(PAgenesCluster other) {
		setNumPoints(other.getNumPoints());
		setCenter(other.getCenter());
		setRadius(other.getRadius());
		setMeasure(other.getMeasure());
		setId(other.getId());
		setVectorNames(other.vectorNames);
	}

	/**
	 * addVector
	 * 
	 * @param point
	 */
	public void addVector(NamedVector point) {
		this.vectorNames.add(point.getName());
	}

	/**
	 * combine
	 * 
	 * @param other
	 */
	public void combine(PAgenesCluster other) {
		if (other.getVectorNames() != null && other.getVectorNames().size() != 0) {
			for (String vectorName : other.getVectorNames()) {
				this.vectorNames.add(vectorName);
			}
		}
	}

	/**
	 * @return the vectorNames
	 */
	public Collection<String> getVectorNames() {
		return vectorNames;
	}

	/**
	 * @param vectorNames
	 *            the vectorNames to set
	 */
	public void setVectorNames(Collection<String> vectorNames) {
		this.vectorNames = vectorNames;
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
		super.readFields(in);
		vectorNames = Lists.newArrayList();
		int vectorNameSize = in.readInt();
		for (int i = 0; i < vectorNameSize; i++) {
			vectorNames.add(in.readUTF());
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
		super.write(out);
		int vectorNamesSize = 0;
		out.writeInt(vectorNamesSize);
		if (vectorNames != null && vectorNames.size() != 0) {
			vectorNamesSize = vectorNames.size();
			for (String vectorName : vectorNames) {
				out.writeUTF(vectorName);
			}
		}
	}

}
