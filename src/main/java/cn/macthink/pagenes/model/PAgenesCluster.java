/**
 * Project:cn.macthink.pagenes
 * File Created at 2013年6月30日
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.model;

import java.util.Collection;

import org.apache.mahout.clustering.iterator.DistanceMeasureCluster;
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
	 * Constructor
	 */
	public PAgenesCluster() {
		super();
		vectorNames = Lists.newArrayList();
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
		this();
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

}
