/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-16
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes.util.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * IdentityMapper
 * 
 * @author Macthink
 */
public class IdentityMapper extends Mapper<Writable, Writable, Writable, Writable> {

	@Override
	protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}

}
