/**
 * Project:hadoop-tdt-clustering
 * File Created at 2013-6-15
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.common.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * SequenceFileUtils
 * 
 * @author Macthink
 */
public class SequenceFileUtils {

	/**
	 * readFile
	 * 
	 * @param conf
	 * @param fileUri
	 * @throws IOException
	 */
	public static void readLocalFile(String fileUri) throws IOException {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "local");
		conf.set("fs.default.name", "file:///");
		readFile(conf, "file:///" + fileUri);
	}

	/**
	 * readFile
	 * 
	 * @param fileUri
	 * @throws IOException
	 */
	public static void readFile(Configuration conf, String fileUri) throws IOException {
		URI uri = URI.create(fileUri);
		FileSystem fs = FileSystem.get(uri, conf);
		Path path = new Path(uri);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while (reader.next(key, value)) {
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
				position = reader.getPosition();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
	}

	/**
	 * main
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String basePath = Class.class.getClass().getResource("/").toString().replaceAll("file:/", "");
		String seqFile = basePath + "clusters-9/part-r-00000";
		SequenceFileUtils.readLocalFile(seqFile);
	}
}
