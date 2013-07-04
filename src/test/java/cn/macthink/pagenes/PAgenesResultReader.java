/**
 * Project:cn.macthink.pagenes
 * File Created at 2013年7月3日
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes;

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
 * PAgenesResultReader
 * 
 * @author Macthink
 */
public class PAgenesResultReader {

	/**
	 * main
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String basePath = Class.class.getClass().getResource("/").toString().replaceAll("file:/", "");
		// String seqFile1 = basePath + "1.init-clusters/part-m-00003";
		// String seqFile2 = basePath + "3.merge-clusters/part-r-00003";
		String seqFile3 = basePath + "3.merge-clusters/part-r-00001";

		String seqFile = seqFile3;
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "local");
		conf.set("fs.default.name", "file:///");
		URI uri = URI.create("file:///" + seqFile);
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

}
