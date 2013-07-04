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

import cn.macthink.common.util.SequenceFileUtils;

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
		String seqFile = basePath + "3.merge-clusters/part-r-00003";
		SequenceFileUtils.readLocalFile(seqFile);
	}

}
