/**
 * Project:cn.macthink.pagenes
 * File Created at 2013年6月30日
 * Auther:Macthink
 * 
 * Copyright 2013 Macthink.cn.
 * All rights reserved.
 */
package cn.macthink.pagenes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.macthink.pagenes.step1.BuildInitClustersMapper;

/**
 * PAgenesDriver
 * 
 * @author Macthink
 */
public class PAgenesDriver extends AbstractJob {

	private static final Logger log = LoggerFactory.getLogger(PAgenesDriver.class);

	/**
	 * main
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new PAgenesDriver(), args);
	}

	/**
	 * run
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public int run(String[] args) throws Exception {

		addInputOption();
		addOutputOption();
		addOption(DefaultOptionCreator.distanceMeasureOption().create());
		addOption(DefaultOptionCreator.thresholdOption().create());
		addOption(DefaultOptionCreator.maxIterationsOption().create());
		addOption(DefaultOptionCreator.overwriteOption().create());

		if (parseArguments(args) == null) {
			return -1;
		}

		Path input = getInputPath();
		Path output = getOutputPath();
		String measureClass = getOption(DefaultOptionCreator.DISTANCE_MEASURE_OPTION);
		if (measureClass == null) {
			measureClass = CosineDistanceMeasure.class.getName();
		}
		DistanceMeasure measure = ClassUtils.instantiateAs(measureClass, DistanceMeasure.class);
		double threshold = Double.parseDouble(getOption(DefaultOptionCreator.THRESHOLD_OPTION));
		int maxIterations = Integer.parseInt(getOption(DefaultOptionCreator.MAX_ITERATIONS_OPTION));
		if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
			HadoopUtil.delete(getConf(), output);
		}
		if (getConf() == null) {
			setConf(new Configuration());
		}

		run(getConf(), input, output, measure, threshold, maxIterations);
		return 0;
	}

	/**
	 * run
	 * 
	 * @param conf
	 * @param input
	 *            the directory pathname for input points
	 * @param output
	 *            the directory pathname for output points
	 * @param measure
	 *            the DistanceMeasure to use
	 * @param threshold
	 *            the threshold distance of clusters
	 * @param maxIterations
	 *            the maximum number of iterations
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void run(Configuration conf, Path input, Path output, DistanceMeasure measure, double threshold,
			int maxIterations) throws ClassNotFoundException, IOException, InterruptedException {

		if (log.isInfoEnabled()) {
			log.info("Input: {} Out: {} Distance: {}", new Object[] { input, output, measure.getClass().getName() });
			log.info("threshold: {} max Iterations: {} ", new Object[] { threshold, maxIterations });
		}
		Path initClusterOutput = new Path(output, "init-clusters");
		generateInitCluster(conf, input, initClusterOutput);

	}

	public static void generateInitCluster(Configuration conf, Path input, Path output) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = new Job(conf, "generateInitCluster");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(ClusterWritable.class);

		FileInputFormat.setInputPaths(job, input);
		HadoopUtil.delete(conf, output);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(BuildInitClustersMapper.class);
		job.setNumReduceTasks(0);
		job.setJarByClass(PAgenesDriver.class);

		if (!job.waitForCompletion(true)) {
			throw new InterruptedException("generateInitCluster failed");
		}
	}

}
