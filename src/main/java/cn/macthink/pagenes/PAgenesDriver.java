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

import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import cn.macthink.pagenes.model.PAgenesClusterDistanceWritable;
import cn.macthink.pagenes.step1.BuildInitClustersMapper;
import cn.macthink.pagenes.step2.ComputeClustersDistanceReducer;
import cn.macthink.pagenes.step2.PartitionMapper;
import cn.macthink.pagenes.util.PAgenesConfigKeys;
import cn.macthink.pagenes.util.partitioner.KeyPartitioner;
import cn.macthink.pagenes.util.partitionsort.PartitionSortKeyPair;

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
		addOption(new DefaultOptionBuilder()
				.withLongName("processorNum")
				.withRequired(true)
				.withShortName("n")
				.withArgument(
						new ArgumentBuilder().withName("processorNum").withDefault("2").withMinimum(1).withMaximum(1)
								.create()).withDescription("The number of processors.").create());

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
		int processorNum = Integer.parseInt(getOption("processorNum"));
		if (getConf() == null) {
			setConf(new Configuration());
		}

		run(getConf(), input, output, measure, threshold, maxIterations, processorNum);
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
			int maxIterations, int processorNum) throws ClassNotFoundException, IOException, InterruptedException {

		if (log.isInfoEnabled()) {
			log.info("Input: {} Out: {} Distance: {}", new Object[] { input, output, measure.getClass().getName() });
			log.info("threshold: {} max Iterations: {} ", new Object[] { threshold, maxIterations });
		}
		Path initClusterOutput = new Path(output, "init-clusters");

		log.info("Build Init Cluster!");
		buildInitCluster(conf, input, initClusterOutput);

		log.info("Partition & Compute Clusters Distance!");
		partitionComputeClustersDistance(conf, input, initClusterOutput, measure.getClass().getName(), processorNum);

	}

	/**
	 * Step1:buildInitCluster
	 * 
	 * @param conf
	 * @param input
	 * @param output
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void buildInitCluster(Configuration conf, Path input, Path output) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = new Job(conf, "buildInitCluster");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(ClusterWritable.class);

		job.setMapperClass(BuildInitClustersMapper.class);
		job.setNumReduceTasks(0);
		job.setJarByClass(PAgenesDriver.class);

		FileInputFormat.setInputPaths(job, input);
		HadoopUtil.delete(conf, output);
		FileOutputFormat.setOutputPath(job, output);

		if (!job.waitForCompletion(true)) {
			throw new InterruptedException("buildInitCluster failed");
		}
	}

	public static void partitionComputeClustersDistance(Configuration conf, Path input, Path output,
			String measureClass, int processorNum) throws IOException, InterruptedException, ClassNotFoundException {
		conf.setInt(PAgenesConfigKeys.PROCESSOR_NUM_KEY, processorNum);
		conf.set(PAgenesConfigKeys.DISTANCE_MEASURE_KEY, measureClass);

		Job job = new Job(conf, "partitionComputeClustersDistance");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(ClusterWritable.class);
		job.setOutputKeyClass(PartitionSortKeyPair.class);
		job.setOutputValueClass(PAgenesClusterDistanceWritable.class);

		job.setMapperClass(PartitionMapper.class);
		job.setReducerClass(ComputeClustersDistanceReducer.class);
		job.setPartitionerClass(KeyPartitioner.class);
		job.setNumReduceTasks(processorNum);
		job.setJarByClass(PAgenesDriver.class);

		FileInputFormat.setInputPaths(job, input);
		HadoopUtil.delete(conf, output);
		FileOutputFormat.setOutputPath(job, output);

		if (!job.waitForCompletion(true)) {
			throw new InterruptedException("buildInitCluster failed");
		}
	}
}
