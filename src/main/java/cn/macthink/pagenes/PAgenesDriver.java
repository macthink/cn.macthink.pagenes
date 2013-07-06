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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.macthink.pagenes.model.PAgenesCluster;
import cn.macthink.pagenes.model.PAgenesClusterDistance;
import cn.macthink.pagenes.step1.BuildInitClustersMapper;
import cn.macthink.pagenes.step2.ComputeClustersDistanceReducer;
import cn.macthink.pagenes.step2.PartitionMapper;
import cn.macthink.pagenes.step3.MergeClustersReducer;
import cn.macthink.pagenes.util.PAgenesConfigKeys;
import cn.macthink.pagenes.util.mapper.IdentityMapper;
import cn.macthink.pagenes.util.partitioner.KeyPartitioner;
import cn.macthink.pagenes.util.partitionsort.PartitionSortKeyComparator;
import cn.macthink.pagenes.util.partitionsort.PartitionSortKeyPair;
import cn.macthink.pagenes.util.partitionsort.PartitionSortKeyPairComparator;
import cn.macthink.pagenes.util.partitionsort.PartitionSortKeyPairPartitioner;

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
		addOption(new DefaultOptionBuilder().withLongName("showClusterNumbers").withRequired(false).withShortName("sn")
				.withDescription("If present, show the number of cluster after each iteration.").create());
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
		boolean showClusterNum = false;
		if (hasOption("showClusterNumbers")) {
			showClusterNum = true;
		}
		int processorNum = Integer.parseInt(getOption("processorNum"));
		if (getConf() == null) {
			setConf(new Configuration());
		}

		run(getConf(), input, output, measure, threshold, maxIterations, showClusterNum, processorNum);
		return 0;
	}

	/**
	 * run
	 * 
	 * @param conf
	 * @param input
	 * @param output
	 * @param measure
	 * @param threshold
	 * @param maxIterations
	 * @param showClusterNum
	 * @param processorNum
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void run(Configuration conf, Path input, Path output, DistanceMeasure measure, double threshold,
			int maxIterations, boolean showClusterNum, int processorNum) throws ClassNotFoundException, IOException,
			InterruptedException {
		if (log.isInfoEnabled()) {
			log.info("Input: {}", input);
			log.info("Out: {}", output);
			log.info("Distance: {}", measure.getClass().getName());
			log.info("Threshold: {}", threshold);
			log.info("Iterations: {} ", maxIterations);
			log.info("ProcessNum: {} ", processorNum);
		}
		// start counter1
		long start1 = System.currentTimeMillis();

		// build init cluster
		log.info("Step1: Build Init Cluster");
		Path clustersIn = new Path(output, "init-clusters");
		buildInitCluster(conf, input, clustersIn, measure.getClass().getName());

		// start counter2
		long start2 = System.currentTimeMillis();

		// start iteration
		int iteration = 1;
		boolean converged = false;
		for (; !converged && iteration <= maxIterations; iteration++) {
			Path lastIterationPath = new Path(output, "iteration" + (iteration - 1));
			Path iterationPath = new Path(output, "iteration" + iteration);

			log.info("Iteration:{}, Step2: Partition & Compute Clusters Distance", iteration);
			Path clustersDistance = new Path(iterationPath, "clusters-distance");
			partitionComputeClustersDistance(conf, clustersIn, clustersDistance, measure.getClass().getName(),
					processorNum);

			log.info("Iteration:{}, Step3: Merge Clusters", iteration);
			Path clustersOut = new Path(iterationPath, "clusters");
			Path lastIterationClustersOut = new Path(lastIterationPath, "merge-clusters");
			converged = mergeClusters(conf, clustersDistance, clustersOut, lastIterationClustersOut, threshold,
					processorNum);
			clustersIn = clustersOut;

			if (log.isInfoEnabled() && showClusterNum) {
				FileSystem fs = FileSystem.get(clustersOut.toUri(), conf);
				long numClusters = countClustesNum(clustersOut, conf, fs);
				log.info("Number of clusters: {}", numClusters);
			}
		}
		if (log.isInfoEnabled()) {
			log.info("Program took {} ms (Minutes: {})", System.currentTimeMillis() - start1,
					(System.currentTimeMillis() - start1) / 60000.0);
			log.info("Iterations took {} ms (Minutes: {})", System.currentTimeMillis() - start2,
					(System.currentTimeMillis() - start2) / 60000.0);
		}

		Path finalClustersIn = new Path(output, "iteration" + (iteration - 1) + "-final");
		FileSystem.get(conf).rename(new Path(output, "iteration" + (iteration - 1)), finalClustersIn);

		// print the result
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
	public static void buildInitCluster(Configuration conf, Path input, Path output, String measureClass)
			throws IOException, ClassNotFoundException, InterruptedException {

		Job job = new Job(conf, "buildInitCluster");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(PAgenesCluster.class);

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

	/**
	 * Step2:partitionComputeClustersDistance
	 * 
	 * @param conf
	 * @param input
	 * @param output
	 * @param measureClass
	 * @param processorNum
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void partitionComputeClustersDistance(Configuration conf, Path input, Path output,
			String measureClass, int processorNum) throws IOException, InterruptedException, ClassNotFoundException {
		conf.setInt(PAgenesConfigKeys.PROCESSOR_NUM_KEY, processorNum);
		conf.set(PAgenesConfigKeys.DISTANCE_MEASURE_KEY, measureClass);

		Job job = new Job(conf, "partitionComputeClustersDistance");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PAgenesCluster.class);
		job.setOutputKeyClass(PartitionSortKeyPair.class);
		job.setOutputValueClass(PAgenesClusterDistance.class);

		job.setMapperClass(PartitionMapper.class);
		job.setReducerClass(ComputeClustersDistanceReducer.class);
		job.setPartitionerClass(KeyPartitioner.class);
		job.setNumReduceTasks(processorNum);
		job.setJarByClass(PAgenesDriver.class);

		FileInputFormat.setInputPaths(job, input);
		HadoopUtil.delete(conf, output);
		FileOutputFormat.setOutputPath(job, output);

		if (!job.waitForCompletion(true)) {
			throw new InterruptedException("partitionComputeClustersDistance failed");
		}
	}

	/**
	 * Step3:mergeClusters
	 * 
	 * @param conf
	 * @param input
	 * @param output
	 * @param lastOutput
	 * @param threshold
	 * @param processorNum
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static boolean mergeClusters(Configuration conf, Path input, Path output, Path lastOutput, double threshold,
			int processorNum) throws IOException, ClassNotFoundException, InterruptedException {
		conf.set(PAgenesConfigKeys.DISTANCE_THRESHOLD_KEY, Double.toString(threshold));

		Job job = new Job(conf, "mergeClusters");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(PartitionSortKeyPair.class);
		job.setMapOutputValueClass(PAgenesClusterDistance.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(PAgenesCluster.class);

		job.setMapperClass(IdentityMapper.class);
		job.setPartitionerClass(PartitionSortKeyPairPartitioner.class);
		job.setReducerClass(MergeClustersReducer.class);

		// secondary sort
		job.setSortComparatorClass(PartitionSortKeyPairComparator.class);
		job.setGroupingComparatorClass(PartitionSortKeyComparator.class);

		job.setNumReduceTasks(processorNum);
		job.setJarByClass(PAgenesDriver.class);

		FileInputFormat.setInputPaths(job, input);
		HadoopUtil.delete(conf, output);
		FileOutputFormat.setOutputPath(job, output);

		if (!job.waitForCompletion(true)) {
			throw new InterruptedException("mergeClusters failed");
		}

		return false;
		// FileSystem fs = FileSystem.get(lastOutput.toUri(), conf);
		// return isConverged(output, lastOutput, conf, fs);
	}

	/**
	 * isConverged
	 * 
	 * @param currentOutput
	 * @param lastOutput
	 * @param conf
	 * @param fs
	 * @return
	 * @throws IOException
	 */
	public static boolean isConverged(Path currentOutput, Path lastOutput, Configuration conf, FileSystem fs)
			throws IOException {
		if (!fs.exists(lastOutput)) {
			return false;
		}
		long numClustersOfLastOutput = countClustesNum(lastOutput, conf, fs);
		long numClustersOfCurrentOutput = countClustesNum(currentOutput, conf, fs);
		if (numClustersOfLastOutput == numClustersOfCurrentOutput) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * countClustesNum
	 * 
	 * @param filePath
	 * @param conf
	 * @param fs
	 * @return
	 * @throws IOException
	 */
	public static long countClustesNum(Path filePath, Configuration conf, FileSystem fs) throws IOException {
		if (!fs.exists(filePath)) {
			return 0;
		}
		long clustesNum = 0;
		SequenceFileValueIterator<PAgenesCluster> iterator;
		for (FileStatus part : fs.listStatus(filePath, PathFilters.partFilter())) {
			iterator = new SequenceFileValueIterator<PAgenesCluster>(part.getPath(), true, conf);
			while (iterator.hasNext()) {
				++clustesNum;
				iterator.next();
			}
			iterator.close();
		}
		return clustesNum;
	}
}
