package pl.edu.agh.student.mrgs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {

    public static final String JOB_NAME = "mr-graph-search";

    public static void main(String... args) throws Exception {
        String inputFileName = args[0];
        String outputDirName = args[1];
        String startNodeId = args[2];
        String targetNodeId = args[3];

        Configuration conf = new Configuration();
        conf.set(MapRedUtils.TARGET_NODE, targetNodeId);

        Path inputFile = new Path(inputFileName);
        Path transInputFile = new Path(outputDirName, "0/input.txt");
        MapRedUtils.transformInput(inputFile, transInputFile, startNodeId);
        Path outputDir = new Path(outputDirName);

        Job job;
        int iteration = 1;
        Path currInputFile = transInputFile;
        Path currOutputDir = new Path(outputDir, String.valueOf(iteration));

        do {
            job = setupJob(conf, currInputFile, currOutputDir);

            if (!job.waitForCompletion(true)) {
                throw new Exception(JOB_NAME + " job failed");
            }

            iteration += 1;
            currInputFile = currOutputDir;
            currOutputDir = new Path(outputDir, String.valueOf(iteration));
        } while (!reachedTargetNode(job));
    }

    private static Job setupJob(Configuration conf, Path inputFile, Path outputDir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJobName(JOB_NAME);

        job.setJarByClass(Main.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputDir);

        return job;
    }

    private static boolean reachedTargetNode(Job job) throws IOException {
        return job.getCounters().findCounter(JobProperties.STATUS).getValue() == JobStatuses.FINISHED;
    }

}
