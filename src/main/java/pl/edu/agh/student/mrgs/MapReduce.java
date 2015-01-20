package pl.edu.agh.student.mrgs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduce {

    private String jobName;
    private Path inputFile;
    private Path outputDir;
    private OutputGen outputGen;

    private Configuration conf;

    public MapReduce(String jobName, Path inputFile, Path outputDir, String targetNodeId, OutputGen outputGen) {
        this.jobName = jobName;
        this.inputFile = inputFile;
        this.outputDir = outputDir;
        this.outputGen = outputGen;

        this.conf = new Configuration();
        this.conf.set(MapRedUtils.TARGET_NODE, targetNodeId);
    }

    public void start() throws Exception {
        Job job;
        int iteration = 1;
        Path currInputFile = inputFile;
        Path currOutputDir = outputGen.createOutputDir(outputDir, iteration);

        do {
            job = setupJob(conf, currInputFile, currOutputDir);

            if (!job.waitForCompletion(true)) {
                throw new Exception(jobName + " job failed");
            }

            iteration += 1;
            currInputFile = currOutputDir;
            currOutputDir = outputGen.createOutputDir(outputDir, iteration);
        } while (!reachedTargetNode(job));
    }

    private Job setupJob(Configuration conf, Path inputFile, Path outputDir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJobName(jobName);

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

    private boolean reachedTargetNode(Job job) throws IOException {
        return job.getCounters().findCounter(JobProperties.STATUS).getValue() == JobStatuses.FINISHED;
    }

}
