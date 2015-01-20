package pl.edu.agh.student.mrgs;

import org.apache.hadoop.fs.Path;

public class Main {

    public static final String JOB_NAME = "mr-graph-search";

    static class StandardOutputGen implements OutputGen {
        public Path createOutputDir(Path parent, int iteration) {
            return new Path(parent, String.valueOf(iteration));
        }
    }

    public static void main(String... args) throws Exception {
        String inputFileName = args[0];
        String outputDirName = args[1];
        String startNodeId = args[2];
        String targetNodeId = args[3];

        Path inputFile = new Path(inputFileName);
        Path transInputFile = new Path(outputDirName, "0/input.txt");
        MapRedUtils.transformInput(inputFile, transInputFile, startNodeId);
        Path outputDir = new Path(outputDirName);

        OutputGen outputGen = new StandardOutputGen();
        MapReduce task = new MapReduce(JOB_NAME, transInputFile, outputDir, targetNodeId, outputGen);
        task.start();
    }

}
