package pl.edu.agh.student.mrgs;

import org.apache.hadoop.fs.Path;

public class MapRedUtils {

    /**
     * Transforms original input file for a MapReduce job that represents an adjacency list
     * of a graph we operate on. The input data transformation is about providing an initial
     * distance from a given node to the start node and additional empty column that would
     * hold back path information (path from the given node to the start node).
     * <p/>
     * This method should be invoked before running the actual MapReduce job.
     *
     * @param sourceFile  original input file HDFS path
     * @param targetFile  HDFS path of the file to be created
     * @param startNodeId start node's ID
     */
    public static void transformInput(Path sourceFile, Path targetFile, String startNodeId) {
        // TODO: To implement...
    }

}
