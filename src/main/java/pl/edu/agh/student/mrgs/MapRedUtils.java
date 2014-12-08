package pl.edu.agh.student.mrgs;

import com.google.common.base.Joiner;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class MapRedUtils {

    public static final String SEP = "\t";

    public static final String TARGET_NODE = "target_node";

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
    public static void transformInput(Path sourceFile, Path targetFile, String startNodeId) throws IOException {
        FileSystem fs = sourceFile.getFileSystem(new Configuration());
        InputStream in = fs.open(sourceFile);
        OutputStream out = fs.create(targetFile);

        BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        while (br.ready()) {
            String line = br.readLine();

            String[] parts = line.split(SEP);
            String nodeId = parts[0];
            int distance = (nodeId.equals(startNodeId)) ? 0 : Distance.INFTY;
            String adjNodes = StringUtils.join(parts, SEP, 1, parts.length);

            String outputLine = Joiner.on(SEP).join(nodeId, String.valueOf(distance), "", adjNodes);
            if (br.ready()) {
                outputLine += "\n";
            }

            IOUtils.write(outputLine, out);
        }

        out.close();
    }

}
