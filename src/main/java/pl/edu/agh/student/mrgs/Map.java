package pl.edu.agh.student.mrgs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // Preserve the current node
        context.write(key, value);

        Node node = new Node(key.toString(), value.toString());

        if (node.visited() && node.hasAdjNodes()) {
            for (String adjNodeId : node.adjNodes()) {
                Node adjNode = new Node(adjNodeId).withPrevNode(node);

                context.write(adjNode.toKey(), adjNode.toValue());
            }
        }
    }

}
