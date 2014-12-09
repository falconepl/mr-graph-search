package pl.edu.agh.student.mrgs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int minDistance = Distance.INFTY;
        Node node = null;
        Node bestNode = null;

        for (Text value : values) {
            Node currNode = new Node(key.toString(), value.toString());

            if (currNode.distance() < minDistance) {
                minDistance = currNode.distance();
                bestNode = currNode;
            }

            if (currNode.hasAdjNodes()) {
                node = currNode;
            }
        }

        if (bestNode != null && node != null) {
            node.update(minDistance, bestNode.backPath(), node.adjNodes());
        }

        context.write(key, node.toValue());

        String nodeId = key.toString();
        String targetNodeId = context.getConfiguration().get(MapRedUtils.TARGET_NODE);
        if (minDistance < Distance.INFTY && nodeId.equals(targetNodeId)) {
            context.getCounter(JobProperties.STATUS).setValue(JobStatuses.FINISHED);
        }
    }

}
