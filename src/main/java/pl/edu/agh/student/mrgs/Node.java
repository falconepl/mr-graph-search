package pl.edu.agh.student.mrgs;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import org.apache.hadoop.io.Text;

import java.util.Arrays;

public class Node {

    private final String nodeId;
    private int distance;
    private String backPath;
    private String[] adjNodes = {};

    public static final String SEP = "\t";

    public Node(String nodeId) {
        this.nodeId = nodeId;
    }

    public Node(String key, String value) {
        String[] parts = value.split(MapRedUtils.SEP);

        this.nodeId = key;
        this.distance = Integer.valueOf(parts[0]);
        this.backPath = parts[1];

        if (parts.length > 2) {
            this.adjNodes = Arrays.copyOfRange(parts, 2, parts.length);
        }
    }

    public int distance() {
        return distance;
    }

    public boolean hasAdjNodes() {
        return (adjNodes != null) && (adjNodes.length > 0);
    }

    public String[] adjNodes() {
        return adjNodes;
    }

    public boolean visited() {
        return this.distance < Distance.INFTY;
    }

    public Node withPrevNode(Node prevNode) {
        this.distance = prevNode.distance + 1;

        String sep = (!prevNode.backPath.isEmpty()) ? ":" : "";
        this.backPath = prevNode.backPath + sep + prevNode.nodeId;

        return this;
    }

    public void update(int distance, String[] adjNodes) {
        this.distance = distance;
        this.adjNodes = adjNodes;
    }

    public Text toKey() {
        return new Text(nodeId);
    }

    public Text toValue() {
        String value = Joiner.on(SEP).skipNulls().join(distance, backPath, adjNodes);
        return new Text(value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nodeId);
    }

    @Override
    public String toString() {
        return Joiner.on(" ").skipNulls().join(nodeId, distance, backPath, adjNodes);
    }

}
