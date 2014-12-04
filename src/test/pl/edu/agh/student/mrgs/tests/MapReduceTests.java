package pl.edu.agh.student.mrgs.tests;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import pl.edu.agh.student.mrgs.Distance;
import pl.edu.agh.student.mrgs.Map;
import pl.edu.agh.student.mrgs.Reduce;
import pl.edu.agh.student.mrgs.utils.Utils;

import java.io.IOException;
import java.util.List;

public class MapReduceTests {

    MapDriver<Text, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        Map mapper = new Map();
        Reduce reducer = new Reduce();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapperForUnvisitedNode() throws IOException {
        // Input and output
        Text inKey = Utils.asText("bravo");
        Text inValue = Utils.asText(Distance.INFTY, "", "charlie", "echo");

        // Expected
        mapDriver.withInput(inKey, inValue).withOutput(inKey, inValue);

        mapDriver.runTest();
    }

    @Test
    public void testMapperForStartNode() throws IOException {
        // Input and output
        Text inKey = Utils.asText("alpha");
        Text inValue = Utils.asText(0, "", "bravo");

        // Additional output
        Text outKey = Utils.asText("bravo");
        Text outValue = Utils.asText(1, "alpha");

        Pair<Text, Text> preservedRec = new Pair<>(inKey, inValue);
        Pair<Text, Text> createdRec = new Pair<>(outKey, outValue);
        List<Pair<Text, Text>> outRecords =
                Lists.newArrayList(preservedRec, createdRec);

        // Expected
        mapDriver.withInput(inKey, inValue).withAllOutput(outRecords);

        mapDriver.runTest();
    }

    @Test
    public void testMapperForNodeWithAdjacentNodes() throws IOException {
        // Input and output
        Text inKey = Utils.asText("echo");
        Text inValue = Utils.asText(24, Utils.backPath("charlie", "bravo", "kilo"));

        // Additional output
        Text outKeyCharlie = Utils.asText("charlie");
        Text outKeyBravo = Utils.asText("bravo");
        Text outKeyKilo = Utils.asText("kilo");

        Text outValue = Utils.asText(24 + 1, "echo");

        Pair<Text, Text> charlieRec = new Pair<>(outKeyCharlie, outValue);
        Pair<Text, Text> bravoRec = new Pair<>(outKeyBravo, outValue);
        Pair<Text, Text> kiloRec = new Pair<>(outKeyKilo, outValue);
        List<Pair<Text, Text>> outRecords =
                Lists.newArrayList(charlieRec, bravoRec, kiloRec);

        // Expected
        mapDriver.withInput(inKey, inValue).withAllOutput(outRecords);

        mapDriver.runTest();
    }

}
