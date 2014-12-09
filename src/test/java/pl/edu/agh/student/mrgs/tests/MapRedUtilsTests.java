package pl.edu.agh.student.mrgs.tests;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mrunit.testutil.TemporaryPath;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import pl.edu.agh.student.mrgs.MapRedUtils;
import pl.edu.agh.student.mrgs.utils.Utils;

import java.io.IOException;

public class MapRedUtilsTests {

    @Rule
    public TemporaryPath tempDir = new TemporaryPath();

    @Test
    public void testInputTransformation() throws IOException {
        // Setup
        Path input = tempDir.copyResourcePath("rawInputA.txt");
        Path output = tempDir.getPath("transformedInput");
        Path expectedOutput = tempDir.copyResourcePath("transformedInputA.txt");
        String startNode = "alpha";

        // When
        MapRedUtils.transformInput(input, output, startNode);

        // Then
        String expectedSum = Utils.md5Checksum(expectedOutput, tempDir);
        String outputSum = Utils.md5Checksum(output, tempDir);
        Assert.assertNotNull("Expected output checksum should not be null", expectedSum);
        Assert.assertNotNull("Output checksum should not be null", outputSum);
        Assert.assertEquals("Transformed input file does not match expected output", expectedSum, outputSum);
    }

}
