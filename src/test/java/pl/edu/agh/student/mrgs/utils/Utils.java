package pl.edu.agh.student.mrgs.utils;

import com.google.common.base.Joiner;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.testutil.TemporaryPath;
import org.apache.hadoop.util.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Test utility methods.
 */
public class Utils {

    public static final String SEP = "\t";

    public static Text asText(String node) {
        return new Text(node);
    }

    public static Text asText(int distance, String backPath, String... adjNodes) {
        String adjNodesStr = (adjNodes.length != 0) ? StringUtils.join(SEP, adjNodes) : null;
        String result = Joiner.on(SEP).skipNulls().join(distance, backPath, adjNodesStr);
        return new Text(result);
    }

    public static String backPath(String... nodes) {
        String sep = ":";
        return StringUtils.join(sep, nodes);
    }

    public static String md5Checksum(Path filePath, TemporaryPath tempDir) throws IOException {
        File file = tempDir.getFile(filePath.getName());
        FileInputStream fis = new FileInputStream(file);
        String md5Sum = DigestUtils.md5Hex(fis);
        fis.close();
        return md5Sum;
    }

}
