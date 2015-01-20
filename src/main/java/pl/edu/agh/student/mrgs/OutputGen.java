package pl.edu.agh.student.mrgs;

import org.apache.hadoop.fs.Path;

/**
 * Output generator
 */
public interface OutputGen {

    public Path createOutputDir(Path parent, int iteration);

}
