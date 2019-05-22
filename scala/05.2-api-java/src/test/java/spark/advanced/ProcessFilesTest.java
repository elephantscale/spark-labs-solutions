package spark.advanced;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertTrue;

public class ProcessFilesTest {
    private String testRun = "~/spark/bin/spark-submit" +
            " --class 'spark.advanced.ProcessFiles'" +
            " --master local[*]" +
            " --executor-memory 4g" +
            "  --driver-class-path logging/" +
            "  target/spark.advanced-2.11-jar-with-dependencies.jar" +
            " /data/text/twinkle/1G.data";
    @Test
    public void testSomething() {
        assertTrue(true);
    }
    @Test
    public void testSparkSubmit() {
        List<String> output = Util.runUnixCommand(testRun);
        System.out.println(output);
    }
}
