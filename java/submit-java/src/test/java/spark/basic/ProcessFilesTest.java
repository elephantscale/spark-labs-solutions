package spark.basic;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


import static junit.framework.TestCase.assertTrue;

public class ProcessFilesTest {
    String home = System.getProperty("user.home");
    String project = System.getProperty("user.dir");
    private String testRun = home +
            "/spark/bin/spark-submit" +
            " --class 'spark.basic.ProcessFiles'" +
            " --master local[*]" +
            " --executor-memory 4g " +
            project + "/" +  "target/spark.basic-2.11-jar-with-dependencies.jar" +
            " /data/text/twinkle/1G.data";
    @Test
    public void testSparkSubmit() throws IOException {
        System.out.println("testSparkSubmit");
        List<String> output = Util.runUnixCommand(testRun);
        System.out.println(output.size());
        System.out.println(output.toString());
    }
}
