package spark.basic;
import org.junit.Test;
import java.io.IOException;
import java.util.List;

import static junit.framework.TestCase.assertTrue;


//import static junit.framework.TestCase.assertTrue;

public class ProcessFilesTest {
    String home = System.getProperty("user.home");
    String project = System.getProperty("user.dir");
    private String testRun = home +
          "/spark/bin/spark-submit" +
           " --class spark.basic.ProcessFiles" +
           " " + project + "/" +  "target/spark.basic-2.11.jar" +
           " /data/text/twinkle/1G.data";

    @Test
    public void testSparkSubmit() throws IOException {
        System.out.println("testSparkSubmitExternal");
        List<String> output = Util.runUnixCommand(testRun);
        String first = output.get(0);
        System.out.println(first);
        assertTrue(first.contains("count: 31,300,777"));
    }

    @Test
    public void testSpark() throws IOException {
        System.out.println("testSparkSubmit");
        String[] argv = {"Shalom"};
        ProcessFiles.main(argv);
    }

}
