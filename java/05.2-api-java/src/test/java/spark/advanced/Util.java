package spark.advanced;

import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Util {
    public static List<String> runUnixCommand(String command) {
        ArrayList<String> output = new ArrayList<String>();
        String s= "";
        try {

            Process p = Runtime.getRuntime().exec(command);
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            // read the output from the command
            while ((s = stdInput.readLine()) != null) {
                output.add(s);
            }
            // read any errors from the attempted command
            while ((s = stdError.readLine()) != null) {
                output.add(s);
            }
        } catch (IOException e) {
            System.out.println("Could not run the following command: " + command);
            System.out.println(s);
        }
        return output;
    }
}
