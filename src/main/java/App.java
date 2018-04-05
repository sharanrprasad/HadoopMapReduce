import org.apache.hadoop.io.Text;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/*
 * This Java source file was generated by the Gradle 'init' task.
 */
public class App {
    public String getGreeting() {
        return "Hello world.";
    }

    public static void main(String[] args) {


        java.nio.file.Path outputDirectory = Paths.get("/Users/sharanrprasad/Documents/java8");
        try {
            if (!Files.exists(outputDirectory)) {
                outputDirectory = Files.createDirectory(outputDirectory);
            }
            if(Files.isDirectory(outputDirectory)) {
                java.nio.file.Path filePath = Paths.get(outputDirectory.toUri().getPath() + "/result.txt");
                filePath = Files.createFile(filePath);
                try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
                    writer.write("Total Clicks "  + " \n \n");
                    writer.write("Total Searches  " + " \n \n");
                    writer.write("Unique Users   "  + " \n \n");
                }
            }

        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
