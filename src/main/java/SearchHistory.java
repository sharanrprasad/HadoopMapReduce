
package nz.ac.vuw.ecs.sharanprasad;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper.*;


public class SearchHistory {

    public static class SearchMapper extends Mapper<Text,Text,Text,Text> {

        private String searchID;
        private Configuration conf;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            searchID = conf.get("searchID"," ");
        }

        @Override
        public void map(Text key,Text value,Context context) throws IOException , InterruptedException{
            if(searchID.equalsIgnoreCase(key.toString())){

                String[] valueList = value.toString().split("\\t");
                Optional<String> val = IntStream.range(0,valueList.length).filter( i -> i != 1).mapToObj(i -> valueList[i]).reduce((init,curr) -> init + "\t"  + curr ); //remove the time column
                 if(val.isPresent()) {
                    context.write(new Text(searchID), new Text(val.get()));
                }
            }
        }
    }

    public static class SearchReducer extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text val : values) {
                result.set(val);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 3) {
            System.err.println("Not Enough arguments specify input and output and ID number");
            System.exit(2);
        }
        conf.set("searchID", remainingArgs[2]);
        Job job = Job.getInstance(conf, "search history");
        job.setJarByClass(SearchHistory.class);
        job.setMapperClass(SearchMapper.class);
        job.setReducerClass(SearchReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        KeyValueTextInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        int r = job.waitForCompletion(true) ? 0 : 1;
        System.exit(r);
    }


}

