package nz.ac.vuw.ecs.sharanprasad;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.*;

import java.io.Console;
import java.io.IOException;

public class StatsCounter {

    public  enum  CUSTOM_COUNTER {
        SEARCHES_TOTAL,
        UNIQUE_USERS_TOTAL,
        CLICKS_TOTAL
    }

    public static class StatsMapper extends Mapper<Text,Text,Text,LongWritable> {

        private Configuration conf;
        private static final String urlRegex = "(?)(.*)((http|https)://.*)(.*)";
        private final static LongWritable one = new LongWritable(1);

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
        }

        @Override
        public void map(Text key,Text value,Context context) throws IOException , InterruptedException{
            if(key == null ||  key.toString().isEmpty()  || !key.toString().matches("[0-9]*") ) {
                return;
            }

            if(value.toString().split("\\t").length == 4 ) {
                context.getCounter(CUSTOM_COUNTER.CLICKS_TOTAL).increment(1);

            }
            context.getCounter(CUSTOM_COUNTER.SEARCHES_TOTAL).increment(1);

            context.write(key,one);
        }
    }


    public static class StatsCombiner extends  Reducer<Text,LongWritable,Text,LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static class StatsReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            long sum = 0;
            result.set(sum);
            context.getCounter(CUSTOM_COUNTER.UNIQUE_USERS_TOTAL).increment(1);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 3) {
            System.err.println("Not Enough arguments specify input, output and number of reducers ");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "stats counter");
        job.setJarByClass(StatsCounter.class);
        job.setMapperClass(StatsMapper.class);
        job.setReducerClass(StatsReducer.class);
        job.setCombinerClass(StatsCombiner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        KeyValueTextInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
        System.out.println("Number of reducers " + remainingArgs[2]);
        job.setNumReduceTasks(Integer.parseInt(remainingArgs[2]));


        int r = job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        long totalClicks =  counters.findCounter(CUSTOM_COUNTER.CLICKS_TOTAL).getValue();
        long totalSearches =  counters.findCounter(CUSTOM_COUNTER.SEARCHES_TOTAL).getValue();
        long uniqeUsers =  counters.findCounter(CUSTOM_COUNTER.UNIQUE_USERS_TOTAL).getValue();



        System.out.println("\n\n\n**********\n\n\n");
        System.out.println("Total Clicks  = " + totalClicks);
        System.out.println("\n\n");
        System.out.println("total Searches = " + totalSearches);
        System.out.println("\n\n");
        System.out.println("Unique Users = " + uniqeUsers);
        System.out.println("\n\n\n********** \n\n\n\n");

        Path pt = new Path(remainingArgs[1] + "/results.txt");
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataOutputStream writer = fs.create(pt, true);
            String val = "Total Clicks " + totalClicks + " \n \n";
            writer.write(val.getBytes());
            val = "Total Searches  " + totalSearches + " \n \n";
            writer.write(val.getBytes());
            val = "Unique Users   " + uniqeUsers + " \n \n";
            writer.write(val.getBytes());
            writer.close();
        }catch (IOException e){
            e.printStackTrace();
            throw new Error(e);
        }catch (Exception e){
            e.printStackTrace();
            throw new Error(e);
        }
        System.exit(r);
    }

}
