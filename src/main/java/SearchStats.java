package nz.ac.vuw.ecs.sharanprasad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class SearchStats {

    public static class SearchMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

        private Configuration conf;
        private static final String urlRegex = "(?)(.*)((http|https)://.*.com)(.*)";
        private static final Text keyText = new Text("numberofSearches");
        private final static LongWritable one = new LongWritable(1);

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
        }

        @Override
        public void map(LongWritable key,Text value,Context context) throws IOException , InterruptedException{
            if(!value.toString().matches(urlRegex)){
                context.write(new Text(keyText),one);
            }
        }
    }

    public static class SearchReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
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

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Not Enough arguments specify input and output");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "search statistics");
        job.setJarByClass(SearchStats.class);
        job.setMapperClass(SearchMapper.class);
        job.setCombinerClass(SearchReducer.class);
        job.setReducerClass(SearchReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        int r = job.waitForCompletion(true) ? 0 : 1;
        System.exit(r);
    }
}
