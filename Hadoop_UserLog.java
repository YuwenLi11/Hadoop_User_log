import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Use MapReduce:Analysis user behavior
 */
public class LogApp {

    /**
     * Map：Read the input file
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        LongWritable one = new LongWritable(1);

        private UserAgentParser userAgentParser ;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            userAgentParser = new UserAgentParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // line by line
            String line = value.toString();

            String source = line.substring(getCharacterPosition(line, "\"", 7)) + 1;
            UserAgent agent = userAgentParser.parse(source);
            String platform = agent.getPlatform();

            // usecontext to output the result of map
            context.write(new Text(platform), one);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            userAgentParser = null;
        }
    }


    /**
     * get the index of the needed string from the data
     */
    private static int getCharacterPosition(String value, String operator, int index) {
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int mIdx = 0;
        while (slashMatcher.find()) {
            mIdx++;

            if (mIdx == index) {
                break;
            }
        }
        return slashMatcher.start();
    }

    /**
     * Reduce
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long sum = 0;
            for (LongWritable value : values) {
                // count the sum of the keys' appearance
                sum += value.get();
            }

            // output the statistic result
            context.write(key, new LongWritable(sum));
        }
    }

    /**
     * Driver：contains all the information of the Jobs
     */
    public static void main(String[] args) throws Exception {

        //create Configuration
        Configuration configuration = new Configuration();

        // clear the path from the output
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("output file exists, but is has deleted");
        }

        //create Job
        Job job = Job.getInstance(configuration, "LogApp");

        //set job
        job.setJarByClass(LogApp.class);

        //set the input path
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //set map
        job.setMapperClass(LogApp.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //set reduce
        job.setReducerClass(LogApp.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //set the otput path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
