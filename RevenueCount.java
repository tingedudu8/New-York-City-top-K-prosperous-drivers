
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class RevenueCount extends Configured implements Tool {

    static int printUsage() {
        System.out.println("revenuecount [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class RevenueCountMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        // so we don't have to do reallocations
//        private final static DoubleWritable one = new DoubleWritable(1);
        private Text word = new Text();

        // to check for only alphanumeric
//        String expression = "^[a-zA-Z]*$";
//        Pattern pattern = Pattern.compile(expression);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
//            StringTokenizer itr = new StringTokenizer(value.toString());
//            while (itr.hasMoreTokens()) {
//
//                // check for all alphabetical
//                String nextToken = itr.nextToken ();
//                Matcher matcher = pattern.matcher(nextToken);
//
//                // if not, don't write
//                if (!matcher.matches ())
//                    continue;
//
//                // otherwise, write
//                word.set(nextToken.toLowerCase ());
//                context.write(word, one);
            word.set(value);
            String line = word.toString();
            String [] single = line.split(",");
            if(!single[0].equals("medallion")) {
                try {
                    String taxi = single[1];
                    DoubleWritable one = new DoubleWritable(Double.parseDouble(single[10]));
                    context.write(new Text(taxi), one);
                }
                catch (NullPointerException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    public static class RevenueCountReducer
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "revenue count");
        job.setJarByClass(RevenueCount.class);
        job.setMapperClass(RevenueCountMapper.class);
        job.setCombinerClass(RevenueCountReducer.class);
        job.setReducerClass(RevenueCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
            try {
                if ("-r".equals(args[i])) {
                    job.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " +
                        args[i-1]);
                return printUsage();
            }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 2.");
            return printUsage();
        }
        FileInputFormat.setInputPaths(job, other_args.get(0));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RevenueCount(), args);
        System.exit(res);
    }

}