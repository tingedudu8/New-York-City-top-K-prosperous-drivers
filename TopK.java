
import java.io.IOException;
import java.util.*;

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

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class TopK extends Configured implements Tool {

    static int printUsage() {
        System.out.println("revenuecount [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class Tuple {
        String id;
        double revenue;
        public Tuple(String id, double revenue) {
            this.id = id;
            this.revenue = revenue;
        }

        public String toString() {
            return id + " " + revenue;
        }

    }


    public static class TopKMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        public final IntWritable one = new IntWritable(1);
        public static int N = 5;
        // so we don't have to do reallocations
//        private final static DoubleWritable one = new DoubleWritable(1);
//        private Text word = new Text();
        public Queue<Tuple> TopKPq = new PriorityQueue<>((o1, o2) -> {
            if (o1.revenue == o2.revenue)
                return 0;
            return o2.revenue > o1.revenue ? -1 : 1;
        });


        // to check for only alphanumeric
//        String expression = "^[a-zA-Z]*$";
//        Pattern pattern = Pattern.compile(expression);

        public void map(Object key, Text value, Context context
        ) {
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
//            word.set(value);
            String line = value.toString();
            String[] single = line.split("\\s+");
            String id = single[0];
            String revenue = single[1];
            TopKPq.add(new Tuple(id, Double.parseDouble(revenue)));
            while (TopKPq.size() > N) {
                TopKPq.poll();
            }

//            if(!single[0].equals("medallion")) {
//                try {
//                    String taxi = single[1];
//                    DoubleWritable one = new DoubleWritable(Double.parseDouble(single[10]));
//                    context.write(new Text(taxi), one);
//                }
//                catch (NullPointerException e) {
//                    e.printStackTrace();
//                }
//
//            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            while (!TopKPq.isEmpty()) {
                Tuple t = TopKPq.poll();
                context.write(one, new Text(t.toString()));
            }
        }
    }

    public static class TopKReducer
            extends Reducer<IntWritable,Text, IntWritable, Text> {
//        private DoubleWritable result = new DoubleWritable();
        private Queue<Tuple> TopKPq = new PriorityQueue<>((o1, o2) -> {
            if (o1.revenue == o2.revenue)
                return 0;
            return o2.revenue > o1.revenue ? -1 : 1;
        });


        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
//            double sum = 0;
//            for (DoubleWritable val : values) {
//                sum += val.get();
//            }
//            result.set(sum);
//            context.write(key, result);
            for (Text v : values) {
                String [] str = v.toString().split(" ");
                TopKPq.add(new Tuple(str[0], Double.parseDouble(str[1])));
            }
            while (TopKPq.size() > 5) {
                TopKPq.poll();
            }
            while (!TopKPq.isEmpty()) {
                Tuple t = TopKPq.poll();
                context.write(key, new Text(t.toString()));
            }
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "revenue count");
        job.setJarByClass(TopK.class);
        job.setMapperClass(TopKMapper.class);
//        job.setCombinerClass(TopKReducer.class);
        job.setReducerClass(TopKReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);


        List<String> other_args = new ArrayList<>();
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
        int res = ToolRunner.run(new Configuration(), new TopK(), args);
        System.exit(res);
    }


}