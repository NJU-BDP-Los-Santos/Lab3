package ReduceSide;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

public class Main {
    public static void main(String[] args) throws Exception
    {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: <in> <out>");
                System.exit(2);
            }

            Job job = new Job(conf, "Inverted-Index-Basic");
            job.setJarByClass(Main.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setReducerClass(MergeReducer.class);
            job.setCombinerClass(CombinerSameWordDoc.class);
            job.setPartitionerClass(DividePartitioner.class);
            job.setNumReduceTasks(4);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static class TokenizerMapper extends Mapper<LongWritable,Text,Text,IntWritable>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName(); // 获得当前文件的文件名

            String[] point_divide = fileName.split("\\.");
            String docName = point_divide[0] + point_divide[1]; // 按照输出样例，消除了小说明中的.
            Text word = new Text();
//            IntWritable count;
//            HashMap<Text, Integer> hashMap = new HashMap<>();
            StringTokenizer tokens = new StringTokenizer(value.toString());

            while(tokens.hasMoreTokens())
            {
                word.set(tokens.nextToken());
                Text word_filename = new Text(word + "#" + docName); // 创建复合键
                context.write(word_filename, new IntWritable(1));
//                context.write(word, new Text(fileName));
            }
        }
    }
    public static class CombinerSameWordDoc extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        /**
         * 用来合并同一个 词语-小说 的组
         * @param key
         * @param values 同一个 词语-小说 的列表
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int total = 0;
            for(IntWritable value: values)
            {
                total = total + value.get();
            }
            context.write(key, new IntWritable(total));
        }
    }

    public static class DividePartitioner extends HashPartitioner<Text, IntWritable>
    {
        @Override
        public int getPartition(Text key, IntWritable value,
                                int numPartitions) {
            String real_key = key.toString().split("#")[0];
            return super.getPartition(new Text(real_key), value, numPartitions);
        }
    }

    public static class MergeReducer extends Reducer<Text, IntWritable, Text, Text>
    {
        String t_prev;
        int worddoc_count; // 同键值的计数
        String output_;
        double words_sum;
        double doc_sum;
        @Override
        protected void setup(Context context)
        {
            t_prev = new String();
            worddoc_count = 0;
            output_ = new String();
            words_sum = 0.0;
            doc_sum = 0.0;
        }
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int count = 0;
            for (IntWritable value: values)
            {
                count += value.get();
            }
            String t = key.toString().split("#")[0];
//            System.out.println(t);
//            System.out.println(t_prev);
            if (!t.equals(t_prev) && t_prev != null && !t_prev.equals(""))
            {
                double average = words_sum / doc_sum;
                context.write(new Text(t_prev + "\t" + doubleTransform(average) + ","), new Text(output_));
                output_ = "";
                words_sum = 0.0;
                doc_sum = 0.0;
            }
            words_sum += (double)count;
            doc_sum += 1.0;
            t_prev = t;
            output_ = output_ + key.toString().split("#")[1] + ":" + Integer.toString(count) + ";";
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            double average = words_sum / doc_sum;
            context.write(new Text(t_prev + "\t" + doubleTransform(average) + ","), new Text(output_));
//            context.write(new Text(t_prev + ","), new Text(output_));
        }
    }

}
