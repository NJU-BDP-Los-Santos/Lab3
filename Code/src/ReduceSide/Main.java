package ReduceSide;


//import ReduceSide.ProductOrder;


import org.apache.commons.math3.stat.descriptive.summary.Product;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mockito.internal.matchers.Null;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

public class Main // 主函数（类）
{
    // 需要自定义类型进行表示

    public static void main(String[] args) throws Exception
    {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Please Use the command: <input path> <output path>");
                System.exit(2);
            }

            Job job = new Job(conf, "Reduce-Side-Join");
            job.setJarByClass(Main.class);
            job.setMapperClass(ReaderMapper.class);
            job.setReducerClass(JoinReducer.class);
//            job.setCombinerClass(CombinerSameWordDoc.class);
            job.setPartitionerClass(PidPartitioner.class);
            job.setNumReduceTasks(1);
            job.setGroupingComparatorClass(PidGroup.class);
            job.setOutputKeyClass(ProductOrder.class);
            job.setOutputValueClass(NullWritable.class);
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

    public static class ReaderMapper extends Mapper<LongWritable, Text, ProductOrder, NullWritable>
        // 用于读取文件中的信息，并且组织成为自定义的类型
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();    // 获得当前文件的文件名
            String fileKind = fileName.split(("\\."))[0];       // 获得当前文件的种类：product or order

            StringTokenizer tokens = new StringTokenizer(value.toString());
//            System.out.println(value.toString());
            String[] data = new String[4];

            int i = 0;
            while(tokens.hasMoreTokens())
            {
                data[i] = tokens.nextToken().toString();
                ++i;
            }

            // 这里对i的情况进行分类讨论
            ProductOrder po;
            if (i == 3)
            {
                po = new ProductOrder(Integer.parseInt(data[0]), data[1], Integer.parseInt(data[2]));
            }
            else
            {
                po = new ProductOrder(Integer.parseInt(data[2]), Integer.parseInt(data[0]), data[1], Integer.parseInt(data[3]));
            }

            context.write(po, NullWritable.get());
        }
    }

    public static class PidPartitioner extends HashPartitioner<ProductOrder, NullWritable>
    {
        @Override
        public int getPartition(ProductOrder key, NullWritable value,
                                int numPartitions)
        {
            return key.getPid() % numPartitions;
        }
    }

    public static class PidGroup extends WritableComparator
    {
        public PidGroup()
        {
            super(ProductOrder.class, true);
        }
        public int compare(WritableComparable left, WritableComparable right)
        {
            ProductOrder l = (ProductOrder) left;
            ProductOrder r = (ProductOrder) right;
            int l_id = l.getPid();
            int r_id = r.getPid();
            if (l_id == r_id)
                return 0;
            else if (l_id > r_id)
                return 1;
            else
                return -1;
        }
    }

    public static class JoinReducer extends Reducer<ProductOrder, NullWritable, Text, NullWritable>
    {
        @Override
        protected void reduce(ProductOrder key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
        {
            // 首先找到product对应的那个key
            String pname = key.getPname();
            int price = key.getPrice();
            for (NullWritable value: values)
            {
                if (key.getId() == -1)
                {
                    continue;
                }
//                System.out.println(pname);
                key.setPname(pname);
                key.setPrice(price);
                context.write(key.toText(), value);
            }
        }
    }
}
