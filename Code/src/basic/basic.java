package basic;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.LongWritable;

public class basic {
    //map将输入中的value复制到输出数据的key上，并直接输出

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        //实现map函数
        public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
        {
            String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
            String line = value.toString();
            // 抛弃空记录
            if (line == null || line.equals("")) return;
            // 处理来自表order的记录
            if (filePath.contains("order")) {
                String[] values = line.split(" "); // 按分隔符分割出字段

                String ID = values[0]; // id
                String OrderDate = values[1]; // order_date
                String PID = values[2]; //p_id
                String Num = values[3]; //num

                context.write(new Text(PID),new Text("#a"+ID + ","+OrderDate +","+Num));

            }
            else if (filePath.contains("product")) {
                String[] values = line.split(" "); // 按分隔符分割出字段

                String PID = values[0]; // p_id
                String Name = values[1]; // name
                String Price = values[2]; //price
                context.write(new Text(PID),new Text("#b" + Name + "," + Price));

            }
        }
    }

    //reduce将输入中的key复制到输出数据的key上，并直接输出
    public static class JoinReducer extends Reducer<Text,Text,Text,Text>{
        //实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
            Vector<String> vecA = new Vector<String>(); // 存放来自表A的值
            Vector<String> vecB = new Vector<String>(); // 存放来自表B的值
            System.out.println(values.toString());
            for(Text value : values)
                if(value.toString().startsWith("#a")){
                    vecA.add(value.toString().substring(2));//2???
                    System.out.println(value.toString());
                }
                else if (value.toString().startsWith("#b")) {
                    vecB.add(value.toString().substring(2));
                    System.out.println(value.toString());
                }
            int sizeA = vecA.size();
            int sizeB = vecB.size();



            for (int i = 0; i < sizeA;i++) {
                for (int j = 0; j < sizeB;j++) {
                    String[] indexa = vecA.get(i).split(",");
                    String ID = indexa[0]; // id
                    String OrderDate = indexa[1]; // order_date
                    String Num = indexa[2]; //num
                    String[] indexb = vecB.get(j).split(",");
                    String Name = indexb[0]; // name
                    String Price = indexb[1]; //price
                    context.write(new Text(ID), new Text(OrderDate + " " + key.toString() + " " + Name + " " + Price + " " + Num));
                }
            }

        }

    }

    public static void main(String[] args) throws Exception{


        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.ignoreseparator","true");
        conf.set("mapred.textoutputformat.separator"," ");
        //args='order.csv';
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //JobConf job = new JobConf(Test.class);
        Job job = Job.getInstance(conf);//, "Data Deduplication");
        job.setJarByClass(basic.class);

        //设置Map、Combine和Reduce处理类
        job.setMapperClass(JoinMapper.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(JoinReducer.class);

        //Mapper输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置reduder输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        //设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //JobClient.runJob(job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
