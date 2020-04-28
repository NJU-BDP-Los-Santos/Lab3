package MapSideJoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class main extends Configured implements Tool
{
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        // 用于缓存 order、 product 文件中的数据
        //private Map<String, String> orderMap = new HashMap<String, String>();
        private Map<String, String> productMap = new HashMap<String, String>();

        private Text oKey = new Text();
        private Text oValue = new Text();
        private String[] kv;

        // 此方法会在map方法执行之前执行
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            BufferedReader in = null;

            try {
                // 从当前作业中获取要缓存的文件
                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                String productIndex = null;
                for (Path path : paths) {
                    if (path.toString().contains("product")) {
                        in = new BufferedReader(new FileReader(path.toString()));
                        while (null != (productIndex = in.readLine())) {
                            String[] values = productIndex.split(" "); // 按分隔符分割出字段

                            String PID = values[0]; // p_id
                            String Name = values[1]; // name
                            String Price = values[2]; //price
                            productMap.put(PID, new String(Name + " " + Price));
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            kv = value.toString().split(" ");
            // map join: 在map阶段过滤掉不需要的数据
            if (productMap.containsKey(kv[2])) {
                oKey.set(new String(kv[0] + " " + kv[1] + " " + kv[2] + " " + productMap.get(kv[2]) + " " + kv[3]));
                oValue.set("1");
                context.write(oKey, oValue);
            }
        }

    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        //private Text oValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Text oValue = new Text("");
            //oValue.set(String.valueOf(sumCount));
            context.write(key, oValue);
        }

    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "MapSideJoin");

        job.setJobName("MapSideJoin");
        job.setJarByClass(main.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String[] otherArgs = new GenericOptionsParser(job.getConfiguration(),
                args).getRemainingArgs();

        // 我们把第1个参数的地址作为要缓存的文件路径
        DistributedCache.addCacheFile(new Path(otherArgs[1]).toUri(), job
                .getConfiguration());

        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new main(), args);
        System.exit(res);
    }
}
