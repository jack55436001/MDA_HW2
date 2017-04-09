package org.apache.hadoop.examples;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.HashMap;
import java.util.Map;
import java.text.DecimalFormat;

public class MDA_HW2_Adj {

    public static class MatrixMapper
        extends Mapper<Object, Text, Text, Text>{


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String [] matrix = value.toString().split("\t");
        Text Map_key = new Text(matrix[0]);
        Text Map_value = new Text(matrix[1]);
        context.write(Map_key,Map_value);

        }
    }


public static class MatrixReducer
        extends Reducer<Text,Text,NullWritable,Text> {

    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int page = Integer.parseInt(conf.get("page"));
        float B = Float.parseFloat(conf.get("Beta"));
        float flee_v = (1-B)/(float)page;
        float [] tmp = new float[page];
        int sum=0;
        for(Text val : values){
          int index = Integer.parseInt(val.toString());
          tmp[index]=1;
          sum++;
        }

        if(sum==0){
          sum=1;
        }
        DecimalFormat df = new DecimalFormat("0.000000000");
        for(int i=0;i<page;i++){
          StringBuilder sb = new StringBuilder();
          sb.append("M,"+i+","+key.toString()+","+df.format(flee_v+B*tmp[i]/(float)sum));
          context.write(null,new Text(sb.toString()));
        }

    }
}

public static void run (Map<String , String> path) throws Exception {
    Configuration conf = new Configuration();
    conf.set("page","10879");
    conf.set("Beta","0.8");
    Job job = new Job(conf, "MatrixAdj");
    job.setJarByClass(MDA_HW2_Adj.class);
    job.setMapperClass(MatrixMapper.class);
    job.setReducerClass(MatrixReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(path.get("adj")));
    FileOutputFormat.setOutputPath(job, new Path(path.get("tmp1")));
    job.waitForCompletion(true);

    }
}
