package org.apache.hadoop.examples;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MDA_HW2 {

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
        extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int page = Integer.parseInt(conf.get("page")); 
        float B = Float.parseFloat(conf.get("Beta"));
        float flee_v = (1-B)/page;
        float [] tmp = new float[page+1];
        int sum=0;
        for(Text val : values){
          int index = Integer.parseInt(val.toString());
          tmp[index]=1;
          sum++;
        }
        
        if(sum==0){
          sum=1;
        }
        
        StringBuilder sb = new StringBuilder();
        for(int i=1;i<=page;i++){
          sb.append(","+(float)(flee_v+B*tmp[i]/sum));
        }
        Text v = new Text(sb.toString().substring(1));
        context.write(key,v);
    }
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    conf.set("page","5");
    conf.set("Beta","0.85");
    if (otherArgs.length != 2) {
        System.err.println("Usage: MatrixAdj <in> <out>");
        System.exit(2);
    }
    Job job = new Job(conf, "MatrixAdj");
    job.setJarByClass(MDA_HW2.class);
    job.setMapperClass(MatrixMapper.class);
    job.setReducerClass(MatrixReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}