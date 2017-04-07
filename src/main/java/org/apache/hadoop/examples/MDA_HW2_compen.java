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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import java.net.URI;
import java.util.Map;
import java.text.DecimalFormat;
import java.util.*;

public class MDA_HW2_compen {

    private static int pages = 5;

    public static class MatrixMapper
        extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
              Configuration conf = context.getConfiguration();
              String [] matrix = value.toString().split("\t");
              Text Map_value = new Text(matrix[0]+","+matrix[1]);
              context.write(new Text("a"),Map_value);

        }
    }


    public static class MatrixReducer
        extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        float total = 0;
        List<String> K = new ArrayList<>();
        List<Float> V = new ArrayList<>();
        for(Text val : values){
            String [] matrix = val.toString().split(",");
            total = total + Float.parseFloat(matrix[1]);
            K.add(matrix[0]);
            V.add(Float.parseFloat(matrix[1]));
        }
        float compensate = (1-total)/(float)pages;
        for (int i=0;i<pages;i++){
            float r = 0;
            r = compensate + V.get(i);
            DecimalFormat df = new DecimalFormat("0.000");
            context.write(new Text(K.get(i)),new Text(df.format(r)));
        }


    }
}
public static void removeFile(String input , Configuration conf) throws IOException{
    Path path = new Path(input);
    FileSystem fs = FileSystem.get(conf);
    fs.deleteOnExit(path);
    fs.close();
}

public static void renameFile(String scr,String dst, Configuration conf) throws IOException{
    Path path = new Path(scr);
    Path path2 = new Path(dst);
    FileSystem fs = FileSystem.get(conf);
    fs.rename(path,path2);
    fs.close();
}

public static void run(Map<String , String> path) throws Exception {
    Configuration conf = new Configuration();
    String input = path.get("pr");
    String output = path.get("tmp3");
    removeFile(output,conf);

    Job job = new Job(conf, "Compensate");
    job.setJarByClass(MDA_HW2_compen.class);
    job.setMapperClass(MatrixMapper.class);
    job.setReducerClass(MatrixReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    job.waitForCompletion(true);
    removeFile(input,conf);
    renameFile(output,input,conf);
    }
}
