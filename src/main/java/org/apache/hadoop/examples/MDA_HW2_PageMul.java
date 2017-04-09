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

public class MDA_HW2_PageMul {
    
    private static int pages = 10878;
    private static float B = 0.8f;
    

    public static class MatrixMapper
        extends Mapper<Object, Text, Text, Text>{
    private String flag;
    
    @Override
    protected void setup(Context context) throws IOException,InterruptedException{
        FileSplit split = (FileSplit) context.getInputSplit();
        flag = split.getPath().getParent().getName();
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int m = Integer.parseInt(conf.get("m"));
        int k = Integer.parseInt(conf.get("k"));
        Text Map_key = new Text();
        Text Map_value = new Text();
        if(flag.equals("tmp1"))
        {
            String [] mapAndreduce = value.toString().split(",");
            for(int i=0;i<=k;i++)
            {
                Map_key.set(mapAndreduce[1]+","+Integer.toString(i));
                Map_value.set(mapAndreduce[0]+","+mapAndreduce[2]+","+mapAndreduce[3]);
                context.write(Map_key,Map_value);
            }
        }
        else
        {
            String [] mapAndreduce = value.toString().split("\t");
            for(int i=0;i<=m;i++)
            {
                Map_key.set(Integer.toString(i)+",1");
                Map_value.set("N,"+mapAndreduce[0]+","+mapAndreduce[1]);
                context.write(Map_key,Map_value);
            }
        }

        }
    }


public static class MatrixReducer
        extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
        String [] str;
        HashMap<Integer,Float> m = new HashMap<Integer,Float>();
        HashMap<Integer,Float> n = new HashMap<Integer,Float>();

        for(Text val:values)
        {
          str = val.toString().split(",");
          if(str[0].equals("M"))
          {
            m.put(Integer.parseInt(str[1]),Float.parseFloat(str[2]));
          }
          else
          {
            n.put(Integer.parseInt(str[1]),Float.parseFloat(str[2]));
          }
        }
        Configuration conf = context.getConfiguration();
        int j = Integer.parseInt(conf.get("j"));
        float result = 0;
        float m_ij,n_jk;
        float flee_v = (1-B)/(float)pages;
        
        for(int i=0;i<=j;i++)
        {
          m_ij = m.containsKey(i) ? m.get(i):flee_v;
          n_jk = n.containsKey(i) ? n.get(i):0;
          result = result + m_ij*n_jk;
        }
        String [] key_string = key.toString().split(",");
        DecimalFormat df = new DecimalFormat("0.000000000");
        context.write(new Text(key_string[0]),new Text(df.format(result)));
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
    String input = path.get("tmp1");
    String output = path.get("tmp2");
    String pr = path.get("pr");
    conf.set("m","10878");
    conf.set("j","10878");
    conf.set("k","1");
    removeFile(output,conf);

    Job job = new Job(conf, "PageMul");
    job.setJarByClass(MDA_HW2_PageMul.class);
    job.setMapperClass(MatrixMapper.class);
    job.setReducerClass(MatrixReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, new Path(input) , new Path(pr));
    FileOutputFormat.setOutputPath(job, new Path(output));
    job.waitForCompletion(true);
    removeFile(pr,conf);
    renameFile(output,pr,conf);
    }
}
