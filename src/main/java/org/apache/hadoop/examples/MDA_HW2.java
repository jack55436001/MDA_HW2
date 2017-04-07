package org.apache.hadoop.examples;

import java.io.*;
import java.util.*;
import java.net.*; 
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class MDA_HW2{
  public static void main(String[] args){
      Map<String, String> path = setInOut();
      try{
        MDA_HW2_Adj.run(path);
        int Iter = 1;
        for(int i=0; i < Iter ;i++){
            MDA_HW2_PageMul.run(path);
            MDA_HW2_compen.run(path);
        }
        sortAndWrite(path);
      }
      catch(Exception e){
        e.printStackTrace();
      }
      System.exit(0);
  }
  private static Map<String,String> setInOut(){
    Map<String, String> path = new HashMap<String, String>();
    path.put("adj","/user/root/data/adj5.txt");
    path.put("pr","/user/root/data/input_pr5");
    path.put("tmp1","/user/root/data/tmp1");
    path.put("tmp2","/user/root/data/tmp2");
    path.put("tmp3","/user/root/data/tmp3");
    path.put("output","/user/root/output/pagerank5");
    return path;
  }
  
  private static void sortAndWrite(Map<String,String> path){
    try{
        Path pt=new Path(path.get("pr")+"/part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        FSDataOutputStream os = null;
        List<MyEntry<String,Float>> myList = new ArrayList<MyEntry<String,Float>>();
        String line;
        line=br.readLine();
        while (line != null){
             String [] str = line.split("\t");
             myList.add(new MyEntry<String,Float>(str[0],Float.parseFloat(str[1])));
             line=br.readLine();
         }
         
        Collections.sort(myList, new Comparator<MyEntry<String, Float>>() {
    	    public int compare(MyEntry<String, Float> x, MyEntry<String, Float> y) {
    	    	if(x.getValue() - y.getValue()>0)
    	    		return -1;
    	    	else if(x.getValue() - y.getValue()==0)
    	    		return 0;
    	    	else
    	    		return 1;
    	    }
    	  });
         
        String content="";
        for(int i=0;i<myList.size();i++)
            content=content+myList.get(i).getKey()+"\t"+myList.get(i).getValue().toString()+"\n";
            
        byte[] buff = content.getBytes();
        
        os = fs.create(new Path(path.get("output")));
        os.write(buff, 0, buff.length);
        if(os != null)
        os.close();
        fs.close();
         
         
    }catch(Exception e){

    }
  }

}
