package org.apache.hadoop.examples;

import java.util.HashMap;
import java.util.Map;

public class MDA_HW2{
  public static void main(String[] args){
      Map<String, String> path = setInOut();
      try{
        MDA_HW2_Adj.run(path);
        int Iter = 2;
        for(int i=0; i < Iter ;i++)
            MDA_HW2_PageMul.run(path);
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
    return path;
  }


}
