
package com.ramsane.samplehadoop;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * @author ram
 */
public class ListStatuses {
    
    static String getFileNameFromPath(String path){
        String name = "";
        Pattern pattern = Pattern.compile("/([^/]+)$");
        Matcher matcher = pattern.matcher(path);
        if(matcher.find())
            name = matcher.group(1);
        return name;
    }
    
    public static void main(String[] args) {
        String file_path = "/user/ram/";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        try {
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] statuses = fs.listStatus(new Path(file_path));
            System.out.println("\n");
            System.out.println("Output: ");
            for(FileStatus status : statuses){
                System.out.println("Owner: "+status.getOwner());
                System.out.println("Replication: "+status.getReplication());
                System.out.println("file_name: "+status.getPath());
                System.out.println();
            }
            
            System.out.println("simple list of files..: ");
            for(Path p : FileUtil.stat2Paths(statuses))
                System.out.println(getFileNameFromPath(p.toString()));
            
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
        
    }
 
}
