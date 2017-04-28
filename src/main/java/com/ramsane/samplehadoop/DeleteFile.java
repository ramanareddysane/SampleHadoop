package com.ramsane.samplehadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author ram
 */
public class DeleteFile {
    public static void main(String[] args) {
        String file_path = "temp/";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        try {
            FileSystem fs = FileSystem.get(conf);
            if(fs.delete(new Path(file_path), true))
                System.out.println("all files in " + "/user/ram/temp/" +" including"
                        + " 'temp' directory is deleted...");
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
        
    }
}
