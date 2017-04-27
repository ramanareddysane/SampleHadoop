/*
    this program copies the contents of a file 
    from local machine to hdfs file. We create 
    the file if it doesn't exists..
 */
package com.ramsane.samplehadoop;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 *
 * @author ram
 */
public class CopyContents {
    public static void main(String[] args) {
        String local_path = "/home/ram/Desktop/local_file";
        String dest_path = "copied_from_local";
        try {
            //get input stream of the local file..
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(local_path));
            // get access to hdfs file system..
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            FileSystem fs = FileSystem.get(conf);
            FSDataOutputStream out = fs.create(new Path(dest_path));
            //copy the contents of local file to the newly created file in hdfs..
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }
}
