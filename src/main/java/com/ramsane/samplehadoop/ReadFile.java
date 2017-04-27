/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ramsane.samplehadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 *
 * @author ram
 */
public class ReadFile {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FSDataInputStream is = null;
        try {
            // get file system object....
            FileSystem fs = FileSystem.get(conf);
            is = fs.open(new Path("/big"));
            IOUtils.copyBytes(is, System.out, 4096, false);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }finally{
            IOUtils.closeStream(is);
        }
    }
}
