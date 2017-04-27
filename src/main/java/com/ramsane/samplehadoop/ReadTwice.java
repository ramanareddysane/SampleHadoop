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
public class ReadTwice {
    public static void main(String[] args) {
        Configuration cfg = new Configuration();
        cfg.set("fs.defaultFS", "hdfs://localhost:9000");
        FSDataInputStream in = null;
        try {
            FileSystem fs = FileSystem.get(cfg);
            in = fs.open(new Path("/big"));
            System.out.println("First TIme...");
            IOUtils.copyBytes(in, System.out, 4096,false);
            System.out.println("Second time..");
            in.seek(0);
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }finally{
            IOUtils.closeStream(in);
        }
    }
}
