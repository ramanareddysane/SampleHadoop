package Grep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GrepJob  extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration() , new GrepJob(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        // these are present in /user/ram/grep/... in hdfs
       String input_file_path = "grep/input/grep_input";
       String output_file_path = "grep/output/";
       
       Configuration conf = this.getConf(); // we will provide the regex word
       // in CLI with -D option with "regex" as key..
       Job job = Job.getInstance(conf, "Distributed Grep");
       job.setJarByClass(GrepJob.class);
       //set mapper for this job.. No reducer as of now..
       job.setMapperClass(GrepMapper.class);
       job.setNumReduceTasks(0); // means there are no reducers..
       // set input and output file formats..
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       //set output key and value types. This is applicable to both mapper and 
       //reducer. We can also specify for mapper saperately, if it's a need. 
       job.setOutputKeyClass(NullWritable.class);
       job.setOutputValueClass(Text.class);
       // set input and output path directories..
       FileInputFormat.addInputPath(job, new Path(input_file_path));
       FileOutputFormat.setOutputPath(job, new Path(output_file_path));
       
       return job.waitForCompletion(true) ? 0 : 1;
    }
}