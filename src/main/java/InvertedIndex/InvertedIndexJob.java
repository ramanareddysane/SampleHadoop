package InvertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new InvertedIndexJob(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        String input_path = "invertedIndex/input"; // contains input files in dir in hdfs
        String output_dir = "invertedIndex/output"; // output dir in hdfs
        Configuration conf = this.getConf();
        // create job and assign driver class..
        Job job = Job.getInstance(conf, "INverted Index");
        job.setJarByClass(InvertedIndexJob.class);
        //set mapper and reducer...
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        //set input and output file formats..
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //set output key and value types..
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //finally.., add input and output paths to this MR job..
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_dir));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}