
package URLFrequency;

import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class URLFrequencyJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(),new URLFrequencyJob(), args));
    }
    
    @Override
    public int run(String[] strings) throws Exception {
        String input_path = "urlfrequency/input";
        String output_pat = "urlfrequency/output";
        //get configuration object, create job and add Driver class...
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "URL Frequency Counter");
        job.setJarByClass(URLFrequencyJob.class);
        
        //set mapper to job based on user choice..
        System.out.print("By default we are doing actual url frequency job. "
                + "Do you want to go for IPAddress(from where the requests "
                + "are coming from, to this server..) frequency (yes/no)");
        Scanner sc = new Scanner(System.in);
        String choice = sc.next();
        if(choice.equalsIgnoreCase("yes"))
            job.setMapperClass(IPMapper.class);
        else
            job.setMapperClass(URLMapper.class);
        // set combiner, and reducer to job..
        job.setCombinerClass(URLFrequencyReducer.class);
        job.setReducerClass(URLFrequencyReducer.class);
        //set file input and output formats;
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //set output key and value types...
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // set input file path and output path
        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_pat));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}