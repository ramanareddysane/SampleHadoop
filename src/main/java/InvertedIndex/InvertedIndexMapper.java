package InvertedIndex;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static Text word_name = new Text(); // for output key..
    private static Text doc_name = new Text(); // for output value..
    @Override
    public void map(LongWritable key, Text value, Context context) throws 
            IOException, InterruptedException {
        //get the file name, for which mapper is running..
        String file_name = ((FileSplit)context.getInputSplit()).getPath().getName();
        doc_name.set(file_name);
        String words[] = value.toString().split(" ");
        for(String word : words){
            word_name.set(word.toLowerCase()); // to avoid duplicate words, all
            // keys must be stored in case Insensitive manner...
            context.write(word_name, doc_name);
        }
    }
}