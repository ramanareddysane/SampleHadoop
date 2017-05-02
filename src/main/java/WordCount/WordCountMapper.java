
package WordCount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
    
    private static Text word = new Text(); //for output key value
    private static final LongWritable one = new LongWritable(1); //for output value
    @Override
    public void map(LongWritable byteOffset, Text line, Context context)
                    throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(line.toString());
        while(tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
}