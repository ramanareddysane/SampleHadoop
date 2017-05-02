package WordCount;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private static LongWritable res = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) 
            throws IOException, InterruptedException {
        int sum = 0;
        for(LongWritable value : values)
            sum += value.get();
        res.set(sum);
        context.write(key,res);
    }
    
}
