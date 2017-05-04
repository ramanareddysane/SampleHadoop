
package URLFrequency;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class URLFrequencyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private static LongWritable frequency = new LongWritable();
    @Override
    protected void reduce(Text urlORip, Iterable<LongWritable> freq_counts, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for(LongWritable count : freq_counts)
            sum += count.get();
        frequency.set(sum);
        context.write(urlORip,frequency);
    }
}