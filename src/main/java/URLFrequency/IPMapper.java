
package URLFrequency;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IPMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final LongWritable one = new LongWritable(1);
    private static Text ip_address = new Text();
    @Override
    protected void map(LongWritable byte_offset, Text line, Context context) 
            throws IOException, InterruptedException {
        String log = line.toString().trim();
        String ip = log.substring(0, log.indexOf(' '));
        ip_address.set(ip);
        context.write(ip_address, one);
    }
}
