package URLFrequency;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class URLMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static Text url = new Text();
    private static final LongWritable one = new LongWritable(1);
    @Override
    protected void map(LongWritable byte_offset, Text line, Context context) throws IOException, InterruptedException {
        // get the html file name from the line..
        Matcher matcher = Pattern.compile("(?<=GET.\\/).*\\.html",Pattern.CASE_INSENSITIVE)
                                 .matcher(line.toString());
        if(matcher.find()){
            url.set(matcher.group(0));
            context.write(url, one);
        }
    }
}