/*
The input file contents are...:
        HelloDate.java.  
        import java.util;
        The second example program, and the first example of a class comment.
        Key new ides: (a) Use of import and Java standard library, (b)
        use of comment documentation.  Note the openning
        This example stolen from Bruce Ecke's Thinking in Java.  Most
        examples stolen from Pat Troy and modified lightly.  Examples
        generally don't have any comments; student code will have
        comments, and in particular, class comments.
        @author Robert H. Sloan

        Sole entry point to application, as always.  
        Example of a function comment as well.
        @parem args array of strings, ignored here (command line input)
        @return No value is returned

        public static void main (String[] args)
        System.out.println ("Hello, it's: ");
        System.out.println(new Date());
     You can give any word/regex to get any of the lines. otherwise you might 
get nothing..
 */
package Grep;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author ram
 */
public class GrepMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
    private static NullWritable word_key = NullWritable.get();
    @Override
    public void map(LongWritable key, Text value, Context context) throws 
            IOException, InterruptedException {
        String regex = context.getConfiguration().get("regex", ".*");
        regex = "(?i).*"+regex+".*";
        String line = value.toString();
        if(line.matches(regex))
            context.write(word_key, value);
    }
    
}