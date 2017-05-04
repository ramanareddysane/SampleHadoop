package InvertedIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text word, Iterable<Text> file_names, Context context) throws
            IOException, InterruptedException {
        // preferred hashset over arraylist is to avoid duplicates in file names.
        // It will occur if a word is preset in same file more than once..
        HashSet<String> docs = new HashSet<>();
        for(Text doc : file_names){
            docs.add(doc.toString());
        }
        List<String> list = new ArrayList<>(docs); // best for retrieval
        Collections.sort(list, String.CASE_INSENSITIVE_ORDER);
        //   print the words and docs in alphabetical order.. you can change the
        //sorting order if you want to..
        context.write(word, new Text(list.toString()));
        
    }
    
}