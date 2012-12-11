// PatentJoin.java

// Blake Caldwell <blake.caldwell@colorado.edu>
// Lab 9
// CSCI 7000: Data Center Scale Computing 

package org.myorg;
        
import java.io.IOException;
import java.lang.Integer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PatentJoin extends Configured implements Tool {
    static enum PatentJoinType {
	MAP_CITE_FILE, MAP_PAT_FILE, BAD_RECORD_IN_REDUCE,
	    CITE_VALID, CITE_INVALID, CMADE_VALID,
	    PAT_VALID_STATE, PAT_INVALID_STATE, KEY_NOT_NUM,
	    CMADE_INVALID, PAT_INVALID_COUNTRY, BAD_RECORD,
	    REDUCE_VALID_PAT_RECORD, REDUCE_VALID_CIT_RECORD, REDUCE_INVALID_RECORD,
	    AUGMENT_VALID_CIT_RECORD,AUGMENT_VALID_PAT_RECORD,AUGMENT_INVALID_RECORD,
	    PAT_RECORD, CIT_RECORD, INTERMEDIATE_RECORD, REDUCE_VALID_INTERMEDIATE_RECORD
	    };
    

    public static class filterMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable,Text> {
	private Text citing = new Text();
	private Text country = new Text();
	private Text state = new Text();
	private Text out_value = new Text();
	private LongWritable patent = new LongWritable();
	private final static Text us_country = new Text("\"US\"");

	public void map(LongWritable key, Text value, OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {
	    int numTokens = 0;
	    Long number = 0L;
	    String bookend = "\"";
	    String line = value.toString();
            String[] words = line.split(",");
	    
	    numTokens = words.length;
	    
	    if (numTokens > 5) {
		reporter.incrCounter(PatentJoinType.PAT_RECORD,1);
		// This is a patent record. Our goal here is to pass through records that:
		//   1. are in the US
		//   2. have a valid state 
		country.set(words[4]);
		if (country.equals(us_country) == true) { 	// make sure its in the US and then get the state   
		    if ((words[5].length() == 4) && (words[5].startsWith(bookend)) && (words[5].endsWith(bookend))) {
			reporter.incrCounter(PatentJoinType.PAT_VALID_STATE,1);
			// now set the patent number
			try {
			    number = Long.parseLong(words[0],10);
			    patent.set(number);
			} catch (java.lang.NumberFormatException e) {
			    reporter.incrCounter(PatentJoinType.KEY_NOT_NUM,1);
			}
			//state.set(words[5]);
			//output.collect(patent,state);
			out_value.set(line);
			output.collect(patent,out_value);
		    }
		    else {
			reporter.incrCounter(PatentJoinType.PAT_INVALID_STATE,1);
		    }
		}
		else {
		    reporter.incrCounter(PatentJoinType.PAT_INVALID_COUNTRY,1);
		}
	    }
	    else if (numTokens == 2) {
		reporter.incrCounter(PatentJoinType.CIT_RECORD,1);
		// this is the citation file. swizzle cited and citing
		// and then pass all but the first record through
		String skip_string = "\"CITING\"";
		if (! words[0].startsWith(skip_string)) {
		    try {
			patent.set(Long.parseLong(words[1],10));
		    } catch (java.lang.NumberFormatException e) {
			reporter.incrCounter(PatentJoinType.KEY_NOT_NUM,1);
		    }
		    citing.set(words[0]);
		    output.collect(patent,citing);
		}
	    }
	    else {
		reporter.incrCounter(PatentJoinType.INTERMEDIATE_RECORD,1);
		output.collect(key,value);
	    }
	}
    }
    
    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable,Text>  {
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {
	    String patent_state=null;
	    ArrayList<String> citingPatents= new ArrayList<String>();
	    String currentVal=null;
	    String bookend = "\"";
	    Text value = new Text();
	    LongWritable citing = new LongWritable();
	    String[] words;
	    int numTokens = 0;

	    while (values.hasNext()) { 
		currentVal = values.next().toString();
		words=currentVal.split(",");
		numTokens = words.length;
		if (numTokens > 5) {
		    // patent data
		    if ((words[5].length() == 4) &&
			(words[5].startsWith(bookend)) && 
			(words[5].endsWith(bookend))) {
			reporter.incrCounter(PatentJoinType.REDUCE_VALID_PAT_RECORD,1);
			patent_state = words[5];
		    }
		}
		else {
		    // citation data
		    citingPatents.add(currentVal);
		}
	    }	    
	    if (patent_state != null) {
		for (String val: citingPatents) {
		    // swizzle back
		    citing.set(Long.parseLong(val,10));
		    value.set(key.toString() + " " + patent_state);
		    output.collect(citing, value);
		    reporter.incrCounter(PatentJoinType.REDUCE_VALID_INTERMEDIATE_RECORD,1);
		}
	    }
	    else {
		reporter.incrCounter(PatentJoinType.REDUCE_INVALID_RECORD,1);
	    }
	}
    }
    public static class swizzler extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable,Text> {
	private Text values = new Text();
	private Text country = new Text();
	private Text cited_state = new Text();
	private LongWritable output_key = new LongWritable();
	    
	private final static Text us_country = new Text("\"US\"");
	    
	public void map(LongWritable key, Text value, OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {

	    int numTokens = 0;
	    long num_citations_made = 0;
	    String line = value.toString();
            ArrayList<String> myWords = new ArrayList<String>();
	    if (line.split(",").length <= 1) {
		// hmm, splitting on ',' is not working well, so split on whitespace
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {           // iterate tokens separated by "," in a line
		    myWords.add(tokenizer.nextToken());
		}
	    }
	    else {
		for (String str : line.split(",")) {
		    myWords.add(str);
		}
	    }
	    String[] words = myWords.toArray(new String[myWords.size()]);
	    numTokens = words.length;

	    if (numTokens > 3) {	// this is the patent file
		country.set(words[4]);
		if (country.equals(us_country) == true) { 	// make sure its in the US and then get the state
		    if (words[5].length() == 4) { 
			reporter.incrCounter(PatentJoinType.PAT_VALID_STATE,1);
			if (words[12] != null) {
			    try {
				num_citations_made = Long.parseLong(words[12],10);				    
			    } catch (java.lang.NumberFormatException e) {
				System.err.println("error turning into long: " + words[12]);
				reporter.incrCounter(PatentJoinType.CMADE_INVALID,1);
				num_citations_made = 0;
			    }
			    if (num_citations_made > 0) {
				reporter.incrCounter(PatentJoinType.CMADE_VALID,1);
				try {
				    output_key.set(Long.parseLong(words[0],10));
				} catch (java.lang.NumberFormatException e) {
				    reporter.incrCounter(PatentJoinType.KEY_NOT_NUM,1);
				    return;
				}
				// send on the whole line to the reducer
				values.set(line);
				//values.set(words[5] + " " + words [12]);
				output.collect(output_key,values);
			    }
			    else {
				// make this counter balance (include CMADE=0)
				reporter.incrCounter(PatentJoinType.CMADE_INVALID,1);
			    }
			}
		    }
		    else {
			    reporter.incrCounter(PatentJoinType.PAT_INVALID_STATE,1);
		    }
		}
		else {
		    reporter.incrCounter(PatentJoinType.PAT_INVALID_COUNTRY,1);
		}		
	    }
	    else if (numTokens == 3 ) {     // this is intermediate citation data with the state
		String skip_string = "\"CITING\"";
		if (! words[0].startsWith(skip_string)) {
		    try {
			output_key.set(Long.parseLong(words[0],10));
		    } catch (java.lang.NumberFormatException e) {
			reporter.incrCounter(PatentJoinType.KEY_NOT_NUM,1);
		    }
		    // leave out the CITED patent - don't need now that we know the state
		    String bookend = "\"";
		    // make sure this is actually a string
		    if ((words[2].length() == 4) &&
			(words[2].startsWith(bookend)) &&
			(words[2].endsWith(bookend))) {
			cited_state.set(words[2]);
			output.collect(output_key, cited_state);
			reporter.incrCounter(PatentJoinType.CITE_VALID,1);	       
		    }
		    else {
			reporter.incrCounter(PatentJoinType.BAD_RECORD,1);
			System.err.println("Value should be a state: " + words[2]);
		    }
		}
		else {
		    reporter.incrCounter(PatentJoinType.CITE_INVALID,1);
		}
	    }
	    else {
		reporter.incrCounter(PatentJoinType.BAD_RECORD,1);
		System.err.println("Strange number of tokens in record:" + numTokens);
	    }
	}
    }
    public static class augmenter extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable,Text>  {
       public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {
	   String patent_state=null;
	   int cited_count = 0;
	   int num_splits = 0;
	   String patent_line=null;
	   ArrayList<String> citedStates= new ArrayList<String>();
	   String currentVal=null;
	   Text value = new Text();
	 
	   while (values.hasNext()) {
	       currentVal = values.next().toString();
	       num_splits = currentVal.split(",").length;
	       if (num_splits > 1) {
		   // this is the patent data
		   patent_line = currentVal;
	       }
	       else if (num_splits == 1) {
		   reporter.incrCounter(PatentJoinType.AUGMENT_VALID_CIT_RECORD,1);
		   // these are states of patents cited by this one
		   citedStates.add(currentVal);
	       }
	   }
	    
	   if ((patent_line != null) && (patent_line.split(",").length > 5)) {
	       patent_state = patent_line.split(",")[5];
	       if (patent_state.length() == 4) {
		   for (String state: citedStates) {
		       if (state.equals(patent_state)) {
			   cited_count++;
		       }
		   }
		   if (cited_count == 8) {
		       System.err.println("cited: " + cited_count);
		   }
		   value.set(patent_line + "," + cited_count);
		   output.collect(key, value);
		   reporter.incrCounter(PatentJoinType.AUGMENT_VALID_PAT_RECORD,1);
	       }
	   }
	   else {
	       reporter.incrCounter(PatentJoinType.AUGMENT_INVALID_RECORD,1);
	   }
       }
    }    
    public static class swizzleState extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private Text state = new Text();
	private Text country = new Text();
	private Text count = new Text();
	private final static Text us_country = new Text("\"US\"");

	public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
	    String bookend = "\"";
	    String line = value.toString();
            String[] words = line.split(",");
	    
	    int numTokens = words.length;
	    
	    if (numTokens > 5) {
		// should be, since the is patent data
		reporter.incrCounter(PatentJoinType.PAT_RECORD,1);
		// This is a patent record. Our goal here is to pass through records that:
		//   1. are in the US
		//   2. have a valid state 
		country.set(words[4]);
		if (country.equals(us_country) == true) { 	// make sure its in the US and then get the state   
		    if ((words[5].length() == 4) && (words[5].startsWith(bookend)) && (words[5].endsWith(bookend))) {
			reporter.incrCounter(PatentJoinType.PAT_VALID_STATE,1);
			state.set(words[5]);
			
			count.set(words[words.length-1] + "," + words[12]);
			output.collect(state,count);
		    }
		    else {
			reporter.incrCounter(PatentJoinType.PAT_INVALID_STATE,1);
		    }
		}
		else {
		    reporter.incrCounter(PatentJoinType.PAT_INVALID_COUNTRY,1);
		}
	    }
	}
    }
    
    public static class percentReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>  {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
	    float sum = 0;
	    float total = 0;
	    float percent = 0;
	    Text percent_out = new Text();
	    String value_string = new String();
	    String[] words;

	    while (values.hasNext()) {
		value_string = values.next().toString();
		words=value_string.split(",");
		if (words.length == 2) {
		    sum += Integer.parseInt(words[0],10);
		    total += Integer.parseInt(words[1],10);
		};
	    }	    
	    
	    percent = (sum / total);
	    percent_out.set(String.format("%02.2f",percent));
	    output.collect(key,percent_out);
	}
    }
    
    public int run(String[] args) throws Exception {
	JobConf job1 = new JobConf(new Configuration(), PatentJoin.class);
	job1.setJobName("patentjoin");        

	Path patent_data=new Path(args[0]);
	Path citation_data=new Path(args[1]);
	Path intermediate_data=new Path(args[2]);
	Path augmented_data=new Path(args[3]);
	Path output_data=new Path(args[4]);

	FileInputFormat.setInputPaths(job1, patent_data);
	FileInputFormat.addInputPath(job1, citation_data);
	FileOutputFormat.setOutputPath(job1, intermediate_data);

	job1.setInputFormat(TextInputFormat.class);
        job1.setOutputFormat(TextOutputFormat.class);
        job1.setJarByClass(org.myorg.PatentJoin.class); // added to example for local build

        JobConf mapAConf = new JobConf(false);
        ChainMapper.addMapper(job1, filterMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, mapAConf);

	JobConf reduceAConf = new JobConf(false);
	ChainReducer.setReducer(job1, Reduce.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, reduceAConf);
        //JobClient.runJob(job1);

	JobConf job2 = new JobConf(new Configuration(), PatentJoin.class);
	job2.setJobName("patentjoin2");        

	FileInputFormat.addInputPath(job2, patent_data);
	FileInputFormat.addInputPath(job2, intermediate_data);
	FileOutputFormat.setOutputPath(job2, augmented_data);

	job2.setInputFormat(TextInputFormat.class);
	job2.setOutputFormat(TextOutputFormat.class);
	job2.setJarByClass(org.myorg.PatentJoin.class); // added to example for local build
	
        JobConf mapBConf = new JobConf(false);
        ChainMapper.addMapper(job2, filterMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, mapBConf);        

        JobConf mapCConf = new JobConf(false);
        ChainMapper.addMapper(job2, swizzler.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, mapCConf);        

        JobConf reduceBConf = new JobConf(false);
        ChainReducer.setReducer(job2, augmenter.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, reduceBConf);        
	
	//JobClient.runJob(job2);
	
	JobConf job3 = new JobConf(new Configuration(), PatentJoin.class);
	job3.setJobName("patentjoin2");        

	FileInputFormat.addInputPath(job3, augmented_data);
	FileOutputFormat.setOutputPath(job3, output_data);

	job3.setInputFormat(TextInputFormat.class);
	job3.setOutputFormat(TextOutputFormat.class);
	job3.setJarByClass(org.myorg.PatentJoin.class); // added to example for local build
	
        JobConf mapDConf = new JobConf(false);
        ChainMapper.addMapper(job3, swizzleState.class, LongWritable.class, Text.class, Text.class, Text.class, true, mapBConf);    

        JobConf reduceCConf = new JobConf(false);
        ChainReducer.setReducer(job3, percentReduce.class, Text.class, Text.class, Text.class, Text.class, true, reduceCConf);	
	JobClient.runJob(job3);

	return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PatentJoin(), args);
        System.exit(res);
    }
    
}