// PatentJoin.java

// Blake Caldwell <blake.caldwell@colorado.edu>
// Lab 9
// CSCI 7000: Data Center Scale Computing 

package org.myorg;
        
import java.io.IOException;
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
	    NUMBER_EXCEPTION, PAT_INVALID_COUNTRY, BAD_RECORD,
	    REDUCE_INVALID_STATE, REDUCE_VALID_STATE, REDUCE_BAD_RECORD
	    };
    

    public static class mapperA extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable,Text> {
	private Text cited = new Text();
	private Text country = new Text();
	private Text state = new Text();
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
			state.set(words[5]);
			output.collect(patent,state);
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
		// this is the citation file. just pass all by the first record through
		String skip_string = "\"CITING\"";
		if (! words[0].startsWith(skip_string)) {
		    try {
			patent.set(Long.parseLong(words[0],10));
		    } catch (java.lang.NumberFormatException e) {
			reporter.incrCounter(PatentJoinType.KEY_NOT_NUM,1);
		    }
		    cited.set(words[1]);
		    output.collect(patent,cited);
		}
	    }
	}
    }
    
    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable,Text>  {
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {
	    String patent_state=null;
	    ArrayList<String> citedPatents= new ArrayList<String>();
	    String currentVal=null;
	    String bookend = "\"";
	    Text value = new Text();

	    while (values.hasNext()) { 
		currentVal = values.next().toString();
		if ((currentVal.length() == 4) &&
		    (currentVal.startsWith(bookend)) && 
		    (currentVal.endsWith(bookend))) {
		    reporter.incrCounter(PatentJoinType.REDUCE_VALID_STATE,1);
		    patent_state = currentVal;
		}
		else {
		    citedPatents.add(currentVal);
		}
	    }	    
	    if (patent_state != null) {
		for (String val: citedPatents) {
		    value.set(val + " " + patent_state);
		    output.collect(key, value);
		}
	    }
	    else {
		reporter.incrCounter(PatentJoinType.REDUCE_INVALID_STATE,1);
	    }
	}
    }
    public static class swizzler extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable,Text> {
	private Text values = new Text();
	private Text country = new Text();
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
				reporter.incrCounter(PatentJoinType.NUMBER_EXCEPTION,1);
				num_citations_made = 0;
			    }
			    if (num_citations_made > 0) {
				reporter.incrCounter(PatentJoinType.CMADE_VALID,1);
				try {
				    output_key.set(Long.parseLong(words[0],10));
				} catch (java.lang.NumberFormatException e) {
				    reporter.incrCounter(PatentJoinType.KEY_NOT_NUM,1);
				}
				// send on the whole line to the reducer
				values.set(words[5] + " " + words [12]);
				output.collect(output_key,values);
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
	    else if (numTokens == 3 ) {     // this is the citation file
		String skip_string = "\"CITING\"";
		if (! words[0].startsWith(skip_string)) {
		    // swizzle: make the cited patent the key
		    try {
			output_key.set(Long.parseLong(words[1],10));
		    } catch (java.lang.NumberFormatException e) {
			reporter.incrCounter(PatentJoinType.KEY_NOT_NUM,1);
		    }
		    // leave out the CITING patent - don't need now that we know the state
		    String bookend = "\"";
		    // make sure this is actually a string
		    //if ((words[2].startsWith(bookend)) && (words[2].endsWith(bookend))) {
		    if (words[2].length() == 4) {
			output.collect(output_key, values);
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
	private String patent_state=null;
	private int cited_count = 0;
	private String patent_line=null;
	private ArrayList<String> citedStates= new ArrayList<String>();
	private String currentVal=null;
	private Text value = new Text();
	
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {
	    while (values.hasNext()) { 
		currentVal = values.next().toString();
		if (currentVal.length() > 4) {
		    // this is the patent data
		    patent_line = currentVal;
		}
		else {
		    // these are states of patents cited by this one
		    citedStates.add(currentVal);
		}
	    }
	    
	    if (patent_line != null) {
		patent_state = currentVal.split(",")[5];
		if (patent_state.length() == 4) {
		    for (String state: citedStates) {
			if (state == patent_state) {
			    cited_count++;
			}
		    }
		    value.set(patent_state + "," + cited_count);
		    output.collect(key, value);
		    reporter.incrCounter(PatentJoinType.REDUCE_VALID_STATE,1);
		}
		else {
		    reporter.incrCounter(PatentJoinType.REDUCE_INVALID_STATE,1);
		}
	    }
	    else {
		reporter.incrCounter(PatentJoinType.REDUCE_BAD_RECORD,1);
	    }
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

        //JobConf mapAConf = new JobConf(false);
        //ChainMapper.addMapper(job1, mapperA.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, mapAConf);
	job1.setMapperClass(mapperA.class);

	// Just use a single Reducer. Use these lines to use a chain reducer
	//JobConf reduceConf = new JobConf(false);
	//ChainReducer.setReducer(conf, Reduce.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, reduceConf);
	job1.setReducerClass(Reduce.class);
        JobClient.runJob(job1);

	JobConf job2 = new JobConf(new Configuration(), PatentJoin.class);
	job2.setJobName("patentjoin2");        

	FileInputFormat.addInputPath(job2, patent_data);
	FileInputFormat.addInputPath(job2, intermediate_data);
	FileOutputFormat.setOutputPath(job2, augmented_data);

	job2.setInputFormat(TextInputFormat.class);
	job2.setOutputFormat(TextOutputFormat.class);
	job2.setJarByClass(org.myorg.PatentJoin.class); // added to example for local build

        //JobConf mapBConf = new JobConf(false);
        //ChainReducer.addMapper(conf, swizzler.class, LongWritable.class, Text.class, LongWritable.class, Text.class, true, mapBConf);        
        job2.setMapperClass(swizzler.class);
	//job2.setReducerClass(augmenter.class);
	JobClient.runJob(job2);
	return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PatentJoin(), args);
        System.exit(res);
    }
    
}