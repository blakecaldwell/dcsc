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
	MAP_CITE_FILE, MAP_PAT_FILE,
	    CITE_VALID, CITE_INVALID, CMADE_VALID,
	    PAT_VALID_STATE, PAT_INVALID_STATE,
	    NUMBER_EXCEPTION, PAT_INVALID_COUNTRY, BAD_RECORD
	    };
    

    public static class mapperA extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable,Text> {
	private Text state = new Text();
	private Text country = new Text();
	private Text cited = new Text();
	private LongWritable patent = new LongWritable();
	private String token = new String();
	private final static Text us_country = new Text("\"US\"");

	public void map(LongWritable key, Text value, OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {
	    int count = 0;
	    int numTokens = 0;
	    String line = value.toString();
	    String first_token = new String();
	    StringTokenizer tokenizer = new StringTokenizer(line,",");

	    numTokens = tokenizer.countTokens();
	    if (numTokens > 2) {
		// this is the patent file
		while (tokenizer.hasMoreTokens()) {           // iterate tokens separated by "," in a line
		    token = tokenizer.nextToken();
		    // skip the first 4 tokens (3 + the first one)
		    if (count == 0) {
			// just store this in case this is the first line
			first_token=token;
		    }
		    else if (count == 4) {
			country.set(token);
			if (country.equals(us_country) == false) { 	// make sure its in the US and then get the state
			    break;
			}
		    }	
		    else if (count == 5) {
			// check that state is two characters
 		    
			if (token.length() == 4) {
			    // now set the patent number
			    patent.set(Long.parseLong(first_token,10));
			
			    state.set(token);
			    output.collect(patent,state);
			}
			break;
		    }
		    count++;
		}
	    }
	    else if (numTokens == 2) {
		// this is the citation file
		String skip_string = "\"CITING\"";
		while (tokenizer.hasMoreTokens()) {           // iterate tokens separated by "," in a line
		    token = tokenizer.nextToken();
		    if (count == 0) {
			if (token.startsWith(skip_string)) {
			    break;
			}
			else {
			    patent.set(Long.parseLong(token,10));
			}
		    }
		    else if (count == 1) {
			cited.set(token);
			output.collect(patent,cited);
		    }	
		    count++;
		}
	    }
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable,Text>  {
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {
	    String patent_state=null;
	    ArrayList<String> citedPatents= new ArrayList<String>();
	    String currentVal=null;
	    Text value = new Text();
	    while (values.hasNext()) { 
		currentVal = values.next().toString();
		if (currentVal.length() == 4) {
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
	}
    }
    public static class swizzler extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable,Text> {
	private Text values = new Text();
	private Text country = new Text();
	private LongWritable cited_key = new LongWritable();
	private LongWritable patent = new LongWritable();
	private String value_string = new String();
	private final static Text us_country = new Text("\"US\"");

	public void map(LongWritable key, Text value, OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {
	    int numTokens = 0;
	    long num_citations_made = 0;
	    String line = value.toString();
            String[] words = line.split(",");
	    numTokens = words.length;

	    if (numTokens > 5) {	// this is the patent file
		System.out.println("There are " + numTokens + " tokens");
		country.set(words[4]);
		if (country.equals(us_country) == true) { 	// make sure its in the US and then get the state
		    if (words[5].length() == 4) { 
			reporter.incrCounter(PatentJoinType.PAT_VALID_STATE,1);
			if (words[12] != null) {
			    try {
				num_citations_made = Long.parseLong(words[12],10);
			    } catch (java.lang.NumberFormatException e) {
				reporter.incrCounter(PatentJoinType.NUMBER_EXCEPTION,1);
				num_citations_made = 0;
			    }
			    reporter.incrCounter(PatentJoinType.CMADE_VALID,1);
			    patent.set(Long.parseLong(words[0],10));
			    value_string = words[5] + " " + num_citations_made;
			    values.set(value_string);
			    output.collect(patent,values);
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
	    else if (numTokens >1 ) {     // this is the citation file
		String skip_string = "\"CITING\"";
		if (! words[0].startsWith(skip_string)) {
		    cited_key.set(Long.parseLong(words[1],10));
		    //value_string = words[0] + " " + words[2];
		    value_string = words[2];
		    values.set(value_string);
		    output.collect(cited_key, values);
		    reporter.incrCounter(PatentJoinType.CITE_VALID,1);	       
		}
		else {
		    reporter.incrCounter(PatentJoinType.CITE_INVALID,1);
		}
	    }
	    else {
		reporter.incrCounter(PatentJoinType.BAD_RECORD,1);
		System.out.println("BAD RECORD = " + line );
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
        //JobClient.runJob(job1);

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
	//job2.setReducerClass(IdentityReducer.class);
	JobClient.runJob(job2);
	return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PatentJoin(), args);
        System.exit(res);
    }
    
}