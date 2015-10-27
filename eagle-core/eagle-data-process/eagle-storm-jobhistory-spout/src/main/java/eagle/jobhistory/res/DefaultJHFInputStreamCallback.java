package eagle.jobhistory.res;

import eagle.dataproc.core.EagleOutputCollector;
import eagle.dataproc.core.ValuesArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The output includes two types of data:
 * 1. job data
 * 2. job configuration data
 * 
 * The above two types of data is sent through one single stream but can be differentiated by data type.
 * 
 * The output format is 2 fields, field 1 is data type (job data or job config data) and field 2 is Object
 * JOB_DATA -> {one line of job history log}
 * JOB_CONFIG_DATA -> {map of key/value pairs}
 * 
 * 
 * @author yonzhang
 *
 */
public class DefaultJHFInputStreamCallback implements JHFInputStreamCallback{

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(DefaultJHFInputStreamCallback.class);
	
	private JobHistoryContentFilter filter;
	private EagleOutputCollector eagleCollector;
	
	public DefaultJHFInputStreamCallback(JobHistoryContentFilter filter, EagleOutputCollector eagleCollector){
		this.filter = filter;
		this.eagleCollector = eagleCollector;
	}
	
	@Override
	public void onInputStream(JobMessageId msgId, InputStream jobFileInputStream, org.apache.hadoop.conf.Configuration conf) throws Exception{
		if(!filter.acceptJobFile()){
			// close immediately if we don't need job file
			jobFileInputStream.close();
		}else{
			// read each line and emit
			BufferedReader reader = new BufferedReader(new InputStreamReader(jobFileInputStream));
			String line = null;
			while((line = reader.readLine()) != null){
				eagleCollector.collect(new ValuesArray(msgId, JobHistoryCrawlConstants.JOB_DATA, line));
			}
			reader.close();
		}
		
		Map<String, Object> prop = new HashMap<String, Object>();
		if(filter.acceptJobConfFile()){
			Iterator<Map.Entry<String, String>> iter = conf.iterator();
			while(iter.hasNext()){
				String key = iter.next().getKey();
				if(included(key) && !excluded(key))
					prop.put(key, conf.get(key));
			}
		}
		LOG.info("prop size: " + prop.size());
		// check must-have keys are within prop
		if(matchMustHaveKeyPatterns(prop)){
			LOG.info("emit fields:" + prop);
			eagleCollector.collect(new ValuesArray(msgId, JobHistoryCrawlConstants.JOB_CONFIG_DATA, prop));
		}
	}
	
	private boolean matchMustHaveKeyPatterns(Map<String, Object> prop){
		if(filter.getMustHaveJobConfKeyPatterns() == null){
			return true;
		}
		
		for(Pattern p : filter.getMustHaveJobConfKeyPatterns()){
			boolean matched = false;
			for(Object key : prop.keySet()){
				if(p.matcher((String)key).matches()){
					matched = true;
					break;
				}
			}
			if(!matched)
				return false;
		}
		return true;
	}
	
	private boolean included(String key){
		if(filter.getJobConfKeyInclusionPatterns() == null)
			return true;
		for(Pattern p : filter.getJobConfKeyInclusionPatterns()){
			Matcher m = p.matcher(key);
			if(m.matches())
				return true;
		}
		return false;
	}
	
	private boolean excluded(String key){
		if(filter.getJobConfKeyExclusionPatterns() == null)
			return false;
		for(Pattern p : filter.getJobConfKeyExclusionPatterns()){
			Matcher m = p.matcher(key);
			if(m.matches())
				return true;
		}
		return false;
	}
}
