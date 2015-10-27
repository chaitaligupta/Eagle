package eagle.jobhistory.res;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobHistoryContentFilterBuilder{
	private final static Logger LOG = LoggerFactory.getLogger(JobHistoryContentFilterBuilder.class);
	
	private boolean acceptJobFile;
	private boolean acceptJobConfFile;
	private List<Pattern> mustHaveJobConfKeyPatterns;
	private List<Pattern> jobConfKeyInclusionPatterns;
	private List<Pattern> jobConfKeyExclusionPatterns;
	
	public static JobHistoryContentFilterBuilder newBuilder(){
		return new JobHistoryContentFilterBuilder();
	}
	
	public JobHistoryContentFilterBuilder acceptJobFile(){
		this.acceptJobFile = true;
		return this;
	}
	
	public JobHistoryContentFilterBuilder acceptJobConfFile(){
		this.acceptJobConfFile = true;
		return this;
	}
	
	public JobHistoryContentFilterBuilder mustHaveJobConfKeyPatterns(Pattern ...patterns){
		mustHaveJobConfKeyPatterns = Arrays.asList(patterns);
		if(jobConfKeyInclusionPatterns != null){
			List<Pattern> list = new ArrayList<Pattern>();
			list.addAll(jobConfKeyInclusionPatterns);
			list.addAll(Arrays.asList(patterns));
			jobConfKeyInclusionPatterns = list;
		}
		else
			jobConfKeyInclusionPatterns = Arrays.asList(patterns);
		return this;
	}
	
	public JobHistoryContentFilterBuilder includeJobKeyPatterns(Pattern ... patterns){
		if(jobConfKeyInclusionPatterns != null){
			List<Pattern> list = new ArrayList<Pattern>();
			list.addAll(jobConfKeyInclusionPatterns);
			list.addAll(Arrays.asList(patterns));
			jobConfKeyInclusionPatterns = list;
		}else
			jobConfKeyInclusionPatterns = Arrays.asList(patterns);
		return this;
	}
	
	public JobHistoryContentFilterBuilder excludeJobKeyPatterns(Pattern ...patterns){
		jobConfKeyExclusionPatterns = Arrays.asList(patterns);
		return this;
	}
	
	public JobHistoryContentFilter build(){
		JobHistoryContentFilterImpl filter = new JobHistoryContentFilterImpl();
		filter.setAcceptJobFile(acceptJobFile);
		filter.setAcceptJobConfFile(acceptJobConfFile);
		filter.setMustHaveJobConfKeyPatterns(mustHaveJobConfKeyPatterns);
		filter.setJobConfKeyInclusionPatterns(jobConfKeyInclusionPatterns);
		filter.setJobConfKeyExclusionPatterns(jobConfKeyExclusionPatterns);
		LOG.info("job history content filter:" + filter);
		return filter;
	}
}
