package eagle.jobhistory.res;

import java.util.List;
import java.util.regex.Pattern;

public class JobHistoryContentFilterImpl implements JobHistoryContentFilter{
	private boolean acceptJobFile;
	private boolean acceptJobConfFile;
	private List<Pattern> mustHaveJobConfKeyPatterns;
	private List<Pattern> jobConfKeyInclusionPatterns;
	private List<Pattern> jobConfKeyExclusionPatterns;
	
	@Override
	public boolean acceptJobFile() {
		return acceptJobFile;
	}

	@Override
	public boolean acceptJobConfFile() {
		return acceptJobConfFile;
	}
	
	@Override
	public List<Pattern> getMustHaveJobConfKeyPatterns() {
		return mustHaveJobConfKeyPatterns;
	}
	
	@Override
	public List<Pattern> getJobConfKeyInclusionPatterns() {
		return jobConfKeyInclusionPatterns;
	}
	
	@Override
	public List<Pattern> getJobConfKeyExclusionPatterns() {
		return jobConfKeyExclusionPatterns;
	}

	public void setAcceptJobFile(boolean acceptJobFile) {
		this.acceptJobFile = acceptJobFile;
	}

	public void setAcceptJobConfFile(boolean acceptJobConfFile) {
		this.acceptJobConfFile = acceptJobConfFile;
	}

	public void setJobConfKeyInclusionPatterns(
			List<Pattern> jobConfKeyInclusionPatterns) {
		this.jobConfKeyInclusionPatterns = jobConfKeyInclusionPatterns;
	}

	public void setJobConfKeyExclusionPatterns(
			List<Pattern> jobConfKeyExclusionPatterns) {
		this.jobConfKeyExclusionPatterns = jobConfKeyExclusionPatterns;
	}
	
	public void setMustHaveJobConfKeyPatterns(List<Pattern> mustHaveJobConfKeyPatterns){
		this.mustHaveJobConfKeyPatterns = mustHaveJobConfKeyPatterns;
	}	
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("job history file:");
		sb.append(acceptJobFile);
		sb.append(", job config file:");
		sb.append(acceptJobConfFile);
		if(acceptJobConfFile){
			sb.append(", must contain keys:");
			sb.append(mustHaveJobConfKeyPatterns);
			sb.append(", include keys:");
			sb.append(jobConfKeyInclusionPatterns);
			sb.append(", exclude keys:");
			sb.append(jobConfKeyExclusionPatterns);
		}
		return sb.toString();
	}
}
