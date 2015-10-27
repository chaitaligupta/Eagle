package eagle.jobhistory.res;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;

/**
 * define what content in job history stream should be streamed
 * @author yonzhang
 *
 */
public interface JobHistoryContentFilter extends Serializable {
	boolean acceptJobFile();
	boolean acceptJobConfFile();
	List<Pattern> getMustHaveJobConfKeyPatterns();
	List<Pattern> getJobConfKeyInclusionPatterns();
	List<Pattern> getJobConfKeyExclusionPatterns();
}
 