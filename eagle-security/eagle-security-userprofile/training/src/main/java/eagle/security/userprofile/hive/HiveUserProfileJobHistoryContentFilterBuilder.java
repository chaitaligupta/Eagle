package eagle.security.userprofile.hive;

import eagle.jobhistory.res.JobHistoryContentFilter;
import eagle.jobhistory.res.JobHistoryContentFilterBuilder;

import java.util.regex.Pattern;

public class HiveUserProfileJobHistoryContentFilterBuilder {
    public static JobHistoryContentFilter buildFilter(){
        return JobHistoryContentFilterBuilder.newBuilder().acceptJobConfFile().
                mustHaveJobConfKeyPatterns(Pattern.compile("hive.query.string")).
                includeJobKeyPatterns(Pattern.compile("hive.query.string"),
                        Pattern.compile("mapreduce.job.user.name"),
                        Pattern.compile("hive.current.database"),
                        Pattern.compile("mapreduce.job.cache.files.timestamps"))
                .build();
    }
}