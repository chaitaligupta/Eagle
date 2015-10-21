package eagle.jobhistory.zkres;

import java.util.List;

public interface JobHistoryZKStateLCM {
	void ensureJobPartitions(int numTotalPartitions);
	String readProcessedDate(int partitionId);
	List<String> readProcessedJobs(String date);
	void updateProcessedDate(int partitionId, String date);
	void addProcessedJob(String date, String jobId);
	void truncateProcessedJob(String date);
	void truncateEverything();
}
