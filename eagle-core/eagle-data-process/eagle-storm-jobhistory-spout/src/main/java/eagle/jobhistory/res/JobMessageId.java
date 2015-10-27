package eagle.jobhistory.res;

import com.google.common.base.Objects;

public class JobMessageId {
	public String jobID;
	public String completedTime;

	public JobMessageId(String jobID, String completedTime) {
		this.jobID = jobID;
		this.completedTime = completedTime;
	}

	@Override
	public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final JobMessageId other = (JobMessageId) obj;
        return Objects.equal(this.jobID, other.jobID) 
        	  && Objects.equal(this.completedTime, other.completedTime);
	}
	
	@Override
	public int hashCode() {
		return jobID.hashCode() ^ completedTime.hashCode();
	}
	
	@Override
	public String toString() {
		return "jobID=" + jobID 
			 + ", completedTime=" + completedTime;
	}
}
