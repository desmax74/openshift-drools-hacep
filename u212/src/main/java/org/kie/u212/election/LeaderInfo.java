package org.kie.u212.election;

import java.util.Date;
import java.util.Set;

/**
 * from org.apache.camel.component.kubernetes.cluster.lock
 * Overview of a leadership status.
 */
public class LeaderInfo {

    private String groupName;

    private String leader;

    private Date localTimestamp;

    private Set<String> members;

    public LeaderInfo() {
    }

    public LeaderInfo(String groupName, String leader, Date timestamp, Set<String> members) {
        this.groupName = groupName;
        this.leader = leader;
        this.localTimestamp = timestamp;
        this.members = members;
    }

    public boolean hasEmptyLeader() {
        return this.leader == null;
    }

    public boolean hasValidLeader() {
        return this.leader != null && this.members.contains(this.leader);
    }

    public boolean isValidLeader(String pod) {
        if(pod == null) return false;
        return hasValidLeader() && pod.equals(leader);
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public Date getLocalTimestamp() {
        return localTimestamp;
    }

    public void setLocalTimestamp(Date localTimestamp) {
        this.localTimestamp = localTimestamp;
    }

    public Set<String> getMembers() {
        return members;
    }

    public void setMembers(Set<String> members) {
        this.members = members;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LeaderInfo{");
        sb.append("groupName='").append(groupName).append('\'');
        sb.append(", leader='").append(leader).append('\'');
        sb.append(", localTimestamp=").append(localTimestamp);
        sb.append(", members=").append(members);
        sb.append('}');
        return sb.toString();
    }
}

