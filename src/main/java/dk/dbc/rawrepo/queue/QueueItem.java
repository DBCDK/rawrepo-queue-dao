package dk.dbc.rawrepo.queue;

import java.sql.Timestamp;

public class QueueItem {

    private String bibliographicRecordId;
    private int agencyId;
    private String worker;
    private Timestamp queued;
    private int priority;

    public QueueItem() {
    }

    public QueueItem(String bibliographicRecordId, int agencyId, String worker, Timestamp queued, int priority) {
        this.bibliographicRecordId = bibliographicRecordId;
        this.agencyId = agencyId;
        this.worker = worker;
        this.queued = queued;
        this.priority = priority;
    }

    public String getBibliographicRecordId() {
        return bibliographicRecordId;
    }

    public int getAgencyId() {
        return agencyId;
    }

    public String getWorker() {
        return worker;
    }

    public Timestamp getQueued() {
        return queued;
    }

    public int getPriority() {
        return priority;
    }

    @Override
    public String toString() {
        return "QueueItem{" +
                "bibliographicRecordId='" + bibliographicRecordId + '\'' +
                ", agencyId=" + agencyId +
                ", worker='" + worker + '\'' +
                ", queued=" + queued +
                ", priority=" + priority +
                '}';
    }
}
