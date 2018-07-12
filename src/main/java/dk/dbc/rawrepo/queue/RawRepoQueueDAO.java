package dk.dbc.rawrepo.queue;

import java.sql.Connection;
import java.util.List;

public abstract class RawRepoQueueDAO {

    public static class Builder {
        private final Connection connection;

        private Builder(Connection connection) {
            this.connection = connection;
        }

        public RawRepoQueueDAO build() throws QueueException {
            RawRepoQueueDAOImpl dao = new RawRepoQueueDAOImpl(connection);
            dao.validateConnection();

            return dao;
        }
    }

    /**
     * Make a dao builder
     *
     * @param connection the database configuration
     * @return builder
     */
    public static Builder builder(Connection connection) {
        return new Builder(connection);
    }

    /**
     * Pull a job from the queue
     * <p>
     * Note: a queue should be dequeued either with this or
     * {@link #dequeue(java.lang.String, int) dequeue}, but not both. It could
     * break for long queues.
     *
     * @param worker name of worker that want's to take a job
     * @return job description
     * @throws QueueException done at failure
     */
    public abstract QueueItem dequeue(String worker) throws QueueException;

    /**
     * Pull jobs from the queue
     * <p>
     * Note: a queue should be dequeued either with this or
     * {@link #dequeue(java.lang.String) dequeue}, but not both. It could break
     * for long queues.
     *
     * @param worker name of worker that want's to take a job
     * @param wanted number of jobs to dequeue
     * @return job description list
     * @throws QueueException done at failure
     */
    public abstract List<QueueItem> dequeue(String worker, int wanted) throws QueueException;

    /**
     * QueueJob has failed
     *
     * @param queueJob job that failed
     * @param error    what happened (empty string not allowed)
     * @throws QueueException done at failure
     */
    public abstract void queueFail(QueueItem queueJob, String error) throws QueueException;

    /**
     * QueueJob has failed
     *
     * @param queueJob job that failed
     * @param error    what happened (empty string not allowed)
     * @throws QueueException done at failure
     */
    public abstract void queueFailWithSavepoint(QueueItem queueJob, String error) throws QueueException;
}
