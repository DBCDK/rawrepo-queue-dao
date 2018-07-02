package dk.dbc.rawrepo.queue;

import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class RawRepoQueueDAOImpl extends RawRepoQueueDAO {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RawRepoQueueDAOImpl.class.getName());
    private static final XLogger LOGGER_QUEUE = XLoggerFactory.getXLogger(RawRepoQueueDAOImpl.class.getName() + "#queue");
    private static final String LOG_DATABASE_ERROR = "Error accessing database";

    private final Connection connection;

    private static final String CALL_ENQUEUE = "SELECT * FROM enqueue(?, ?, ?, ?, ?, ?)";
    private static final String CALL_DEQUEUE = "SELECT * FROM dequeue(?)";
    private static final String CALL_DEQUEUE_MULTI = "SELECT * FROM dequeue(?, ?)";
    private static final String QUEUE_ERROR = "INSERT INTO jobdiag(bibliographicrecordid, agencyid, worker, error, queued) VALUES(?, ?, ?, ?, ?)";

    public RawRepoQueueDAOImpl(Connection connection) {
        this.connection = connection;
    }

    public void validateConnection() throws QueueException {

    }

    /**
     * Pull a job from the queue
     *
     * @param worker name of worker that want's to take a job
     * @return job description
     * @throws QueueException when something goes wrong
     */
    @Override
    public QueueItem dequeue(String worker) throws QueueException {
        try (CallableStatement stmt = connection.prepareCall(CALL_DEQUEUE)) {
            stmt.setString(1, worker);
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    QueueItem job = new QueueItem(resultSet.getString("bibliographicrecordid"),
                            resultSet.getInt("agencyid"),
                            resultSet.getString("worker"),
                            resultSet.getTimestamp("queued"),
                            resultSet.getInt("priority"));
                    LOGGER_QUEUE.debug("Dequeued job = {}; worker = {}", job, worker);
                    return job;
                }
                return null;
            }
        } catch (SQLException ex) {
            LOGGER.error(LOG_DATABASE_ERROR, ex);
            throw new QueueException("Error dequeueing job", ex);
        }
    }

    /**
     * Pull a job from the queue
     *
     * @param worker name of worker that want's to take a job
     * @return job description
     * @throws QueueException when something goes wrong
     */
    @Override
    public List<QueueItem> dequeue(String worker, int wanted) throws QueueException {
        List<QueueItem> result = new ArrayList<>();
        try (CallableStatement stmt = connection.prepareCall(CALL_DEQUEUE_MULTI)) {
            int pos = 1;
            stmt.setString(pos++, worker);
            stmt.setInt(pos, wanted);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    QueueItem job = new QueueItem(resultSet.getString("bibliographicrecordid"),
                            resultSet.getInt("agencyid"),
                            resultSet.getString("worker"),
                            resultSet.getTimestamp("queued"),
                            resultSet.getInt("priority"));
                    result.add(job);
                    LOGGER_QUEUE.debug("Dequeued job = {}; worker = {}", job, worker);
                }
                return result;
            }
        } catch (SQLException ex) {
            LOGGER.error(LOG_DATABASE_ERROR, ex);
            throw new QueueException("Error dequeueing jobs", ex);
        }
    }

    /**
     * QueueJob has failed, log to database
     *
     * @param queueJob job that failed
     * @param error    what happened (empty string not allowed)
     * @throws QueueException when something goes wrong
     */
    @Override
    public void queueFail(QueueItem queueJob, String error) throws QueueException {
        if (error == null || error.equals("")) {
            throw new QueueException("Error cannot be empty in queueFail");
        }
        try (PreparedStatement stmt = connection.prepareStatement(QUEUE_ERROR)) {
            int pos = 1;
            stmt.setString(pos++, queueJob.getBibliographicRecordId());
            stmt.setInt(pos++, queueJob.getAgencyId());
            stmt.setString(pos++, queueJob.getWorker());
            stmt.setString(pos++, error);
            stmt.setTimestamp(pos, queueJob.getQueued());
            stmt.executeUpdate();
        } catch (SQLException ex) {
            LOGGER.error(LOG_DATABASE_ERROR, ex);
            throw new QueueException("Error reporting job status", ex);
        }
    }

    /**
     * QueueJob has failed
     *
     * @param queueJob job that failed
     * @param error    what happened (empty string not allowed)
     * @throws QueueException when something goes wrong
     */
    @Override
    public void queueFailWithSavepoint(QueueItem queueJob, String error) throws QueueException {
        if (error == null || error.equals("")) {
            throw new QueueException("Error cannot be empty in queueFail");
        }
        try (PreparedStatement rollback = connection.prepareStatement("ROLLBACK TO DEQUEUED")) {
            rollback.execute();
        } catch (SQLException ex) {
            LOGGER.error(LOG_DATABASE_ERROR, ex);
            throw new QueueException("Error rolling back", ex);
        }
        queueFail(queueJob, error);
    }


}
