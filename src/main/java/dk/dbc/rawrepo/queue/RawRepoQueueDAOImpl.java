package dk.dbc.rawrepo.queue;

import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RawRepoQueueDAOImpl extends RawRepoQueueDAO {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RawRepoQueueDAOImpl.class.getName());
    private static final XLogger LOGGER_QUEUE = XLoggerFactory.getXLogger(RawRepoQueueDAOImpl.class.getName() + "#queue");
    private static final String LOG_DATABASE_ERROR = "Error accessing database";

    private final Connection connection;

    private static final String VALIDATE_CONNECTION = "SELECT 1";
    private static final String CALL_ENQUEUE = "SELECT * FROM enqueue(?, ?, ?, ?, ?, ?)";
    private static final String CALL_DEQUEUE = "SELECT * FROM dequeue(?)";
    private static final String CALL_DEQUEUE_MULTI = "SELECT * FROM dequeue(?, ?)";
    private static final String QUEUE_ERROR = "INSERT INTO jobdiag(bibliographicrecordid, agencyid, worker, error, queued) VALUES(?, ?, ?, ?, ?)";
    private static final String CONFIGURATIONS_ALL = "SELECT key, value FROM configurations";

    public RawRepoQueueDAOImpl(Connection connection) {
        this.connection = connection;
    }

    public void validateConnection() throws QueueException {
        int reply = 0;
        try (CallableStatement stmt = connection.prepareCall(VALIDATE_CONNECTION)) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    reply = resultSet.getInt(1);
                }
            }
            if (reply != 1) {
                throw new QueueException("Database error! '" + Integer.toString(reply) + "' was returned instead of '1'");
            }
        } catch (SQLException ex) {
            LOGGER.error(LOG_DATABASE_ERROR, ex);
            throw new QueueException("Error connection to the database engine", ex);
        }
    }

    @Override
    public HashMap<String, String> getConfiguration() throws ConfigurationException {
        HashMap<String, String> configuration = new HashMap<>();
        try (CallableStatement stmt = connection.prepareCall(CONFIGURATIONS_ALL)) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    configuration.put(resultSet.getString("key"), resultSet.getString("value"));
                }
                return configuration;
            }
        } catch (SQLException ex) {
            LOGGER.error(LOG_DATABASE_ERROR, ex);
            throw new ConfigurationException("Error dequeueing jobs", ex);
        }
    }

    /**
     * Put job(s) on the queue (in the database)
     *
     * @param bibliographicRecordId id of the record to queue
     * @param agencyId              the agency owning the record
     * @param provider              change initiator
     * @param changed               is job for a record that has been changed
     * @param leaf                  is this job for a tree leaf
     * @throws QueueException when something goes wrong
     */
    @Override
    public void enqueue(String bibliographicRecordId, int agencyId, String provider, boolean changed, boolean leaf) throws QueueException {
        enqueue(bibliographicRecordId, agencyId, provider, changed, leaf, 1000);
    }

    @Override
    public void enqueue(String bibliographicRecordId, int agencyId, String provider, boolean changed, boolean leaf, int priority) throws QueueException {
        String recordId = bibliographicRecordId + ":" + agencyId;
        LOGGER.debug("Enqueue: job = {}; provider = {}; changed = {}; leaf = {}, priority = {}", recordId, provider, changed, leaf, priority);

        try (PreparedStatement stmt = connection.prepareStatement(CALL_ENQUEUE)) {
            stmt.setString(1, bibliographicRecordId);
            stmt.setInt(2, agencyId);
            stmt.setString(3, provider);
            stmt.setString(4, changed ? "Y" : "N");
            stmt.setString(5, leaf ? "Y" : "N");
            stmt.setInt(6, priority);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    if (resultSet.getBoolean(2)) {
                        LOGGER.info("Queued: worker = {}; job = {}", resultSet.getString(1), recordId);
                    } else {
                        LOGGER.info("Queued: worker = {}; job = {}; skipped - already on queue", resultSet.getString(1), recordId);
                    }
                }

            }
        } catch (SQLException ex) {
            LOGGER.error(LOG_DATABASE_ERROR, ex);
            throw new QueueException("Error queueing job", ex);
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
            stmt.setString(1, worker);
            stmt.setInt(2, wanted);
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
            stmt.setString(1, queueJob.getBibliographicRecordId());
            stmt.setInt(2, queueJob.getAgencyId());
            stmt.setString(3, queueJob.getWorker());
            stmt.setString(4, error);
            stmt.setTimestamp(5, queueJob.getQueued());
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
            throw new QueueException("Error rolling back", ex);
        }
        queueFail(queueJob, error);
    }


}
