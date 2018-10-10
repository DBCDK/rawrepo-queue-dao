package dk.dbc.rawrepo.queue;

import dk.dbc.commons.testutils.postgres.connection.PostgresITConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RawRepoQueueDAOIT {

    private Connection connection;
    private PostgresITConnection postgres;

    @Before
    public void setup() throws SQLException {
        postgres = new PostgresITConnection("rawrepo");
        connection = postgres.getConnection();
        // Don't do this unless you know what you are doing. You need to be superuser in the database
        // before you can do it. Only effect seems to be that sql statements are written to the pg logfile.
        // connection.prepareStatement("SET log_statement = 'all';").execute();
        resetDatabase();
    }

    @After
    public void teardown() throws SQLException {
        postgres.close();
    }

    private void resetDatabase() throws SQLException {
        postgres.clearTables("relations", "records", "records_archive", "queue", "queuerules", "queueworkers", "jobdiag");

        PreparedStatement stmt = connection.prepareStatement("INSERT INTO queueworkers(worker) VALUES(?)");
        stmt.setString(1, "changed");
        stmt.execute();
        stmt.setString(1, "leaf");
        stmt.execute();
        stmt.setString(1, "node");
        stmt.execute();

        stmt = connection.prepareStatement("INSERT INTO queuerules(provider, worker, changed, leaf) VALUES('test', ?, ?, ?)");
        stmt.setString(1, "changed");
        stmt.setString(2, "Y");
        stmt.setString(3, "A");
        stmt.execute();
        stmt.setString(1, "leaf");
        stmt.setString(2, "A");
        stmt.setString(3, "Y");
        stmt.execute();
        stmt.setString(1, "node");
        stmt.setString(2, "A");
        stmt.setString(3, "N");
        stmt.execute();
    }

    @Test
    public void testGetConfiguration() throws Exception {
        postgres.clearTables("configurations");

        PreparedStatement stmt = connection.prepareStatement("INSERT INTO configurations (key, value) VALUES(?, ?)");
        stmt.setString(1, "RAWREPO_RECORD_SERVICE_URL");
        stmt.setString(2, "http://RAWREPO_RECORD_SERVICE_URL:42");
        stmt.execute();

        RawRepoQueueDAO dao = RawRepoQueueDAO.builder(connection).build();

        HashMap<String, String> expected = new HashMap<>();
        expected.put("RAWREPO_RECORD_SERVICE_URL", "http://RAWREPO_RECORD_SERVICE_URL:42");

        assertEquals(expected, dao.getConfiguration());
    }

    /**
     * Raise an (descriptive) exception if a collection of strings doesn't match
     * supplied list
     *
     * @param col   collection
     * @param elems string elements collection should consist of
     */
    private static void collectionIs(Collection<String> col, String... elems) {
        HashSet<String> missing = new HashSet<>();
        Collections.addAll(missing, elems);
        HashSet<String> extra = new HashSet<>(col);
        extra.removeAll(missing);
        missing.removeAll(col);
        if (!extra.isEmpty() || !missing.isEmpty()) {
            throw new RuntimeException("missing:" + missing.toString() + ", extra=" + extra.toString());
        }
    }

    private Collection<String> getQueueState() throws SQLException {
        Set<String> result = new HashSet<>();
        PreparedStatement stmt = connection.prepareStatement("SELECT bibliographicrecordid, agencyid, worker, COUNT(queued) FROM QUEUE GROUP BY bibliographicrecordid, agencyid, worker");
        if (stmt.execute()) {
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                result.add(resultSet.getString(1) + ":" + resultSet.getInt(2) + ":" + resultSet.getString(3) + ":" + resultSet.getInt(4));
            }
        }
        return result;
    }

    @Test
    public void testDequeue() throws SQLException, QueueException {
        RawRepoQueueDAO dao = RawRepoQueueDAO.builder(connection).build();
        connection.setAutoCommit(false);

        dao.enqueue("A", 1, "test", true, true);
        connection.commit();
        collectionIs(getQueueState(),
                "A:1:changed:1", "A:1:leaf:1");

        QueueItem job = dao.dequeue("changed");
        assertNotNull(job);
    }

    @Test
    public void testDequeuePriority() throws SQLException, QueueException {
        RawRepoQueueDAO dao = RawRepoQueueDAO.builder(connection).build();
        connection.setAutoCommit(false);

        // Lower number = faster dequeuing
        // No priority defaults to 1000
        dao.enqueue("RECORD_0", 870970, "test", true, true);
        dao.enqueue("RECORD_1", 870970, "test", true, true, 1000);
        dao.enqueue("RECORD_2", 870970, "test", true, true, 10);
        dao.enqueue("RECORD_3", 870970, "test", true, true, 1000);
        dao.enqueue("RECORD_4", 870970, "test", true, true, 5);
        dao.enqueue("RECORD_5", 870970, "test", true, true, 1000);
        dao.enqueue("RECORD_6", 870970, "test", true, true);
        dao.enqueue("RECORD_7", 870970, "test", true, true, 1000);
        dao.enqueue("RECORD_8", 870970, "test", true, true, 10);
        dao.enqueue("RECORD_9", 870970, "test", true, true, 42);

        connection.commit();

        collectionIs(getQueueState(),
                "RECORD_0:870970:changed:1", "RECORD_0:870970:leaf:1",
                "RECORD_1:870970:changed:1", "RECORD_1:870970:leaf:1",
                "RECORD_2:870970:changed:1", "RECORD_2:870970:leaf:1",
                "RECORD_3:870970:changed:1", "RECORD_3:870970:leaf:1",
                "RECORD_4:870970:changed:1", "RECORD_4:870970:leaf:1",
                "RECORD_5:870970:changed:1", "RECORD_5:870970:leaf:1",
                "RECORD_6:870970:changed:1", "RECORD_6:870970:leaf:1",
                "RECORD_7:870970:changed:1", "RECORD_7:870970:leaf:1",
                "RECORD_8:870970:changed:1", "RECORD_8:870970:leaf:1",
                "RECORD_9:870970:changed:1", "RECORD_9:870970:leaf:1");

        assertEquals("RECORD_4", dao.dequeue("changed").getBibliographicRecordId());
        assertEquals("RECORD_2", dao.dequeue("changed").getBibliographicRecordId());
        assertEquals("RECORD_8", dao.dequeue("changed").getBibliographicRecordId());
        assertEquals("RECORD_9", dao.dequeue("changed").getBibliographicRecordId());
        assertEquals("RECORD_0", dao.dequeue("changed").getBibliographicRecordId());
        assertEquals("RECORD_1", dao.dequeue("changed").getBibliographicRecordId());
        assertEquals("RECORD_3", dao.dequeue("changed").getBibliographicRecordId());
        assertEquals("RECORD_5", dao.dequeue("changed").getBibliographicRecordId());
        assertEquals("RECORD_6", dao.dequeue("changed").getBibliographicRecordId());
        assertEquals("RECORD_7", dao.dequeue("changed").getBibliographicRecordId());
    }

    @Test
    public void testQueueFail() throws SQLException, QueueException {
        RawRepoQueueDAO dao = RawRepoQueueDAO.builder(connection).build();
        connection.setAutoCommit(false);

        try (PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) FROM jobdiag")) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (!resultSet.next() || resultSet.getInt(1) != 0) {
                    Assert.fail("jobdiag is not empty before test");
                }
            }
        }

        QueueItem queueJob = new QueueItem("abcdefgh", 123456, "node", new Timestamp(0), 1000);
        dao.queueFail(queueJob, "What!");

        try (PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) FROM jobdiag")) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (!resultSet.next() || resultSet.getInt(1) != 1) {
                    Assert.fail("jobdiag is not set after test");
                }
            }
        }
    }

    @Test
    public void testDequeueBulk() throws SQLException, QueueException {
        RawRepoQueueDAO dao = RawRepoQueueDAO.builder(connection).build();
        connection.setAutoCommit(false);
        for (int i = 0; i < 10; i++) {
            dao.enqueue("rec" + i, 123456, "test", false, false);
        }
        connection.commit();
        connection.setAutoCommit(false);

        collectionIs(getQueueState(),
                "rec0:123456:node:1",
                "rec1:123456:node:1",
                "rec2:123456:node:1",
                "rec3:123456:node:1",
                "rec4:123456:node:1",
                "rec5:123456:node:1",
                "rec6:123456:node:1",
                "rec7:123456:node:1",
                "rec8:123456:node:1",
                "rec9:123456:node:1");

        dao.dequeue("node", 4);

        collectionIs(getQueueState(),
                "rec4:123456:node:1",
                "rec5:123456:node:1",
                "rec6:123456:node:1",
                "rec7:123456:node:1",
                "rec8:123456:node:1",
                "rec9:123456:node:1");
        connection.rollback();
        collectionIs(getQueueState(),
                "rec0:123456:node:1",
                "rec1:123456:node:1",
                "rec2:123456:node:1",
                "rec3:123456:node:1",
                "rec4:123456:node:1",
                "rec5:123456:node:1",
                "rec6:123456:node:1",
                "rec7:123456:node:1",
                "rec8:123456:node:1",
                "rec9:123456:node:1");
        connection.setAutoCommit(false);

        dao.dequeue("node", 4);

        collectionIs(getQueueState(),
                "rec4:123456:node:1",
                "rec5:123456:node:1",
                "rec6:123456:node:1",
                "rec7:123456:node:1",
                "rec8:123456:node:1",
                "rec9:123456:node:1");
        connection.commit();
        collectionIs(getQueueState(),
                "rec4:123456:node:1",
                "rec5:123456:node:1",
                "rec6:123456:node:1",
                "rec7:123456:node:1",
                "rec8:123456:node:1",
                "rec9:123456:node:1");
    }
}
