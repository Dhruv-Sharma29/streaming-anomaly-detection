package pipeline.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * At-least-once JDBC sink for idempotent statements (upserts / insert-or-ignore).
 *
 * Writes are buffered and flushed before every checkpoint barrier (SinkWriter.flush), so every
 * record that precedes a completed checkpoint is in the database. After a failure, Flink replays
 * from the last checkpoint and some records are written again; because every statement is an
 * idempotent upsert of a deterministic value, the replay leaves the same final rows. Any SQL
 * error fails the task and triggers that same replay path. Chosen over the JDBC connector to keep
 * the guarantee explicit and avoid its 3.x to 4.x API changes; the cost is owning ~80 lines.
 */
public class UpsertSink<T> implements Sink<T> {
    private final String jdbcUrl;
    private final String sql;
    private final StatementBinder<T> binder;
    private final int batchSize;
    private final long flushIntervalMs;  // 0: flush only on full batch and before checkpoints

    public UpsertSink(String jdbcUrl, String sql, StatementBinder<T> binder, int batchSize) {
        this(jdbcUrl, sql, binder, batchSize, 0);
    }

    /**
     * flushIntervalMs > 0 also flushes when a write arrives that long after the last flush. That
     * trades more, smaller database round trips for lower result latency. Correctness is
     * unchanged: the flush before each checkpoint still happens.
     */
    public UpsertSink(String jdbcUrl, String sql, StatementBinder<T> binder, int batchSize, long flushIntervalMs) {
        this.jdbcUrl = jdbcUrl;
        this.sql = sql;
        this.binder = binder;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        return new Writer();
    }

    private class Writer implements SinkWriter<T> {
        private Connection conn;
        private PreparedStatement ps;
        private int pending;
        private long lastFlush = System.currentTimeMillis();

        private PreparedStatement statement() throws SQLException {
            if (conn == null || conn.isClosed()) {
                conn = DriverManager.getConnection(jdbcUrl);
                conn.setAutoCommit(true);
                ps = conn.prepareStatement(sql);
            }
            return ps;
        }

        @Override
        public void write(T element, Context context) throws IOException {
            try {
                binder.bind(statement(), element);
                ps.addBatch();
                if (++pending >= batchSize
                        || (flushIntervalMs > 0 && System.currentTimeMillis() - lastFlush >= flushIntervalMs)) executeBatch();
            } catch (SQLException e) {
                throw new IOException("sink write failed", e);
            }
        }

        private void executeBatch() throws SQLException {
            if (pending > 0) {
                ps.executeBatch();
                pending = 0;
            }
            lastFlush = System.currentTimeMillis();
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            try {
                executeBatch();
            } catch (SQLException e) {
                throw new IOException("sink flush failed", e);
            }
        }

        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
        }
    }
}
