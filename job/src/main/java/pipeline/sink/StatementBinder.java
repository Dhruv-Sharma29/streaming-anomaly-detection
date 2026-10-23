package pipeline.sink;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface StatementBinder<T> extends Serializable {
    void bind(PreparedStatement ps, T value) throws SQLException;
}
