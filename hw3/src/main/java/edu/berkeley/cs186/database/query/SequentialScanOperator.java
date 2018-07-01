package edu.berkeley.cs186.database.query;

import java.util.Iterator;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class SequentialScanOperator extends QueryOperator {
  private Database.Transaction transaction;
  private String tableName;

  /**
   * Creates a new SequentialScanOperator that provides an iterator on all tuples in a table.
   *
   * NOTE: Sequential scans don't take a source operator because they must always be at the bottom
   * of the DAG.
   *
   * @param transaction
   * @param tableName
   * @throws QueryPlanException
   * @throws DatabaseException
   */
  public SequentialScanOperator(Database.Transaction transaction,
                                String tableName) throws QueryPlanException, DatabaseException {
    super(OperatorType.SEQSCAN);
    this.transaction = transaction;
    this.tableName = tableName;
    this.setOutputSchema(this.computeSchema());
  }

  public String getTableName() {
    return this.tableName;
  }

  public Iterator<Record> iterator() throws DatabaseException {
    return this.transaction.getRecordIterator(tableName);
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }

  public String str() {
    return "type: " + this.getType() +
            "\ntable: " + this.tableName;
  }
}
