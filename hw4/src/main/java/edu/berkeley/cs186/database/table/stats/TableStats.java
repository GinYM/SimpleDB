package edu.berkeley.cs186.database.table.stats;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;

/**
 * Every table in a database maintains a set of table statistics which are
 * updated whenever a tuple is added or deleted to it. These table statistics
 * consist of an estimated number of records in the table, an estimated number
 * of pages used by the table, and a histogram on every column of the table.
 * For example, we can construct a TableStats and add/remove records from
 * it like this:
 *
 *   // Create a TableStats object for a table with columns (x: int, y: float).
 *   List<String> fieldNames = Arrays.asList("x", "y");
 *   List<Type> fieldTypes = Arrays.asList(Type.intType(), Type.floatType());
 *   Schema schema = new Schema(fieldNames, fieldTypes);
 *   TableStats stats = new TableStats(schema);
 *
 *   // Add and remove tuples from the stats.
 *   IntDataBox x1 = new IntDataBox(1);
 *   FloatDataBox y1 = new FloatDataBox(1);
 *   Record r1 = new Record(schema, Arrays.asList(x1, y1));
 *
 *   IntDataBox x2 = new IntDataBox(1);
 *   FloatDataBox y2 = new FloatDataBox(1);
 *   Record r2 = new Record(schema, Arrays.asList(x2, y2));
 *
 *   stats.addRecord(r1);
 *   stats.addRecord(r2);
 *   stats.removeRecord(r1);
 *
 * Later, we can use the statistics maintained by a TableStats object for
 * things like query optimization:
 *
 *   stats.getNumRecords(); // Estimated number of records.
 *   stats.getNumPages();   // Estimated number of pages.
 *   stats.getHistograms(); // Histograms on each column.
 */
public class TableStats {
  private Schema tableSchema;
  private int numRecords;
  private List<Histogram> histograms;

  /** Construct a TableStats for an empty table with schema `tableSchema`. */
  public TableStats(Schema tableSchema) {

    this.tableSchema = tableSchema;
    this.numRecords = 0;
    this.histograms = new ArrayList<Histogram>();

  }

  private TableStats(Schema tableSchema, int numRecords, List<Histogram> histograms) {
    this.tableSchema = tableSchema;
    this.numRecords = numRecords;
    this.histograms = histograms;
  }

  // Modifiers /////////////////////////////////////////////////////////////////
  public void addRecord(Record record) {
    numRecords++;

  }

  public void refreshHistograms(int buckets, Table tab) {

    int count = 0;
    int totalRecords = 0;
    for (Type t : tableSchema.getFieldTypes()) {

      Histogram h = new Histogram(buckets);
      h.buildHistogram(tab, count);
      this.histograms.add(h);
      totalRecords += h.getCount();
      count++;
    }

    this.numRecords = (int) Math.round(((float)totalRecords)/count);

  }

  public void removeRecord(Record record) {
    numRecords = Math.max(numRecords - 1, 0);
  }

  // Accessors /////////////////////////////////////////////////////////////////
  public Schema getSchema() {
    return tableSchema;
  }

  public int getNumRecords() {
    return numRecords;
  }

  /**
   * Calculates the number of data pages required to store `numRecords` records
   * assuming that all records are stored as densely as possible in the pages.
   */
  public int getNumPages() {
    int numRecordsPerPage = Table.computeNumRecordsPerPage(Page.pageSize, tableSchema);
    if (numRecords % numRecordsPerPage == 0) {
      return numRecords / numRecordsPerPage;
    } else {
      return (numRecords / numRecordsPerPage) + 1;
    }
  }

  public List<Histogram> getHistograms() {
    return histograms;
  }


  // Copiers ///////////////////////////////////////////////////////////////////
  /**
   * Estimates the table statistics for the table that would be produced after
   * filtering column `i` with `predicate` and `value`. For simplicity, we
   * assume that columns are completeley uncorrelated. For example, imagine the
   * following table statistics for a table T(x:int, y:int).
   *
   *   numRecords = 100
   *   numPages = 2
   *               Histogram x                         Histogram y
   *               ===========                         ===========
   *   60 |                       50       60 |
   *   50 |        40           +----+     50 |
   *   40 |      +----+         |    |     40 |
   *   30 |      |    |         |    |     30 |   20   20   20   20   20
   *   20 |   10 |    |         |    |     20 | +----+----+----+----+----+
   *   10 | +----+    | 00   00 |    |     10 | |    |    |    |    |    |
   *   00 | |    |    +----+----+    |     00 | |    |    |    |    |    |
   *       ----------------------------        ----------------------------
   *        0    1    2    3    4    5          0    1    2    3    4    5
   *              0    0    0    0    0               0    0    0    0    0
   *
   * If we apply the filter `x < 20`, we estimate that we would have the
   * following table statistics.
   *
   *   numRecords = 50
   *   numPages = 1
   *               Histogram x                         Histogram y
   *               ===========                         ===========
   *   50 |        40                      50 |
   *   40 |      +----+                    40 |
   *   30 |      |    |                    30 |
   *   20 |   10 |    |                    20 |   10   10   10   10   10
   *   10 | +----+    | 00   00   50       10 | +----+----+----+----+----+
   *   00 | |    |    +----+----+----+     00 | |    |    |    |    |    |
   *       ----------------------------        ----------------------------
   *        0    1    2    3    4    5          0    1    2    3    4    5
   *              0    0    0    0    0               0    0    0    0    0
   */
  public TableStats copyWithPredicate(int column,
                                      PredicateOperator predicate,
                                      DataBox d) {
    float reductionFactor = histograms.get(column).computeReductionFactor(predicate, d);
    List<Histogram> copyHistograms = new ArrayList<>();
    for (int j = 0; j < histograms.size(); ++j) {
      Histogram histogram = histograms.get(j);
      if (column == j) {
        copyHistograms.add(histogram.copyWithPredicate(predicate, d));
      } else {
        copyHistograms.add(histogram.copyWithReduction(reductionFactor));
      }
    }

    Histogram qhistogram = histograms.get(column);
    int numRecords = qhistogram.getCount();
    return new TableStats(this.tableSchema, numRecords, copyHistograms);
  }

  /**
   * Creates a new TableStats which is the statistics for the table
   * that results from this TableStats joined with the given TableStats.
   *
   * TODO(mwhittaker): Not sure what this code is doing. Figure it out and
   * document it. Also clean up the code.
   *
   * @param leftIndex the index of the join column for this
   * @param rightStats the TableStats of the right table to be joined
   * @param leftIndex the index of the join column for the right table
   * @return new TableStats based off of this and params
   */
  public TableStats copyWithJoin(int leftIndex,
                                 TableStats rightStats,
                                 int rightIndex) {
    // Compute the new schema.
    List<String> joinedFieldNames = new ArrayList<>();
    joinedFieldNames.addAll(tableSchema.getFieldNames());
    joinedFieldNames.addAll(rightStats.tableSchema.getFieldNames());

    List<Type> joinedFieldTypes = new ArrayList<>();
    joinedFieldTypes.addAll(tableSchema.getFieldTypes());
    joinedFieldTypes.addAll(rightStats.tableSchema.getFieldTypes());

    Schema joinedSchema = new Schema(joinedFieldNames, joinedFieldTypes);

    //System.out.println(this.numRecords  + " " +rightStats.getNumRecords());

    int inputSize = this.numRecords * rightStats.getNumRecords();

    int leftNumDistinct;
    if (this.histograms.size() > 0)
      leftNumDistinct = this.histograms.get(leftIndex).getNumDistinct() + 1;
    else
      leftNumDistinct = 1;

    int rightNumDistinct;
    if (rightStats.histograms.size() > 0)
      rightNumDistinct = rightStats.histograms.get(rightIndex).getNumDistinct() + 1;
    else
      rightNumDistinct = 1;

    float reductionFactor = 1.0f / Math.max(leftNumDistinct, rightNumDistinct);

    List<Histogram> copyHistograms = new ArrayList<Histogram>();

    int leftNumRecords = this.numRecords;
    int rightNumRecords = rightStats.getNumRecords();

    //todo fix
    float leftReductionFactor = leftNumDistinct/ Math.max(leftNumDistinct, rightNumDistinct);
    float rightReductionFactor = rightNumDistinct/ Math.max(leftNumDistinct, rightNumDistinct);

    float joinReductionFactor = leftReductionFactor;

//    Histogram joinHistogram = this.histograms.get(leftIndex);

    for (int i = 0; i < this.histograms.size(); i++) {
      Histogram leftHistogram = this.histograms.get(i);
      copyHistograms.add(leftHistogram.copyWithReduction(leftReductionFactor));
    }

    for (int i = 0; i < rightStats.histograms.size(); i++) {
      Histogram rightHistogram = rightStats.histograms.get(i);
      copyHistograms.add(rightHistogram.copyWithReduction(rightReductionFactor));
    }

    int outputSize = (int)(reductionFactor*inputSize);

    return new TableStats(joinedSchema, outputSize, copyHistograms);
  }
}
