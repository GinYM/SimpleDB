package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class BNLJOperator extends JoinOperator {

  protected int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }


  /**
   * BNLJ: Block Nested Loop Join
   *  See lecture slides.
   *
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might prove to be a useful reference).
   */
  private class BNLJIterator extends JoinIterator {
    /**
     * Some member variables are provided for guidance, but there are many possible solutions.
     * You should implement the solution that's best for you, using any member variables you need.
     * You're free to use these member variables, but you're not obligated to.
     */

    private Iterator<Page> leftIterator = null;
    private Iterator<Page> rightIterator = null;
    private BacktrackingIterator<Record> leftRecordIterator = null;
    private BacktrackingIterator<Record> rightRecordIterator = null;
    private Record leftRecord = null;
    private Record rightRecord = null;
    private Record nextRecord = null;
    private int maxPage = 8;

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      //throw new UnsupportedOperationException("hw3: TODO");
      this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
      this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
      //this.leftIterator.next();
      //this.rightIterator.next();
      //this.leftPageIterator = PNLJOperator.this.getPageIterator(this.getLeftTableName());
      //this.rightPageIterator = PNLJOperator.this.getPageIterator(this.getRightTableName());
      this.leftIterator.next();
      this.rightIterator.next();
      this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftIterator, maxPage);
      this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);

      this.nextRecord = null;

      this.leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
      this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;

      //this.count++;

      if (rightRecord != null) {
        leftRecordIterator.mark();
        rightRecordIterator.mark();
      }
      else return;

      try {
        fetchNextRecord();
      } catch (DatabaseException e) {
        this.nextRecord = null;
      }
    }

    private void fetchNextRecord() throws DatabaseException {
      if (this.leftRecord == null) throw new DatabaseException("No new record to fetch");
      this.nextRecord = null;
      do {
        if (this.rightRecord != null ) {
          DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
          DataBox rightJoinValue = this.rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
          if (leftJoinValue.equals(rightJoinValue)) {
            List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(this.rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
          }
          this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
          //if(this.rightRecordIterator != null){
          //this.count++;
          //System.out.println(this.count);
          //}
          //System.out.println(this.rightRecord);
        }else{
          updateRecod();
        }
      } while (!hasNext());
    }

    private void updateRecod() throws DatabaseException{
      if(leftRecordIterator.hasNext()==false && rightRecordIterator.hasNext() == false && leftIterator.hasNext() == false  && rightIterator.hasNext() == false){
        throw new DatabaseException("Finished");
      }

      if(leftRecordIterator.hasNext()){
        leftRecord = leftRecordIterator.next();
        rightRecordIterator.reset();
        assert(rightRecordIterator.hasNext());
        rightRecord = rightRecordIterator.next();
      }else if(rightIterator.hasNext()){
        //reset leftRecord
        leftRecordIterator.reset();
        assert(leftRecordIterator.hasNext());
        leftRecord = leftRecordIterator.next();

        //fetch new page
        //rightIterator.next();
        this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);
        rightRecord = rightRecordIterator.next();
        rightRecordIterator.mark();
      }else{
        //fetch new page left
        //leftIterator.next();
        this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftIterator, maxPage);
        leftRecord = leftRecordIterator.next();
        leftRecordIterator.mark();

        //reset to initial page for right
        rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
        rightIterator.next();
        this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), rightIterator, 1);
        rightRecord = rightRecordIterator.next();
        rightRecordIterator.mark();
      }
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      //throw new UnsupportedOperationException("hw3: TODO");
      return this.nextRecord != null;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      //throw new UnsupportedOperationException("hw3: TODO");
      if (!this.hasNext()) {
        throw new NoSuchElementException();
      }

      Record nextRecord = this.nextRecord;
      try {
        this.fetchNextRecord();
      } catch (DatabaseException e) {
        this.nextRecord = null;
      }
      return nextRecord;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
