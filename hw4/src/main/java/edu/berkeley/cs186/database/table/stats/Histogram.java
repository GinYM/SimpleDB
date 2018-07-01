package edu.berkeley.cs186.database.table.stats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.databox.TypeId;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * A histogram maintains approximate statistics about a (potentially large) set
 * of values without explicitly storing the values. For example, given the
 * following set of numbers:
 *
 *   4, 9, 10, 10, 10, 13, 15, 16, 18, 18, 22, 23, 25, 26, 24, 42, 42, 42
 *
 * We can build the following histogram:
 *
 *   10 |
 *    9 |         8
 *    8 |       +----+
 *    7 |       |    |
 *    6 |       |    |
 *    5 |       |    | 4
 *    4 |       |    +----+      3
 *    3 |    2  |    |    |    +----+
 *    2 |  +----+    |    | 1  |    |
 *    1 |  |    |    |    +----+    |
 *    0 |  |    |    |    |    |    |
 *       ------------------------------
 *        0    10    20  30    40    50
 *
 * A histogram is an ordered list of B "buckets", each of which defines a range (low, high).
 * For the first, B - 1 buckets, the low of the range is inclusive an the high of the
 * range is exclusive. For the last Bucket the high of the range is inclusive as well.
 * Each bucket counts the number of values that fall within its range. In this project,
 * you will work with a floating point histogram where low and high are defined by floats.
 * For any other data type, we will map it so it fits into a floating point histogram.
 *
 *
 * The primary data structure to consider is Bucket<Float> [] buckets, which is a list of Bucket
 * objects
 *
 * Bucket<Float> b = new Bucket(10.0, 100.0); //defines a bucket whose low value is 10 and high is 100
 * b.getStart(); //returns 10.0
 * b.getEnd(); //returns 100.0
 * b.increment(15);// adds the value 15 to the bucket
 * b.getCount();//returns the number of items added to the bucket
 * b.getDistinctCount();//returns the approximate number of distinct iterms added to the bucket
 *
 *
 */
public class Histogram {

  private Bucket<Float> [] buckets; //An array of float buckets the basic data structure

  private float minValue;
  private float maxValue;
  private float width;
  private int numBuckets;


  /*This constructor initialize a histogram object with a set number of buckets*/
  public Histogram(int numBuckets) {
    buckets = new Bucket[numBuckets];
    this.numBuckets = numBuckets;

  }


  /*This is a copy constructor that generates a new histogram from a bucket list*/
  private Histogram(Bucket<Float> buckets []) {
    this.buckets = buckets;
    this.numBuckets = buckets.length;
    this.minValue = buckets[0].getStart();
    this.width = buckets[0].getEnd() - buckets[0].getStart();
    this.maxValue = buckets[this.numBuckets-1].getEnd();
  }



  /** We only consider float histograms, and these two methods turn every data type into a float.
   *  We call this mapping quantization. That means given any DataBox, we turn it into a float number.
   *  For Booleans, Integers, Floats, order is preserved in the mapping. But for strings, only equalities
   *  are preserved.
   */
  private float quantization(Record record, int attribute){
        DataBox d = record.getValues().get(attribute);
        return quantization(d);
  }

  private float quantization(DataBox d){

        switch (d.type().getTypeId()) {
          case BOOL:   { return (d.getBool()) ? 1.0f : 0.0f; }

          case INT:    { return (float) d.getInt(); }

          case FLOAT:  { return d.getFloat(); }

          case STRING: { return (float) (d.getString().hashCode()); }
        }

        return 0f;
  }




  /** buildHistogram() takes a table and an attribute and builds a fixed width histogram, with
   *  the following procedure.
   *
   *  1. Take a pass through the full table, and store the min and the max "quantized" value.
   *  2. Calculate the width which is the (max - min)/#buckets
   *  3. Create empty bucket objects and place them in the array.
   *  4. Populate the buckets by incrementing
   *
   *  Edge cases: width = 0, put an item only in the last bucket.
   *              final bucket is inclusive on the last value.
   */
  public void buildHistogram(Table table, int attribute){

    // TODO: HW4 implement

    //1. first calculate the min and the max values

    //2. calculate the width of each bin

    //3. create each bucket object

    //4. populate the data using the increment(value) method

    throw new NotImplementedException();

  }

  private int bucketIndex(float v) {
    if (Math.abs(v - maxValue) < 0.00001) return buckets.length - 1;
    return (int) Math.floor((v - minValue) / width);
  }



  //Accessor Methods//////////////////////////////////////////////////////////////
  /** Return an estimate of the number of distinct values in the histogram. */
  public int getNumDistinct(){
    int sum = 0;
    for (int i=0; i<this.numBuckets; i++)
      sum += this.buckets[i].getDistinctCount();

    return sum;
  }

  /** Return an estimate of the number of the total values in the histogram. */
  public int getCount(){
    int sum = 0;
    for (int i=0; i<this.numBuckets; i++)
      sum += this.buckets[i].getCount();

    return sum;
  }

  /* Returns the bucket object at i */
  public Bucket<Float> get(int i){
    return buckets[i];
  }







  //Operations//////////////////////////////////////////////////////////////

  /* Given a predicate, return a multiplicative mask for the histogram. That is,
   * an array of size numBuckets where each entry is a float between 0 and 1 that represents a
   * scaling to update the histogram count. Suppose we have this histogram with 5 buckets:
   *
   *   10 |
   *    9 |         8
   *    8 |       +----+
   *    7 |       |    |
   *    6 |       |    |
   *    5 |       |    | 4
   *    4 |       |    +----+      3
   *    3 |    2  |    |    |    +----+
   *    2 |  +----+    |    | 1  |    |
   *    1 |  |    |    |    +----+    |
   *    0 |  |    |    |    |    |    |
   *       ------------------------------
   *        0    10    20  30    40    50
   *
   * Then we get a mask, [0, .25, .5, 0, 1], the resulting histogram is:
   *
   *   10 |
   *    9 |
   *    8 |
   *    7 |
   *    6 |
   *    5 |
   *    4 |                        3
   *    3 |          2    2      +----+
   *    2 |       +----+----+    |    |
   *    1 |       |    |    |    |    |
   *    0 |    0  |    |    |  0 |    |
   *       ------------------------------
   *        0    10    20  30    40    50
   *
   * Counts are always an integer and round to the nearest value.
   */
  public float[] filter(PredicateOperator predicate, DataBox value){

    float qvalue =  quantization(value);

      //do not handle non equality predicates on strings
      if (value.type().getTypeId() == TypeId.STRING &&
            ! (predicate == PredicateOperator.EQUALS ||
               predicate == PredicateOperator.NOT_EQUALS) ){

        return stringNonEquality(qvalue);

      } else if (predicate == PredicateOperator.EQUALS) {

        return allEquality(qvalue);

      }
      else if (predicate == PredicateOperator.NOT_EQUALS) {

        return allNotEquality(qvalue);

      }
      else if (predicate == PredicateOperator.GREATER_THAN) {

        return allGreaterThan(qvalue);

      }
      else if (predicate == PredicateOperator.LESS_THAN) {

        return allLessThan(qvalue);

      }
      else if (predicate == PredicateOperator.GREATER_THAN_EQUALS) {

        return allGreaterThanEquals(qvalue);

      }
      else {

        return allLessThanEquals(qvalue);

      }

  }


  /** Given, we don't handle non equality comparisons of strings. Return 1*/
  private float [] stringNonEquality(float qvalue){
    float [] result = new float[this.numBuckets];
    for (int i=0;i<this.numBuckets;i++)
      result[i] = 1.0f;
    return result;
  }


  /*Nothing fancy here take max of gt and equals*/
  private float [] allGreaterThanEquals(float qvalue){

    float [] result = new float[this.numBuckets];
    float [] resultGT = allGreaterThan(qvalue);
    float [] resultEquals = allEquality(qvalue);

    for (int i=0; i<this.numBuckets; i++)
      result[i] = Math.max(resultGT[i], resultEquals[i]);

    return result;
  }

  /*Nothing fancy here take max of lt and equals*/
  private float [] allLessThanEquals(float qvalue){

    float [] result = new float[this.numBuckets];
    float [] resultLT = allLessThan(qvalue);
    float [] resultEquals = allEquality(qvalue);

    for (int i=0; i<this.numBuckets; i++)
      result[i] = Math.max(resultLT[i], resultEquals[i]);

    return result;
  }





  //Operations To Implement//////////////////////////////////////////////////////////////

  /**
   *  Given a quantized value, scale the bucket that contains the value by 1/distinctCount,
   *  and set all other values to 0.
   */
  private float [] allEquality(float qvalue){
    float [] result = new float[this.numBuckets];

    // TODO: HW4 implement;

    throw new NotImplementedException();

    // return result;
  }


 /**
   *  Given a quantized value, scale the bucket that contains the value by 1-1/distinctCount,
   *  and set all other values to 1.
   */
  private float [] allNotEquality(float qvalue){
    float [] result = new float[this.numBuckets];


    // TODO: HW4 implement;

    throw new NotImplementedException();


    // return result;
  }


  /**
   *  Given a quantized value, scale the bucket that contains the value by (end - q)/width,
   *  and set all other buckets to 1 if higher and 0 if lower.
   */
  private float [] allGreaterThan(float qvalue){

    float [] result = new float[this.numBuckets];

    // TODO: HW4 implement;

    throw new NotImplementedException();


    // return result;
  }


 /**
   *  Given a quantized value, scale the bucket that contains the value by (q-start)/width,
   *  and set all other buckets to 1 if lower and 0 if higher.
   */
  private float [] allLessThan(float qvalue){

    float [] result = new float[this.numBuckets];

    // TODO: HW4 implement;

    throw new NotImplementedException();

    // return result;
  }







  // Cost Estimation ///////////////////////////////////////////////////////////////////

  /**
   * Return an estimate of the reduction factor for a given filter. For
   * example, consider again the example histogram from the top of the file.
   * The reduction factor for the predicate `>= 25` is 0.5 because roughly half
   * of the values are greater than or equal to 25.
   */
  public float computeReductionFactor(PredicateOperator predicate, DataBox value){

    float [] reduction = filter(predicate, value);

    float sum = 0.0f;
    int total = 0;

    for (int i=0; i< this.numBuckets; i++)
    {
      //non empty buckets
      sum += reduction[i]*this.buckets[i].getDistinctCount();
      total += this.buckets[i].getDistinctCount();
    }

    return sum/total;

  }

  /**
   * Given a histogram for a dataset, return a new histogram for the same
   * dataset with a filter applied. For example, if apply the filter `>= 20` to
   * the example histogram from the top of the file, we would get the following
   * histogram:
   *
   *    6 |
   *    5 |              4
   *    4 |            +----+      3
   *    3 |            |    |    +----+
   *    2 |            |    | 1  |    |
   *    1 |    0    0  |    +----+    |
   *    0 |  +----+----+    |    |    |
   *       ------------------------------
   *         0    1    2    3    4    5
   *               0    0    0    0    0
   */
  public Histogram copyWithPredicate(PredicateOperator predicate, DataBox value){

    float [] reduction = filter(predicate, value);
    Bucket<Float> [] newBuckets = this.buckets.clone();

    for (int i=0; i< this.numBuckets; i++){
      int newCount = (int) Math.round(reduction[i]*this.buckets[i].getCount());
      int newDistinctCount = (int) Math.round(reduction[i]*this.buckets[i].getDistinctCount());

      newBuckets[i].setCount(newCount);
      newBuckets[i].setDistinctCount(newCount);
    }

    return new Histogram(newBuckets);

  }


  //uniformly reduces the values across the board with the mean reduction assumes uncorrelated
  public Histogram copyWithReduction(float reduction){

    Bucket<Float> [] newBuckets = this.buckets.clone();

    for (int i=0; i< this.numBuckets; i++){
      int newCount = (int) Math.round(reduction*this.buckets[i].getCount());
      int newDistinctCount = (int) Math.round(reduction*this.buckets[i].getDistinctCount());

      newBuckets[i].setCount(newCount);
      newBuckets[i].setDistinctCount(newCount);
    }

    return new Histogram(newBuckets);

  }



}
