# Project 4: Cost Estimation and Query Optimization

## Logistics

**Due date: Friday 04/06/2018, 11:59:59 PM**

## Background
A query optimizer attempts to find the optimal execution plan for a SQL statement. The optimizer selects the plan with the lowest estimated cost among all considered candidate plans. The optimizer uses available statistics to estimate cost. Because the database has many internal statistics and tools at its disposal, the optimizer is usually in a better position than the user to determine the optimal method of statement execution.

For a specific query in a given environment, the cost computation accounts for metrics of query execution such as I/O. For example, consider a query that selects all employees who are managers. If the statistics indicate that 80% of employees are managers, then the optimizer may decide that a full table scan is most efficient. However, if statistics indicate that very few employees are managers and there is an index on that key, then reading an index followed by a table access by rowid may be more efficient than a full table scan.

In this project, you will design two pieces of a relational query optimizer: (1) the cost-estimator and (2) optimal query plan chooser. This project builds on top of the code and functionality that you should be familiar with from previous projects. We have released the staff solutions for HW 3, and you should integrate those into your codebase (see below).

## Getting Started
First, open up Virtual Box and power on the CS186 virtual machine. Once the machine is booted up, open a terminal and go to the `course-projects` folder you created in hw0.
```
$ cd course-projects
```
Make sure that you switch to the master branch:
```
$ git checkout master
```
It is good practice to run `git status` to make sure that you haven't inadvertently changed anything in the master branch.
Now, you want to add the reference to the staff repository so you call pull the new homework files:
```
$ git fetch staff master
$ git merge staff/master master
```
The `git merge` will give you a warning and a merge prompt if you have made any conflicting changes to master (not really possible with hw1!).

As with hw1, hw2, and hw3, make sure you create a new branch for your work:
```
git checkout -b hw4
```
Now, you should be ready to start the homework. *Don't forget to push to this branch when you are done with everything!*

## Background: Query Interface
Databases are represented by `Database` objects that can be created as follows:
```java
Database db = new Database('myDBFolder');
```
This creates a database where all of the tables will be stored in the `myDBFolder` directory on your filesystem.
The next relevant class is the `Schema` class, which defines table schemas.
`Schema` objects can be created by providing a list of field names and field types:
```
List<String> names = Arrays.asList("boolAttr", "intAttr",
                                   "stringAttr", "floatAttr");

List<Type> types = Arrays.asList(Type.boolType(), Type.intType(),
                                  Type.stringType(5), Type.floatType());

Schema s = new Schema(names, types);
```
Tables can be created as follows:
```
//creates a table with with schema s
db.createTable(s, "myTableName");

//creates a table with with schema s and builds an index on the intAttr field
db.createTableWithIndices(s, "myTableName",
                           Arrays.asList("intAttr"));
```
The `QueryPlan` interface allows you to generate SQL-like queries without having to parse actual SQL queries:
```java

/**
* SELECT * FROM myTableName WHERE stringAttr = 'CS 186'
*/

// create a new transaction
Database.Transaction transaction = db.beginTransaction();


// add a select to the QueryPlan
QueryPlan query = transaction.query("myTableName");
query.select("stringAttr", PredicateOperator.EQUALS, "CS 186");

// execute the query and get the output
Iterator<Record> queryOutput = query.executeOptimal();
```
To consider a more complicated example:
```java

/**
* SELECT *
* FROM Students as S, Enrollment as E
* WHERE E.sid = S.sid AND
*       E.cid = 'CS 186'
*/

// create a new transaction
Database.Transaction transaction = this.database.beginTransaction();

// alias both the Students and Enrollments tables
transaction.queryAs("Students", "S");
transaction.queryAs("Enrollments", "E");

// add a join and a select to the QueryPlan
QueryPlan query = transaction.query("S");
query.join("E", "S.sid", "E.sid");
query.select("E.cid", PredicateOperator.EQUALS, "CS 186");

// execute the query and get the output
Iterator<Record> queryOutput = query.executeOptimal();
```
`query.join` specifies an equality join. Its first argument is one of the two relations to be joined; the remaining two arguments are a key from the left table and the right table to join.

Note the `executeOptimal()` method above, this returns and executes optimal query plan. To see what this query plan is, you can print the query operator object:
```
// assuming query.executeOptimal() has already been called as above
QueryOperator finalOperator = query.getFinalOperator();
System.out.println(finalOperator.toString());



type: BNLJ
leftColumn: S.sid
rightColumn: E.sid
    (left)
    type: WHERE
    column: E.cid
    predicate: EQUALS
    value: CS 186
        type: SEQSCAN
        table: E

    (right)
    type: SEQSCAN
    table: S
```

In summary, if you would like to run queries on the database, you can create a new `QueryPlan` by calling `Transaction#query` and passing the name of the base table for the query. You can then call the `QueryPlan#select`, `QueryPlan#join`, etc. methods in order to generate as simple or as complex a query as you would like. Finally, call `QueryPlan#executeOptimal` to run the query optimizer,  execute the query, and get a response of the form `Iterator<Record>`. You can also use the `Transaction#queryAs` methods to alias tables.

## Your Assignment

Alright, now you are ready to write some code! **NOTE**: Throughout this project, you're free to add any helper methods you'd like to write. However, it is very important that you **do not change any of the interfaces that we've given you**.

It's also a good idea to always check the course repo for updates on the project.

### Part 1: Cost Estimation and Maintenance of Statistics
The first part of building the query optimizer is ensuring that each query operator has the appropriate IO cost estimates. In order to estimate IO costs for each query operator, you will need the table statistics for any input operators. This information is accessible from the `QueryOperator#getStats` method. The `TableStats` object returned represents estimated statistics of the operator's output, including information such as number of tuples and number of pages in the output among others. These statistics are generated whenever a `QueryOperator` is constructed.

**NOTE** that these statistics are meant to be approximate so please pay careful attention to how we define the quantities to track.

We will use histograms to track table statistics. A histogram maintains approximate statistics about a (potentially large) set of values without explicitly storing the values.
A histogram is an ordered list of B "buckets", each of which defines a range \[low, high). For the first, B - 1 buckets, the low of the range is inclusive and the high of the range is exclusive. **Exception**: For the last Bucket the high of the range is inclusive as well.  Each bucket counts the number of values and distinct values that fall within its range:
```java
Bucket<Float> b = new Bucket(10.0, 100.0); //defines a bucket whose low value is 10 and high is 100
b.getStart(); //returns 10.0
b.getEnd(); //returns 100.0
b.increment(15);// adds the value 15 to the bucket
b.getCount();//returns the number of items added to the bucket
b.getDistinctCount();//returns the approximate number of distinct iterms added to the bucket
```

In our implementation, the `Histogram` class, you will work with a floating point histogram where low and high are defined by floats. All other data types are backed by this floating point histogram through a "quantization" function `Histogram#quantization`. The histogram tracks statistics for all the values in a column of a table; we also need to support filtering the histogram based on given predicates.  After implementing all the methods below, you should be passing all of the tests in `TestHistogram`.

#### 1.1
Write the `Histogram#buildHistograms` method. Given a table and an attribute, this method initializes the buckets and sets them to an initial count based on the data in the table. You will need to use the appropriate `Bucket` methods to do this--see the comments inside the method.

#### 1.2
The `Histogram#filter` method is given a predicate and returns a multiplicative mask for the histogram. That is, an array of size `numBuckets` where each entry is a float between 0 and 1 that represents a scaling factor to use in updating the histogram count. `Histogram#filter` takes in a predicate operator (<,>,=,!=, >=, <=), and a `DataBox` value to compare and returns the mask. Write the `Histogram#allEquality` , `Histogram#allNotEquality`, `Histogram#allGreaterThan`, and `Histogram#allLessThan` methods. For all data types, these methods filter the histogram. They return a multiplicative mask for the specific predicate operator.


### Part 2 Query Optimization
Before you get started, you should take a look at the different provided constructors and methods in `QueryPlan`. This will give you an idea of the components of the query that you have to optimize. Pay close attention to the `QueryOperator` class, whose sub-classes implement the physical operators that actually answer the query.

To implement the single-table example in the previous part with a sequential scan:
```java
/**
* SELECT * FROM myTableName WHERE stringAttr = 'CS 186'
*/
QueryOperator source = SequentialScanOperator(transaction, myTableName);
QueryOperator select = SelectOperator(source, 'stringAttr', PredicateOperator.EQUALS, "CS 186");

select.iterator() //iterator over the results
```

To implement the join example in the previous part with a sequential scan and a block nested loop join:
```
/**
* SELECT *
* FROM Students as S, Enrollment as E
* WHERE E.sid = S.sid AND
*       E.cid = 'CS 186'
*/
QueryOperator s = SequentialScanOperator(transaction, 'Students');
QueryOperator e = SequentialScanOperator(transaction, 'Enrollment');

QueryOperator e186 = SelectOperator(e, 'cid', PredicateOperator.EQUALS, "CS 186");

BNLJOperator bjoin = BNLJOperator(s, e186, 'S.sid','E.sid', transaction);

bjoin.iterator() //iterator over the results
```
This defines a tree of `QueryOperator` objects, and `QueryPlan` finds such a tree to minimize I/O cost. Each `QueryOperator` has two relevant methods `estimateIOCost()` (which returns an estimated IO cost based on any stored statistics) and `iterator()` (which returns a iterator over the result tuples).

#### 2.1 System R Dynamic Programming
`QueryPlan#executeOptimal()` provides the scaffolding for the System R dynamic programming search algorithm. It constructs the optimal tree of operators and then returns the result iterator.

#### 2.1.1 Single Table Access Selection (Pass 1)
The first part of the search algorithm involves finding the lowest cost plans for accessing each individual table reference in the query. You will be implementing this functionality in `QueryPlan#minCostSingleAccess`. There are two possible scan operators you can use a `SequentialScanOperator` or `IndexScanOperator`. You should first calculate the estimated IO cost of performing a sequential scan. Then, if there are any eligible indices that can be used to scan the table, it should calculate the estimated IO cost of performing such an index scan. The `QueryPlan#getEligibleIndexColumns` method can be used to determine whether there are any existing indices that can be used for this query. This returns the set of columns on which there exists a index (independent of what query you are running). If the `IndexScanOperator` has a strictly lower cost, use the `IndexScanOperator` instead.

Then, as part of a heuristic-based optimization we covered in class, you should push down any selections that correspond to the table. You should be applying predicates as soon as they are eligible during bottom-up enumeration `QueryPlan#addEligibleSelections` which will be called by the `QueryPlan#minCostSingleAccess` method. See the comment in the code for details.

The end result of this method should be a query operator that starts with either a `SequentialScanOperator` or `IndexScanOperator` followed by zero or more `SelectOperator`'s.
After implementing all the methods up to this point, you should be passing all of the tests in `TestSingleAccess`. These tests do not involve any joins.

Returning our attention to `QueryPlan#executeOptimal()`, the result is put into a map structure, which maps each table name to its lowest cost operator. Each table is represented as a singleton set, which will be the input for the next stage of the algorithm.

#### 2.1.2 Join Algorithms (Pass i > 1)
The second part of the search algorithm involves finding the lowest cost join between each set of tables formed in the previous pass and a separate single table. You will be implementing this functionality in `QueryPlan#minCostJoins`. This method takes a map in a map of left-deep plans on $i$ relations and should produce a map of left-deep plans on $i+1$ relations. All subsets of $i+1$ should have an entry in the map be included unless they involved cartesian products. Use the list of explicit join conditions added through the `QueryPlan#join` method to identify potential joins. The end result of this method should be a mapping from a set of tables to a join query operator that corresponds to the lowest cost join estimated.

#### 2.1.3 Optimal Plan Selection

The next part is to write the outermost driver method of the optimizer, `QueryPlan#executeOptimal`. This method should invoke the various passes of the Selinger dynamic programming algorithm, and in the end return the optimal plan for the full set of tables. You first have to find the optimal single table access plan for all the individual tables that you want to join, and then recursively use `QueryPlan#minCostJoins` to find the best joins between tables until all tables have been joined together. Finally, you have to add the remaining groupBy and project operators that are part of the query but have not been added to the query plan so far.  After implementing all the methods up to this point, you should be passing all of the tests in `TestOptimizationJoins` and `TestBasicQuery`.

#### 2.1.4 Interesting Orders

Something we have ignored up to now in our implementation of the System R Query Optimizer is the inclusion of table plans that have interesting orders. We consider a join order interesting if the resulting table is sorted on a key that is used in a later join, `GROUP BY` or `ORDER BY`. Thus, your final task is to implement the function in `QueryPlan#findInterestingOrders`. This method takes in the map of optimal single access plans for each table produced by Pass 1 of the System R algorithm and returns a mapping of table names to a list of tables that result in an interesting order when joined with the aforementioned table.


### Submitting the Assignment

After you complete the assignment, simply commit and git push your hw4 branch. 60% of your grade will come from passing the unit tests we provide to you. 40% of your grade will come from passing unit tests that we have not provided to you. If your code does not compile on the VM with maven, we reserve the right to give you a 0 on the assignment.
