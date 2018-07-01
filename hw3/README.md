# Homework 3: Iterators and Join Algorithms
**Due 11:59 PM PST Thursday, March 15, 2018**

## Overview
In this assignment, you will implement iterators  and join algorithms over tables in Java. In this
document, we explain

- how to fetch the release code from GitHub,
- how to program in Java on the virtual machine, and
- what code you have to implement.

## Step 0: Fetching the Assignment
First, **boot your VM and open a terminal window**. Then run the following to
checkout the master branch.

```bash
git checkout master
```

If this command fails, you may be on another branch with uncommited changes.
Run `git branch` to see what branch you are on and `git status` to check for
uncommited changes. Once all changes are committed, run `git checkout master`.

Next, run the following to pull the homework from GitHub and change to a new
`hw3` branch:

```bash
git pull staff master
git checkout -b hw3
```

## Step 1: Getting Started with Java
Navigate to the `hw3` directory. In the
`src/main/java/edu/berkeley/cs186/database` directory, you will find all of the
Java 8 code we have provided to you. In the
`src/test/java/edu/berkeley/cs186/database` directory, you will find all of the
unit tests we have provided to you. To build and test the code with maven, run
the following in the `hw3` directory:

```bash
mvn clean compile # Compile the code.
mvn clean test    # Test the code. Not all tests will pass until you finish the assignment.
```

You are free to use any text editor or IDE to complete the project, but **we
will build and test your code on the VM with maven**. We recommend completing
the project with either Eclipse or IntelliJ, both of which come installed on
the VM:

```bash
eclipse # Launch eclipse.
idea.sh # Launch IntelliJ.
```

There are instructions online for how to import a maven project into
[Eclipse][eclipse_maven] and [IntelliJ][intellij_maven]. There are also
instructions online for how to debug Java in [Eclipse][eclipse_debugging] and
[IntelliJ][intellij_debugging]. When IntelliJ prompts you for an SDK, select
the one in `/home/vagrant/jdk1.8.0_131`. It bears repeating that even though
you are free to complete the project in Eclipse or IntelliJ, **we will build
and test your code on the VM with maven**.

## Step 2: Getting Familiar with the Release Code
Navigate to the `hw3/src/main/java/edu/berkeley/cs186/database` directory. You
will find six directories: `common`, `databox`, `io`, `table`, `index`, and `query`, and two files, `Database` and `DatabaseException`.
You do not have to deeply understand all of the code, but since all future
programming assignments will reuse this code, it's worth becoming a little
familiar with it. **In this assignment, though, you may only modify files in
the `query` and `table` directories**. See the Homework 2 specification for information on `databox`, `io`, and `index`.

### common
The `common` directory now contains an interface called a `BacktrackingIterator`. Iterators that implement this will be able to mark a point during iteration, and reset back to that mark. For example, here we have a back tracking iterator that just returns 1, 2, and 3, but can backtrack:

```java
BackTrackingIterator<Integer> iter = new BackTrackingIteratorImplementation();
iter.next(); //returns 1
iter.next(); //returns 2
iter.mark();
iter.next(); //returns 3
iter.hasNext(); //returns false
iter.reset();
iter.hasNext(); // returns true
iter.next(); //returns 2

```
`ArrayBacktrackingIterator` implements this interface. It takes in an array and returns a backtracking iterator over the values in that array.

### Table
The `table` directory now contains an implementation of
relational tables that store values of type `DataBox`. The `RecordId` class uniquely identifies a record on a page by its page number and entry number on that page. A `Record` is represented as a list of DataBoxes. A `Schema` is represented as list of column names and a list of column types. A `RecordIterator` takes in an iterator over `RecordId`s for a given table and returns an iterator over the corresponding records. A `Table` is made up of pages, with the first page always being the header page for the file. See the comments in `Table` for how the data of a table is serialized to a file.

### Database
The `Database` class represents a database. It is the interface through which we can create and update tables, and run queries on tables. When a user is operating on the database, they start a `transaction`, which allows for atomic access to tables in the database. You should be familiar with the code in here as it will be helpful when writing your own tests.

### Query
The `query` directory contains what are called query operators. These are operators that are applied to one or more tables, or other operators. They carry out their operation on their input operator(s) and return iterators over records that are the result of applying that specific operator. We call them “operators” here to distinguish them from the Java iterators you will be implementing.

`SortOperator` does the external merge sort algorithm covered in lecture. It contains a subclass called a `Run`. A `Run` is just an object that we can add records to, and read records from. Its underlying structure is a Table.

`JoinOperator` is the base class that join operators you will implement extend. It contains any methods you might need to deal with tables through the current running transaction. This means you should not deal directly with `Table` objects in the `Query` directory, but only through methods given through the current transaction.



## Step 3: Implementing Iterators and Join Algorithms


#### Notes Before You Begin
 In lecture, we sometimes use the words `block` and `page` interchangeably to describe a single unit of transfer from disc. The notion of a `block` when discussing join algorithms is different however. A `page` is a single unit of transfer from disc, and a  `block` is one or more `pages`. All uses of `block` in this project refer to this alternate definition.

 Besides when the comments tell you that you can do something in memory, everything else should be **streamed**. You should not hold more pages in memory at once than the given algorithm says you are allowed to.

  Remember the test cases we give you are not comprehensive, so you should write your own tests to further test your code and catch edge cases. Also, we give you all the tests for the current state of the database, but we skip some of them for time.

  The tests we provide to you for this HW are under `TestTable` for part 1, `TestJoinOperator` for parts 2 and 4, and `TestSortOperator` for part 3. If you are running tests from the terminal (and not an IDE), you can pass `-Dtest=TestName` to `mvn test` to only run a single file of tests.

#### 1. Table Iterators

In the `table` directory, fill in the classes `Table#RIDPageIterator` and `Table#RIDBlockIterator`. The tests in `TestTable` should pass once this is complete.

*Note on testing*: If you wish to write your own tests on `Table#RIDBlockIterator`, be careful with using the `Iterator<Page> block, int maxPages` constructor: you have to get a new `Iterator<Page>` if you want to recreate the iterator in the same test.

#### 2. Nested Loops Joins

Move to the `query` directory. You may first want to take a look at `SNLJOperator` (SNLJ: Simple Nested Loop Join).

##### 2.a Page Nested Loop Join
Complete `PNLJOperator.java`. Further instructions in source code.

##### 2.b Block Nested Loop Join
Complete `BNLJOperator.java`. Further instructions in source code. 


The PNLJ and BNLJ tests in `TestJoinOperator` should pass once this is complete.

#### 3: External Sort

Complete implementing `SortOperator.java`. The tests in `TestSortOperator` should pass once this is complete.

In the hidden tests, we may test the methods independently by replacing other methods with the staff solution, so make sure they each function exactly as described in the comments. This also allows for partial credit should one of your methods not work correctly.

#### 4: Sort Merge Join

Complete implementing `SortMergeOperator.java`. The sort phase of this join should use your previously implemented `SortOperator#sort` method. Note that we do not do the optimization discussed in lecture where the join happens during the last pass of sorting the two tables. We keep the sort phase completely separate from the join phase. The SortMerge tests in `TestJoinOperator` should pass once this is complete.

In the hidden tests, we may test `SortMergeOperator` independently of `SortOperator` by replacing your sort with the staff solution, so make sure it functions as described.

## Step 4: Submitting the Assignment
After you complete the assignment, simply commit and `git push` your `hw3`
branch. 60% of your grade will come from passing the unit tests we provide to
you. 40% of your grade will come from passing unit tests that we have not
provided to you. If your code does not compile on the VM with maven, we reserve
the right to give you a 0 on the assignment.

[eclipse_maven]: https://stackoverflow.com/a/36242422
[intellij_maven]: https://www.jetbrains.com/help/idea//2017.1/importing-project-from-maven-model.html
[eclipse_debugging]: http://www.vogella.com/tutorials/EclipseDebugging/article.html
[intellij_debugging]: https://www.jetbrains.com/help/idea/debugging.html
