# Homework 2: B+ Trees
**Due 11:59 PM Thursday, February 22**

## Overview
In this assignment, you will implement persistent B+ trees in Java. In this
document, we explain

- how to fetch the release code from GitHub,
- how to program in Java on the virtual machine, and
- what code you have to implement.

## Step 0: Fetching the Assignment and Setup
First, **boot your VM and open a terminal window**. Then run the following to
checkout the master branch.

```bash
git checkout master
```

If this command fails, you may be on another branch with uncommited changes.
Run `git branch` to see what branch you are on and `git status` to check for
uncommited changes. Once all changes are committed, run `git checkout master`.

Next, run the following to pull the homework from GitHub and change to a new
`hw2` branch:

```bash
git pull staff master
git checkout -b hw2
```

Next, fetch the graphviz package:
```bash
sudo apt-get install -y graphviz
```

## Step 1: Getting Started with Java
Navigate to the `hw2` directory. In the
`src/main/java/edu/berkeley/cs186/database` directory, you will find all of the
Java 8 code we have provided to you. In the
`src/test/java/edu/berkeley/cs186/database` directory, you will find all of the
unit tests we have provided to you. To build and test the code with maven, run
the following in the `hw2` directory:

```bash
mvn clean compile # Compile the code.
mvn clean test    # Test the code. Not all tests will pass until you finish the
                  # assignment.
```

You are free to use any text editor or IDE to complete the project, but **we
will build and test your code on the VM with maven**. We **highly highly recommend** completing
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
Navigate to the `hw2/src/main/java/edu/berkeley/cs186/database` directory. You
will find five directories: `common`, `databox`, `io`, `table`, and `index`.
You do not have to deeply understand all of the code, but since all future
programming assignments will reuse this code, it's worth becoming a little
familiar with it. **In this assignment, though, you may only modify files in
the `index` directory**.

### common
The `common` directory contains miscellaneous and generally useful bits of code
that are not particular to this assignment.

### databox
Like most DBMSs, the system we are working on in this assignment has its own
type system, which is distinct from the type system of the programming language
used to implement the DBMS. (Our DBMS doesn't quite provide SQL types either,
though it's modeled on a simplified version of SQL types). In this homework,
we'll need to write Java code to create and manipulate the DBMS types and any
data we store.

The `databox` directory contains the classes which represent the values
stored in a database, as well as their types. Specifically, the `DataBox` class
represents values and the `Type` class represents types. Here's an example:

```java
DataBox x = new IntDataBox(42); // The integer value '42'.
Type t = Type.intType();        // The type 'int'.
Type xsType = x.type();         // Get x's type: Type.intType()
int y = x.getInt();             // Get x's value: 42
String s = x.getString();       // An exception is thrown.
```

### io
The `io` directory contains code that allows you to allocate, read, and write
pages to and from a file. All modifications to the pages of the file are
persisted to the file. The two main classes of this directory are
`PageAllocator` which can be used to allocate pages in a file, and `Page` which
represents pages in the file. Here's an example of how to persist data into a
file using a `PageAllocator`:

```java
// Create a page allocator which stores data in the file "foo.data". Setting
// wipe to true clears out any data that may have previously been in the file.
bool wipe = true;
PageAllocator allocator = new PageAllocator("foo.data", wipe);

// Allocate a page in the file. All pages are assigned a unique page number
// which can be used to fetch the page.
int pageNum = allocator.allocPage(); // The page number of the allocated page.
Page page = allocator.fetchPage(pageNum); // The page we just allocated.
System.out.println(pageNum); // 0. Page numbers are assigned 0, 1, 2, ...

// Write data into the page. All data written to the page is persisted in the
// file automatically.
ByteBuffer buf = page.getByteBuffer();
buf.putInt(42);
buf.putInt(9001);
```

And here's an example of how to read data that's been persisted to a file:

```java
// Create a page allocator which stores data in the file "foo.data". Setting
// wipe to false means that this page allocator can read any data that was
// previously stored in "foo.data".
bool wipe = false;
PageAllocator allocator = new PageAllocator("foo.data", wipe);

// Fetch the page we previously allocated.
Page page = allocator.fetchPage(0);

// Read the data we previously wrote.
ByteBuffer buf = page.getByteBuffer();
int x = buf.getInt(); // 42
int y = buf.getInt(); // 9001
```

### table
In future assignments, the `table` directory will contain an implementation of
relational tables that store values of type `DataBox`. For now, it only
contains a `RecordId` class which uniquely identifies a record on a page by its
page number and entry number.

```java
// The jth record on the ith page.
RecordId rid = new RecordId(i, (short) j);
```

### index
We describe the `index` directory in the next section.

## Step 3: Implementing B+ Trees
The `index` directory contains an partial implementation of a B+ tree
(`BPlusTree`), an implementation that you will complete in this assignment.
Every B+ tree maps keys of type `DataBox` to values of type `RecordId`. A B+
tree is composed of inner nodes (`InnerNode`) and leaf nodes (`LeafNode`).
Every B+ tree is persisted to a file, and every inner node and leaf node is
stored on its own page.

In this assignment, do the following:

1. Read through all of the code in the `index` directory. Many comments contain
   critical information on how you must implement certain functions. For
   example, `BPlusNode::put` specifies how to redistribute entries after a
   split. You are responsible for reading these comments. If you do not obey
   the comments, you will lose points. Here are a few of the most notable
   points:
    - Our implementation of B+ trees does not support duplicate keys. You will
      throw an exception whenever a duplicate key is inserted.
    - Our implementation of B+ trees assumes that inner nodes and leaf nodes
      can be serialized on a single page. You do not have to support nodes that
      span multiple pages.
    - Our implementation of delete does not rebalance the tree. Thus, the
      invariant that all non-root leaf nodes in a B+ tree of order `d` contain
      between `d` and `2d` entries is broken. Note that actual B+ trees **do rebalance**
      after deletion, but we will **not** be implementing rebalancing trees in this project
      for the sake of simplicity.
2. Implement the `LeafNode::fromBytes` function that reads a `LeafNode` from a
   page. For information on how a leaf node is serialized, see
   `LeafNode::toBytes`. For an example on how to read a node from disk, see
   `InnerNode::fromBytes`.
3. Implement the `get`, `getLeftmostLeaf`, `put`, `remove`, and `bulkLoad` methods of
   `InnerNode` and `LeafNode`. For information on what these methods do, refer
   to the comments in `BPlusNode`. Don't forget to call `sync` when
   implementing `put`, `remove`, and `bulkLoad`; it's easy to forget.
4. Implement the `get`, `scanAll`, `scanGreaterEqual`, `put`, `remove`, and `bulkLoad`
   methods of `BPlusTree`. In order to implement `scanAll` and
   `scanGreaterEqual`, you will have to complete the `BPlusTreeIterator` class.

After this, you should pass all the tests we have provided to you (and any you
add yourselves).

Note that you may not modify the signature of any methods or classes that we
provide to you (except `BPlusTreeIterator`), but you're free to add helper
methods. Also, you may only modify code in the `index` directory.

### Debugging
As stated earlier, we highly recommend using a Java IDE for this project because it will require a
lot of debugging. In office hours, we will **not** help you until you have tried using
the debugger to solve your issue.

Debugging large B+ trees is hard. To make it a bit easier, we also gave you the `BPlusTree::toDotPDFFile` method. This method converts a tree to dot representation, saves it to a .dot file, and then converts it to a PDF file that will be stored in your `src` directory. You can then use the web browser to view the PDF file. Make sure that your file string argument for the `BPlusTree::toDotPDFFile` method has ".pdf" at the end.

## Step 4: Submitting the Assignment
After you complete the assignment, simply commit and `git push` your `hw2`
branch. 60% of your grade will come from passing the unit tests we provide to
you. 40% of your grade will come from passing unit tests that we have not
provided to you. If your code does not compile on the VM with maven, we reserve
the right to give you a 0 on the assignment.

[eclipse_maven]: https://stackoverflow.com/a/36242422
[intellij_maven]: https://www.jetbrains.com/help/idea//2017.1/importing-project-from-maven-model.html
[eclipse_debugging]: http://www.vogella.com/tutorials/EclipseDebugging/article.html
[intellij_debugging]: https://www.jetbrains.com/help/idea/debugging.html
