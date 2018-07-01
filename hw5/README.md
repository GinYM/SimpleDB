# Project 5: Lock Manager

## Logistics
**Due date: Saturday 4/28/2018, 11:59:59 PM**

* This is your last homework for CS 186 -- you're almost done!
* Please make sure you submit to the correct branch, or your work will not be
  graded.
  
# Getting Started
First, open up Virtual Box and power on the CS186 virtual machine. Once the
machine is booted up, open a terminal and go to the `course-projects` folder
you created in hw0.
```
$ cd course-projects
```
Make sure that you switch to the master branch:
```
$ git checkout master
```
It is good practice to run `git status` to make sure that you haven't
inadvertently changed anything in the master branch.  Now, you want to add the
reference to the staff repository so you call pull the new homework files:
```
$ git fetch staff master
$ git merge staff/master master
```
The `git merge` will give you a warning and a merge prompt if you have made any
conflicting changes to master.

As with hw1, hw2, hw3, and hw4, make sure you create a new branch for your work:
```
git checkout -b hw5
```
Now, you should be ready to start the homework. *Don't forget to push to this
branch when you are done with everything!*

## API Overview
You will be implementing the logic for table and page-level locking. The goal of
this project is to test your understanding of locking fundamentals. 
To that end, we have created a high-level API for the LockManager,
Transaction, and Request objects that are managed by a single-threaded service.
Requests to these objects are handled sequentially and atomically by that service. 
(In many database systems, these objects are shared across database engine
threads, and hence require additional logic to manage their concurrent data
structures. We have abstracted that problem away for you.)

**NOTE:** Please do not change any of the interfaces that we've given you. It's
also a good idea to always check Piazza for updates on the project.

* Transactions are represented through the `Transaction` class.
* There are two types of resources (see the `Resource` class) on which transactions can obtain locks: 
tables (see the `Table` class) and pages (see the `Page` class). Tables consist of 1 of more pages. 
* Each `Request` object stores
    * the transaction that made the request
    * the type of lock requested
        * The locks you will need to support are `LockType.S`, `LockType.X`, `LockType.IS`, and `LockType.IX`.
          You will not need to support SIX locks.
        * Remember that since tables consist of pages, transactions can request intent locks as well as regular locks on a table.
          A transaction cannot request an intent lock on a page.
* The `LockManager` keeps track of the lock information for each resource using
  `ResourceLock` objects.
* Each `ResourceLock` object stores
    * a list of `Request` objects that represent which transactions own which type of lock on this resource
    * a queue of waiting `Request` objects 
    
## Part 1: Acquire Lock
For the first part of this project you will implement the `LockManager#acquire`
method. When a transaction `T` tries to acquire a lock on
a resource, either it is granted the lock or it gets added to the back of the FIFO queue
for the lock and the transaction is blocked. More specifically:

* If `T`'s lock request is _compatible_ with the resource's lock, it is added to the list of owners for the lock.
    * Concretely, you need to make a `Request` object and add it to the list of the lock's `lockOwners`.
    * A request is considered compatible on a resource if it is compatible with all the
requests that are currently granted on the resource based on the lock comparability matrix that you should 
have seen in lecture. We recommend you directly implement this matrix somewhere in your code, but you are not
required to do so.
    * We have provided an unimplemented `LockManager#compatible` helper method that checks to see if a lock request is
compatible. We encourage you to implement and use this helper method, but it is not required (the tests do not call this function directly).

* If `T`'s lock request is not compatible, `T` is placed on a FIFO queue of transactions
that are waiting to acquire the lock.
    * Concretely, you need to make a `Request` object and add it to the back of the lock's `requestersQueue`.
    * Make sure to call `Transaction#sleep` to update the status of `T` to `Transaction.Status.Waiting`.
      In a real database system this would cause the current thread to suspend execution
      for a specified period.

Note that we prioritize lock upgrades. This means that if a transaction is requesting a lock
upgrade (S to an X lock) and currently owns an S lock, if we can perform the
upgrade immediately (based on the compatibility matrix), then we do. Otherwise, we "prioritize" the
upgrade by placing it at the front of the queue. You DO NOT have to worry about IS to IX upgrades. 
We will not test this case.

We will also not be testing lock escalation (IS to S or IX to X). You do not have to handle this case in your code.

Throw an `IllegalArgumentException` in any of the following error cases:
* If a blocked transaction calls acquire
* If a transaction requests a lock that it already holds
* If a transaction that currently holds an X lock on a resource requests an S lock on the same resource (downgrade)
* If a transaction that currently holds an IX lock on a table requests an IS lock the same table (downgrade)
* If a transaction requests an intent lock on a page
* If a transaction requests an S or X lock on a page without having an appropriate intent lock on the parent table.

## Part 2: Release Lock

Next, you will implement `LockManager#release`. This method releases the lock
held by a transaction for a specific resource. More specifically:

* The transaction should release any lock it contains on the resource.
* The set of mutually compatible requests from the beginning of the lock's `requestersQueue`
should be granted and the corresponding transaction should be woken up. 
* We have provided an unimplemented `LockManager#promote` helper method that will grant mutually
compatible lock requests for the resource from the FIFO queue. We encourage you to implement and
use this helper method, but you are free not to if you do not want to (the tests do not call this function
directly). You are also free to call `LockManager#compatible` in this part if you wish to.

Remember to wake up the thread by calling `Transaction#wake`if it is removed
from `requestersQueue` and granted a lock. This will update the status of a
transaction to `Transaction.Status.Running`.

Throw an `IllegalArgumentException` in any of the following error cases:
* If a blocked transaction calls release
* If the transaction doesn't hold any of the four possible lock types on this resource
* If a transaction is trying to release a table level lock without having released all the locks for the pages of that table first

## Part 3: Holds Lock

Next, you will implement `LockManager#holds`. This should be short, as it is
just checking if a given transaction holds a lock of a given type on a given
resource.

After you complete all three parts above, you should be passing all the tests in
`TestLockManager.java`. If you are failing a specific test, please read through the
test to see what it is doing and then step through the test using the debugger on your
IDE to see what case you code is not handling correctly.

### Submitting the Assignment
After you complete the assignment, simply commit and git push your hw5 branch.
60% of your grade will come from passing the unit tests we provide to you. 
40% of your grade will come from passing unit tests that we have not provided to you. 
If your code does not compile on the VM with maven, we reserve the right to give you a 0 on the assignment.
