# Project 5: Lock Manager

## Logistics
**Due date: Tuesday 11/21/2017, 1:59:59 PM**

* This is your last project for CS 186 -- you're almost done!
* Please make sure you submit to the correct branch, or your work will not be
  graded.
* There are no hidden tests for this project. You only need to pass the
  provided staff tests.

## API Overview
You will be implementing the logic for table-level locking (no intent locks) as
well as the wait-die and wound-wait deadlock avoidance algorithms. The goal of
this project is to test your understanding of locking and deadlock
fundamentals. To that end, we have created a high-level API for the LockManager,
Transaction, and Request objects that are managed by a single-threaded service.
Requests to these objects are handled sequentially and atomically by that service. 
(In many database systems, these objects are shared across database engine
threads, and hence require additional logic to manage their concurrent data
structures. We have abstracted that problem away for you.)

**NOTE:** Please do not change any of the interfaces that we've given you. It's
also a good idea to always check Piazza for updates on the project.

* Transactions are represented through the `Transaction` class.
* The `LockManager` keeps track of the lock information for each table using
  `TableLock` objects.
* Each `TableLock` object stores
    * the type of the lock (either `LockType.Shared` or `LockType.Exclusive`)
    * the set of transactions that own the lock (could be more than one for
      shared locks)
    * a queue of `Request` objects 
* Each `Request` object stores
    * the transaction that made the request
    * the type of lock requested
* The `LockManager` also keeps track of which deadlock policy (if any) is being
  enforced on this lock manager

# Part 0: Fetching the Assignment
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

# Part 1: Locking

### 1.1 Acquire Lock
For the first part of this project you will implement the `LockManager#acquire`
method. When a transaction `T` tries to acquire a (shared or exclusive) lock on
a table, one of the following things will happen:

1. If no other transaction currently holds a lock on the table, then `T`'s lock
   request is granted.
2. If some other transaction currently holds an exclusive lock on the table,
   then `T`'s lock request is denied and `T` blocks.
3. If some other transactions currently hold a shared lock on the table, and
   `T` is requesting a shared lock, then `T`'s lock request is granted.
4. If some other transactions currently hold a shared lock on the table, and
   `T` is requesting an exclusive lock, then `T`'s lock request is denied and
   `T` blocks.
5. If `T` holds a shared lock on the table, and no other transaction holds a
   shared lock on the table, and`T` is requesting an exclusive lock, then `T`'s
   lock request is granted. Its lock is _promoted_ from a shared lock to an
   exclusive lock.

If `T`'s lock request is granted, we say that the lock request is _compatible_
with the table's lock. We have provided an unimplemented
`LockManager#compatible` helper method that checks to see if a lock request is
compatible. We encourage you to implement and use this helper method, but you
are free not to if you do not want to (the tests do not call this function
directly).

If `T`'s lock request is denied, `T` is placed on a FIFO queue of transactions
that are waiting to acquire the lock. Concretely, you need to make a `Request`
object and add it to the lock's `requestersQueue`. If the request was for a
promotion, add the request to the front of the queue. Make sure that call
`Transaction#sleep` to update the status of `T` to `Transaction.Status.Waiting`.
In a real database system this causes the current thread to suspend execution
for a specified period. Later on, in part 2, we will worry about avoiding
deadlocks.

Also note the following corner cases:
* If a blocked transaction calls acquire, throw an `IllegalArgumentException`.
* If a transaction that currently holds an exclusive lock on a table requests a
  shared lock on the same table, throw an `IllegalArgumentException`.
* If a transaction requests a lock that it already holds, throw an
  `IllegalArgumentException`.

### 1.2 Release Lock
Next, you will implement `LockManager#release`. This method releases a lock
held by a transaction. After the lock is released, we do one of the following
things:

* If there is a single shared owner of the lock, and that owner has an
  exclusive lock request in `requestersQueue`, we grant the lock request. This
  is a lock promotion.
* If there are multiple shared owners of the lock, then all shared requests in
  `requestersQueue` should be granted.
* If no transaction owns the lock and if the first request in `requestersQueue`
  is a shared request, then all shared requests should be granted.
* If no transaction owns the lock and if the first request in `requestersQueue`
  is an exclusive request, that exclusive request should be granted.
* If the lock has no owners and `requestersQueue` is empty, remove the lock
  from the `tableToTableLock` map.

Note that we prioritize lock promotions meaning that if a transaction releasing
its shared lock on the table allows for a lock promotion for another
transaction that currently owns a shared lock on that table, we prioritize the
promotion over what is in front of the queue.

Remember to wake up the thread by calling `Transaction#wake`if it is removed
from `requestersQueue` and granted a lock. This will update the status of a
transaction to `Transaction.Status.Running`.

Also note the following corner cases:
* If a blocked transaction calls `release`, throw an
  `IllegalArgumentException`.
* If a transaction tries to `release` a lock it does not hold, throw an
  `IllegalArgumentException`.

### 1.3 Holds Lock
Next, you will implement `LockManager#holds`. This should be short, as it is
just checking if a given transaction holds a lock of a given type on a given
table.

After you complete Part 1, you should be passing all the tests in
`TestLockManager.java`.

# Part 2: Deadlock Avoidance
In this part of the project, the `LockManager` will avoid deadlock based on
which policy (wait-die or wound-wait) is passed in. We assign priorities to
each `Transaction` based on timestamps, and you will use these priorities to
implement the two policies. Remember from lecture that an earlier timestamp
means higher priority. After you implement the `LockManager#waitDie` and
`LockManager#woundWait` functions (specified below), make sure that you
actually call the appropriate one (based on the value of the
`deadlockAvoidanceType` variable) in `LockManager#acquire` if the lock request
is not compatible.

### 2.1 Wait-Die
Implement the `LockManager#waitDie` function. Under the wait-die deadlock
avoidance algorithm, if a transaction wants an incompatible lock on a table
that another transaction already holds the lock for, the requesting transaction
will wait in the FIFO queue if it has a lower timestamp (higher priority) than
the transaction holding the lock. If the requesting transaction has a higher
timestamp (lower priority) than the transaction owning the lock, it will abort
and not wait for the lock to be released. In the case that there is more than
one lock owner (for shared locks), the requesting transaction should wait if
its timestamp is lower than that of every transaction owning the lock;
otherwise, it should abort. Don't forget to call `Transaction#sleep` if it ends
up waiting.

For this project, all you have to do to "abort" a transaction is call
`Transaction#abort`. In a real database system, we would need to rollback the
changes made by the transaction as well.

### 2.2 Wound-Wait
Implement the `LockManager#woundWait` function. Under the wound-wait deadlock
avoidance algorithm, if the transaction requesting an incompatible lock has a
lower timestamp (higher priority) than the transaction holding the lock, then
the owner transaction will abort and the requesting transaction will receive
the lock. If the requesting transaction has a higher timestamp (lower priority)
than the transaction holding the lock, it will wait in the FIFO queue as usual.
In the case that there is more than one lock owner (for shared locks), all the
owning transactions should abort if the requesting transaction has a lower
timestamp than the oldest one in the owners set. Otherwise, the requesting
transaction should just be placed in the queue. If it ends up waiting, don't
forget to call `Transaction#sleep` which updates the status of the transaction
to `Transaction.Status.Waiting`.

After you complete Part 2, you should be passing all the tests in
`TestDeadlock.java`.

### Submitting the Assignment
After you complete the assignment, simply commit and git push your hw5 branch.
Your grade will be determined based on passing the unit tests we provide to you
(again, there are no hidden tests for this project). If your code does not
compile on the VM with maven, we reserve the right to give you a 0 on the
assignment.