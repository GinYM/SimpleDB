import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestDeadlock {

    private LockManager lockMan;

    @Test
    public void testWaitDie1() {
        /**
         * If a transaction with a lower priority (higher timestamp) requests
         *  a conflicting lock, it should abort.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.WaitDie);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);
        lockMan.acquire(t2, "A", LockManager.LockType.Shared);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertEquals(Transaction.Status.Running, t1.getStatus());
        assertEquals(Transaction.Status.Aborting, t2.getStatus());
    }

    @Test
    public void testWaitDie2() {
        /**
         * If a transaction with a higher priority (lower timestamp) requests a
         * conflicting lock, it should wait until the lock is released.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.WaitDie);

        Transaction t1 = new Transaction("t1", 2);
        Transaction t2 = new Transaction("t2", 1);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
        assertEquals(Transaction.Status.Running, t1.getStatus());
        assertEquals(Transaction.Status.Waiting, t2.getStatus());

        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
        assertEquals(Transaction.Status.Running, t1.getStatus());
        assertEquals(Transaction.Status.Running, t2.getStatus());
    }

    @Test
    public void testWaitDie3() {
        /**
         * Transactions with higher priority than lock owner are placed on FIFO
         *  queue and upgraded when lock is released.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.WaitDie);

        Transaction t1 = new Transaction("t1", 2);
        Transaction t2 = new Transaction("t2", 1);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Exclusive);
        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
        assertEquals(Transaction.Status.Running, t1.getStatus());
        assertEquals(Transaction.Status.Running, t2.getStatus());
    }

    @Test
    public void testWoundWait1() {
        /**
         * Request for conflicting lock from thread with lower priority (higher
         * 	timestamp) is placed in FIFO queue.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.WoundWait);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
        assertEquals(Transaction.Status.Running, t1.getStatus());
        assertEquals(Transaction.Status.Waiting, t2.getStatus());
    }

    @Test
    public void testWoundWait2() {
        /**
         * Request for conflicting lock from thread with higher priority (lower
         * 	timestamp) aborts current lock holder and grants lock to requesting
         * 	thread
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.WoundWait);

        Transaction t1 = new Transaction("t1", 2);
        Transaction t2 = new Transaction("t2", 1);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Exclusive);

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
        assertEquals(Transaction.Status.Aborting, t1.getStatus());
        assertEquals(Transaction.Status.Running, t2.getStatus());
    }

    @Test
    public void testWoundWait3() {
        /**
         * Lock is upgraded from FIFO queue.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.WoundWait);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Exclusive);
        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
        assertEquals(Transaction.Status.Running, t1.getStatus());
        assertEquals(Transaction.Status.Running, t2.getStatus());
    }

    @Test
    public void testWoundWaitHard() {
        /**
         * Higher priority transaction aborts multiple lower priority
         * 	conflicting transactions.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.WoundWait);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);
        Transaction t3 = new Transaction("t3", 3);
        Transaction t4 = new Transaction("t4", 4);

        lockMan.acquire(t2, "A", LockManager.LockType.Shared);
        lockMan.acquire(t3, "A", LockManager.LockType.Shared);
        lockMan.acquire(t4, "A", LockManager.LockType.Shared);

        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t3, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t4, "A", LockManager.LockType.Shared));

        assertEquals(Transaction.Status.Running, t2.getStatus());
        assertEquals(Transaction.Status.Running, t3.getStatus());
        assertEquals(Transaction.Status.Running, t4.getStatus());

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t3, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t4, "A", LockManager.LockType.Shared));
        assertEquals(Transaction.Status.Aborting, t2.getStatus());
        assertEquals(Transaction.Status.Aborting, t3.getStatus());
        assertEquals(Transaction.Status.Aborting, t4.getStatus());
    }

    @Test
    public void testWoundAbortsFromQueue1() {
        /**
         * If a transaction was aborted by a higher priority transaction, if that
         *  transaction also had requests in the queue, they should be aborted.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.WoundWait);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);
        Transaction t3 = new Transaction("t3", 3);
        Transaction t4 = new Transaction("t4", 4);

        lockMan.acquire(t2, "A", LockManager.LockType.Shared);
        lockMan.acquire(t3, "A", LockManager.LockType.Shared);
        lockMan.acquire(t4, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t3, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t4, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));

        assertEquals(Transaction.Status.Waiting, t2.getStatus());
        assertEquals(Transaction.Status.Running, t3.getStatus());
        assertEquals(Transaction.Status.Running, t4.getStatus());

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t3, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t4, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
        assertEquals(Transaction.Status.Aborting, t2.getStatus());
        assertEquals(Transaction.Status.Aborting, t3.getStatus());
        assertEquals(Transaction.Status.Aborting, t4.getStatus());

        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
    }

    @Test
    public void testWoundAbortsFromQueue2() {
        /**
         * If a transaction was on the queue but not a lock owner, the transaction
         *  remains on the queue when higher priority transaction acquires lock.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.WoundWait);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);
        Transaction t3 = new Transaction("t3", 3);

        lockMan.acquire(t2, "A", LockManager.LockType.Shared);
        lockMan.acquire(t3, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t3, "A", LockManager.LockType.Exclusive));

        assertEquals(Transaction.Status.Running, t2.getStatus());
        assertEquals(Transaction.Status.Waiting, t3.getStatus());

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t3, "A", LockManager.LockType.Exclusive));
        assertEquals(Transaction.Status.Running, t1.getStatus());
        assertEquals(Transaction.Status.Aborting, t2.getStatus());
        assertEquals(Transaction.Status.Waiting, t3.getStatus());

        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertTrue(lockMan.holds(t3, "A", LockManager.LockType.Exclusive));
    }
}
