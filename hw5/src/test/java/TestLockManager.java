import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestLockManager {

    private LockManager lockMan;

    @Test
    public void testSimpleAcquire() {
        /**
         * Acquire shared and exclusive locks
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "B", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));

        assertTrue(lockMan.holds(t2, "B", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "B", LockManager.LockType.Shared));

    }

    @Test
    public void testSimpleRelease() {
        /**
         * Lock release without upgrade
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));

        lockMan.release(t1, "A");
        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Shared));
    }

    @Test
    public void testTwoSharedDifferentTransaction() {
        /**
         * Two threads can acquire shared lock on same table
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Shared);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Shared));

        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Shared));

        lockMan.release(t2, "A");

        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Shared));
    }

    @Test
    public void testSharedAndExclusiveDifferentTransactions() {
        /**
         * Prevents incompatible locks on same object. Upgrade from
         *  FIFO queue on lock release.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));

        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));

        lockMan.release(t2, "A");

        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
    }

    @Test
    public void testTwoExclusiveDifferentTransaction() {
        /**
         * Exclusive locks not shared. Upgrade from FIFO queue on lock release.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);
        lockMan.acquire(t2, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));

        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));

        lockMan.release(t2, "A");

        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
    }

    @Test
    public void testSimpleUpgrade() {
        /**
         * Upgrades shared to exclusive lock.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
    }

    @Test
    public void testHardUpgrade1() {
        /**
         * Upgrades shared lock to exclusive lock. Priority given to transaction
         *  that already has shared lock.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));

        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));

        lockMan.release(t2, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
    }

    @Test
    public void testHardUpgrade2() {
        /**
         * Cannot upgrade to exclusive lock if multiple transaction share the
         * lock. Once shared locks are released, upgrade to exclusive possible.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Shared);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Shared));

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Shared));

        lockMan.release(t2, "A");

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Shared));

        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Shared));

    }

    @Test
    public void testHardUpgrade3() {
        /**
         * Check that lock upgrades are prioritized when promoting from the request
         *  queue.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);
        Transaction t3 = new Transaction("t3", 3);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Shared);
        lockMan.acquire(t3, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t3, "A", LockManager.LockType.Exclusive));

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t3, "A", LockManager.LockType.Exclusive));

        lockMan.release(t2, "A");

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t3, "A", LockManager.LockType.Exclusive));

        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertTrue(lockMan.holds(t3, "A", LockManager.LockType.Exclusive));

    }

    @Test
    public void testFIFOQueueLocks() {
        /**
         * Gives priority based on position in FIFO queue. Requests for conflicting
         *  locks handled in order
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);
        Transaction t3 = new Transaction("t3", 3);

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);
        lockMan.acquire(t2, "A", LockManager.LockType.Exclusive);
        lockMan.acquire(t3, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t3, "A", LockManager.LockType.Exclusive));

        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t3, "A", LockManager.LockType.Exclusive));

        lockMan.release(t2, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
        assertTrue(lockMan.holds(t3, "A", LockManager.LockType.Exclusive));

    }

    @Test
    public void testManySharedPromoteSimultaneous() {
        /**
         * Promotes multiple locks at once if possible. In this example, multiple
         *  shared locks can be upgraded at once after the exclusive lock is
         *  released.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);
        Transaction t3 = new Transaction("t3", 3);
        Transaction t4 = new Transaction("t4", 4);

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);
        lockMan.acquire(t2, "A", LockManager.LockType.Shared);
        lockMan.acquire(t3, "A", LockManager.LockType.Shared);
        lockMan.acquire(t4, "A", LockManager.LockType.Shared);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t3, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t4, "A", LockManager.LockType.Shared));

        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t3, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t4, "A", LockManager.LockType.Shared));

        lockMan.release(t2, "A");
        lockMan.release(t3, "A");
        lockMan.release(t4, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t3, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t4, "A", LockManager.LockType.Shared));

    }

    @Test
    public void testStatusUpdates() {
        /**
         * Check if status are updated properly when a transaction is placed in
         *  and removed from the waiting queue.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);
        lockMan.acquire(t2, "A", LockManager.LockType.Exclusive);

        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
        assertEquals(Transaction.Status.Running, t1.getStatus());
        assertEquals(Transaction.Status.Waiting, t2.getStatus());

        lockMan.release(t1, "A");

        assertFalse(lockMan.holds(t1, "A", LockManager.LockType.Exclusive));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
        assertEquals(Transaction.Status.Running, t1.getStatus());
        assertEquals(Transaction.Status.Running, t2.getStatus());

    }

    @Test(expected = IllegalArgumentException.class)
    public void requestHeldLock() {
        /**
         * Throws an exception when a transaction requests a lock that it already
         *  holds.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);
        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalSharedRequest() {
        /**
         * Throws an exception when transaction holding exclusive lock requests
         *  a shared lock on the same table.
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);
        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
    }

    @Test(expected = IllegalArgumentException.class)
    public void blockedRequestsLock() {
        /**
         * Throws an exception when a blocked transaction calls acquire
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);
        lockMan.acquire(t2, "A", LockManager.LockType.Shared);


        assertEquals(Transaction.Status.Waiting, t2.getStatus());

        lockMan.acquire(t2, "B", LockManager.LockType.Shared);
    }

    @Test(expected = IllegalArgumentException.class)
    public void releaseUnheldLock() {
        /**
         * Throws an exception when a transaction releases a lock it doesn't hold
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);

        lockMan.release(t1, "A");
    }

    @Test(expected = IllegalArgumentException.class)
    public void blockedTransactionReleases() {
        /**
         * Throws an exception when a blocked transaction calls release
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Exclusive);
        lockMan.acquire(t2, "B", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Shared);

        assertEquals(Transaction.Status.Waiting, t2.getStatus());

        lockMan.release(t2, "A");
    }

    @Test
    public void skipQueue() {
        /**
         * Throws an exception when a blocked transaction calls release
         */
        lockMan = new LockManager(LockManager.DeadlockAvoidanceType.None);

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);
        Transaction t3 = new Transaction("t3", 2);

        lockMan.acquire(t1, "A", LockManager.LockType.Shared);
        lockMan.acquire(t2, "A", LockManager.LockType.Shared);
        lockMan.acquire(t3, "A", LockManager.LockType.Exclusive);
        lockMan.acquire(t2, "A", LockManager.LockType.Exclusive);

        assertEquals(Transaction.Status.Waiting, t3.getStatus());
        assertTrue(lockMan.holds(t1, "A", LockManager.LockType.Shared));
        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Shared));
        assertFalse(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));

        lockMan.release(t1, "A");

        assertTrue(lockMan.holds(t2, "A", LockManager.LockType.Exclusive));
    }

}