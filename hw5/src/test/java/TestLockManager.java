import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestLockManager {
    private LockManager lockMan;

    @Test
    public void testSimpleAcquire() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Resource r1 = new Table("A");
        Resource r2 = new Table("B");

        lockMan.acquire(t1, r1, LockManager.LockType.S);
        lockMan.acquire(t2, r2, LockManager.LockType.X);

        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.X));

        assertTrue(lockMan.holds(t2, r2, LockManager.LockType.X));
        assertFalse(lockMan.holds(t2, r2, LockManager.LockType.S));
    }

    @Test
    public void testSimpleRelease() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Resource r1 = new Table("A");

        lockMan.acquire(t1, r1, LockManager.LockType.S);
        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.S));

        lockMan.release(t1, r1);
        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.S));
    }

    @Test
    public void testTwoSDifferentTransaction() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Resource r1 = new Table("A");

        lockMan.acquire(t1, r1, LockManager.LockType.S);
        lockMan.acquire(t2, r1, LockManager.LockType.S);

        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.S));

        lockMan.release(t1, r1);

        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.S));

        lockMan.release(t2, r1);

        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.S));
    }

    @Test
    public void testSAndXDifferentTransactions() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Resource r1 = new Table("A");

        lockMan.acquire(t1, r1, LockManager.LockType.S);
        lockMan.acquire(t2, r1, LockManager.LockType.X);

        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.X));

        lockMan.release(t1, r1);

        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.X));

        lockMan.release(t2, r1);

        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.X));
    }

    @Test
    public void testTwoXDifferentTransaction() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Resource r1 = new Table("A");

        lockMan.acquire(t1, r1, LockManager.LockType.X);
        lockMan.acquire(t2, r1, LockManager.LockType.X);

        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.X));

        lockMan.release(t1, r1);

        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.X));

        lockMan.release(t2, r1);

        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.X));
    }

    @Test
    public void testFIFOQueueLocks() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);
        Transaction t3 = new Transaction("t3", 3);

        Resource r1 = new Table("A");

        lockMan.acquire(t1, r1, LockManager.LockType.X);
        lockMan.acquire(t2, r1, LockManager.LockType.X);
        lockMan.acquire(t3, r1, LockManager.LockType.X);

        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.X));
        assertFalse(lockMan.holds(t3, r1, LockManager.LockType.X));

        lockMan.release(t1, r1);

        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.X));
        assertFalse(lockMan.holds(t3, r1, LockManager.LockType.X));

        lockMan.release(t2, r1);

        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.X));
        assertTrue(lockMan.holds(t3, r1, LockManager.LockType.X));

    }

    @Test
    public void testManySPromoteSimultaneous() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);
        Transaction t3 = new Transaction("t3", 3);
        Transaction t4 = new Transaction("t4", 4);

        Resource r1 = new Table("A");

        lockMan.acquire(t1, r1, LockManager.LockType.X);
        lockMan.acquire(t2, r1, LockManager.LockType.S);
        lockMan.acquire(t3, r1, LockManager.LockType.S);
        lockMan.acquire(t4, r1, LockManager.LockType.S);

        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.S));
        assertFalse(lockMan.holds(t3, r1, LockManager.LockType.S));
        assertFalse(lockMan.holds(t4, r1, LockManager.LockType.S));

        lockMan.release(t1, r1);

        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.S));
        assertTrue(lockMan.holds(t3, r1, LockManager.LockType.S));
        assertTrue(lockMan.holds(t4, r1, LockManager.LockType.S));

        lockMan.release(t2, r1);
        lockMan.release(t3, r1);
        lockMan.release(t4, r1);

        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.S));
        assertFalse(lockMan.holds(t3, r1, LockManager.LockType.S));
        assertFalse(lockMan.holds(t4, r1, LockManager.LockType.S));

    }

    @Test
    public void testStatusUpdates() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Resource r1 = new Table("A");

        lockMan.acquire(t1, r1, LockManager.LockType.X);
        lockMan.acquire(t2, r1, LockManager.LockType.X);

        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.X));
        assertEquals(Transaction.Status.Running, t1.getStatus());
        assertEquals(Transaction.Status.Waiting, t2.getStatus());

        lockMan.release(t1, r1);

        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.X));
        assertEquals(Transaction.Status.Running, t1.getStatus());
        assertEquals(Transaction.Status.Running, t2.getStatus());

    }

    @Test
    public void testTableImmediateUpgrade() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Resource r1 = new Table("A");

        lockMan.acquire(t1, r1, LockManager.LockType.S);

        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.S));

        lockMan.acquire(t1, r1, LockManager.LockType.X);

        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.X));
    }

    @Test
    public void testTableEventualUpgrade() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Resource r1 = new Table("A");

        lockMan.acquire(t1, r1, LockManager.LockType.S);
        lockMan.acquire(t2, r1, LockManager.LockType.S);

        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.S));

        lockMan.acquire(t1, r1, LockManager.LockType.X);

        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.S));

        lockMan.release(t2, r1);

        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.S));

        lockMan.release(t1, r1);

        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.S));

    }

    @Test
    public void skipQueue() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);
        Transaction t3 = new Transaction("t3", 2);

        Resource r1 = new Table("A");

        lockMan.acquire(t1, r1, LockManager.LockType.S);
        lockMan.acquire(t2, r1, LockManager.LockType.S);
        lockMan.acquire(t3, r1, LockManager.LockType.X);
        lockMan.acquire(t2, r1, LockManager.LockType.X);

        assertEquals(Transaction.Status.Waiting, t3.getStatus());
        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.S));
        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.X));

        lockMan.release(t1, r1);

        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.X));
    }

    @Test
    public void testIntentLocks() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);
        Resource r2 = new Page("2", a);

        lockMan.acquire(t1, r0, LockManager.LockType.IS);
        lockMan.acquire(t1, r1, LockManager.LockType.S);
        lockMan.acquire(t2, r0, LockManager.LockType.IX);
        lockMan.acquire(t2, r2, LockManager.LockType.X);

        assertTrue(lockMan.holds(t1, r0, LockManager.LockType.IS));
        assertTrue(lockMan.holds(t2, r0, LockManager.LockType.IX));
        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertTrue(lockMan.holds(t2, r2, LockManager.LockType.X));
    }

    @Test
    public void testSharedPage() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);
        lockMan.acquire(t1, r0, LockManager.LockType.IS);
        lockMan.acquire(t2, r0, LockManager.LockType.IS);
        lockMan.acquire(t1, r1, LockManager.LockType.S);
        lockMan.acquire(t2, r1, LockManager.LockType.S);

        assertTrue(lockMan.holds(t1, r0, LockManager.LockType.IS));
        assertTrue(lockMan.holds(t2, r0, LockManager.LockType.IS));
        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.S));
    }

    @Test
    public void testIXandIS() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);
        Resource r2 = new Page("2", a);
        lockMan.acquire(t1, r0, LockManager.LockType.IX);
        lockMan.acquire(t2, r0, LockManager.LockType.IS);
        lockMan.acquire(t1, r1, LockManager.LockType.X);
        lockMan.acquire(t2, r2, LockManager.LockType.S);

        assertTrue(lockMan.holds(t1, r0, LockManager.LockType.IX));
        assertTrue(lockMan.holds(t2, r0, LockManager.LockType.IS));
        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertTrue(lockMan.holds(t2, r2, LockManager.LockType.S));
    }

    @Test
    public void testIXandIX() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);
        Resource r2 = new Page("2", a);
        lockMan.acquire(t1, r0, LockManager.LockType.IX);
        lockMan.acquire(t2, r0, LockManager.LockType.IX);
        lockMan.acquire(t1, r1, LockManager.LockType.X);
        lockMan.acquire(t2, r2, LockManager.LockType.X);

        assertTrue(lockMan.holds(t1, r0, LockManager.LockType.IX));
        assertTrue(lockMan.holds(t2, r0, LockManager.LockType.IX));
        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.X));
        assertTrue(lockMan.holds(t2, r2, LockManager.LockType.X));
    }

    @Test
    public void testSandIS() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);
        lockMan.acquire(t1, r0, LockManager.LockType.S);
        lockMan.acquire(t2, r0, LockManager.LockType.IS);
        lockMan.acquire(t2, r1, LockManager.LockType.S);
        lockMan.release(t1, r0);
        assertTrue(lockMan.holds(t2, r0, LockManager.LockType.IS));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.S));
        assertFalse(lockMan.holds(t1, r0, LockManager.LockType.S));
    }

    @Test
    public void testIXandX() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Table a = new Table("A");
        Resource r0 = a;

        lockMan.acquire(t1, r0, LockManager.LockType.IX);
        lockMan.acquire(t2, r0, LockManager.LockType.X);

        assertTrue(lockMan.holds(t1, r0, LockManager.LockType.IX));
        assertFalse(lockMan.holds(t2, r0, LockManager.LockType.X));
    }

    @Test
    public void testSharedIntentConflict() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);

        lockMan.acquire(t1, r0, LockManager.LockType.IS);
        lockMan.acquire(t2, r0, LockManager.LockType.IX);
        lockMan.acquire(t1, r1, LockManager.LockType.S);
        lockMan.acquire(t2, r1, LockManager.LockType.X);

        assertTrue(lockMan.holds(t1, r0, LockManager.LockType.IS));
        assertTrue(lockMan.holds(t2, r0, LockManager.LockType.IX));
        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertFalse(lockMan.holds(t2, r1, LockManager.LockType.X));
    }

    @Test
    public void testIntentPromote() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Table a = new Table("A");
        Resource r0 = a;

        lockMan.acquire(t1, r0, LockManager.LockType.S);
        lockMan.acquire(t2, r0, LockManager.LockType.IX);

        assertTrue(lockMan.holds(t1, r0, LockManager.LockType.S));
        assertFalse(lockMan.holds(t2, r0, LockManager.LockType.IX));

        lockMan.release(t1, r0);

        assertFalse(lockMan.holds(t1, r0, LockManager.LockType.S));
        assertTrue(lockMan.holds(t2, r0, LockManager.LockType.IX));
    }

    @Test
    public void testPagePromote() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);

        lockMan.acquire(t1, r0, LockManager.LockType.IS);
        lockMan.acquire(t2, r0, LockManager.LockType.IX);
        lockMan.acquire(t1, r1, LockManager.LockType.S);
        lockMan.acquire(t2, r1, LockManager.LockType.X);
        lockMan.release(t1, r1);

        assertTrue(lockMan.holds(t1, r0, LockManager.LockType.IS));
        assertTrue(lockMan.holds(t2, r0, LockManager.LockType.IX));
        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertTrue(lockMan.holds(t2, r1, LockManager.LockType.X));
    }

    @Test
    public void testPageUpgrade() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);

        lockMan.acquire(t1, r0, LockManager.LockType.IX);
        lockMan.acquire(t1, r1, LockManager.LockType.S);
        lockMan.acquire(t1, r1, LockManager.LockType.X);

        assertTrue(lockMan.holds(t1, r0, LockManager.LockType.IX));
        assertFalse(lockMan.holds(t1, r1, LockManager.LockType.S));
        assertTrue(lockMan.holds(t1, r1, LockManager.LockType.X));
    }

    @Test(expected = IllegalArgumentException.class)
    public void blockedRequestsLock() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Resource r1 = new Table("A");
        Resource r2 = new Table("B");

        lockMan.acquire(t1, r1, LockManager.LockType.X);
        lockMan.acquire(t2, r1, LockManager.LockType.S);

        assertEquals(Transaction.Status.Waiting, t2.getStatus());

        lockMan.acquire(t2, r2, LockManager.LockType.S);
    }

    @Test(expected = IllegalArgumentException.class)
    public void requestHeldLock() {
        /**
         * Throws an exception when a transaction requests a lock that it already
         *  holds.
         */
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Resource r1 = new Table("A");

        lockMan.acquire(t1, r1, LockManager.LockType.X);
        lockMan.acquire(t1, r1, LockManager.LockType.X);
    }

    @Test(expected = IllegalArgumentException.class)
    public void requestDowngrade() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Resource r1 = new Table("A");

        lockMan.acquire(t1, r1, LockManager.LockType.X);
        lockMan.acquire(t1, r1, LockManager.LockType.S);
    }

    @Test(expected = IllegalArgumentException.class)
    public void requestIntentPageIS() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);
        lockMan.acquire(t1, r0, LockManager.LockType.IS);
        lockMan.acquire(t1, r1, LockManager.LockType.IS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssertIS1() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);
        lockMan.acquire(t1, r1, LockManager.LockType.S);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssertIS2() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);
        lockMan.acquire(t1, r0, LockManager.LockType.S);
        lockMan.acquire(t1, r1, LockManager.LockType.S);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssertIX1() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);
        lockMan.acquire(t1, r1, LockManager.LockType.X);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssertIX2() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);
        lockMan.acquire(t1, r0, LockManager.LockType.S);
        lockMan.acquire(t1, r1, LockManager.LockType.X);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssertIX3() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);
        lockMan.acquire(t1, r0, LockManager.LockType.X);
        lockMan.acquire(t1, r1, LockManager.LockType.X);
    }

    @Test(expected = IllegalArgumentException.class)
    public void blockedTransactionReleases() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);
        Transaction t2 = new Transaction("t2", 2);

        Resource r1 = new Table("A");
        Resource r2 = new Table("B");

        lockMan.acquire(t1, r1, LockManager.LockType.X);
        lockMan.acquire(t2, r2, LockManager.LockType.S);
        lockMan.acquire(t2, r1, LockManager.LockType.S);

        assertEquals(Transaction.Status.Waiting, t2.getStatus());

        lockMan.release(t2, r1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void releaseUnheldLock() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Resource r1 = new Table("A");

        lockMan.release(t1, r1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalTableRelease() {
        lockMan = new LockManager();

        Transaction t1 = new Transaction("t1", 1);

        Table a = new Table("A");
        Resource r0 = a;
        Resource r1 = new Page("1", a);
        lockMan.acquire(t1, r0, LockManager.LockType.IS);
        lockMan.acquire(t1, r1, LockManager.LockType.S);

        lockMan.release(t1, r0);

    }
}