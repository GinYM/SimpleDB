import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hashtable that is keyed on the name of the
 * table being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {
    private DeadlockAvoidanceType deadlockAvoidanceType;
    private HashMap<String, TableLock> tableToTableLock;

    public enum DeadlockAvoidanceType {
        None,
        WaitDie,
        WoundWait
    }

    public enum LockType {
        Shared,
        Exclusive
    }

    public LockManager(DeadlockAvoidanceType type) {
        this.deadlockAvoidanceType = type;
        this.tableToTableLock = new HashMap<String, TableLock>();
    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue. Once you have implemented deadlock avoidance algorithms, you
     * should instead check the deadlock avoidance type and call the
     * appropriate function that you will complete in part 2.
     * @param transaction that is requesting the lock
     * @param tableName of requested table
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, String tableName, LockType lockType)
            throws IllegalArgumentException {
        //TODO: HW5 Implement



        if(transaction.getStatus() == Transaction.Status.Aborting || transaction.getStatus() == Transaction.Status.Waiting){
            throw new IllegalArgumentException();
        }

        if(!compatible(tableName, transaction, lockType)){
            //System.out.println(transaction);
            if(deadlockAvoidanceType == DeadlockAvoidanceType.WoundWait){
                woundWait(tableName, transaction, lockType);
            }else if(deadlockAvoidanceType == DeadlockAvoidanceType.WaitDie){
                waitDie(tableName, transaction, lockType);
            }
            return;
        }

        if(tableToTableLock.containsKey(tableName) == false){
            TableLock tl = new TableLock(lockType);
            tl.lockOwners.add(transaction);
            //System.out.println(tl.lockOwners+" "+tl.lockOwners.size());
            tableToTableLock.put(tableName, tl);
            //System.out.println(tableName);
            return;
        }

        TableLock tl = tableToTableLock.get(tableName);

        if(tl.lockOwners.contains(transaction) && tl.lockType == LockType.Exclusive && lockType == LockType.Shared){
            throw new IllegalArgumentException();
        }
        if(tl.lockOwners.contains(transaction) && tl.lockType == lockType ){
            throw new IllegalArgumentException();
        }

        if(tl.lockOwners.contains(transaction) == false && tl.lockType == LockType.Exclusive){
            tl.requestersQueue.offer(new Request(transaction, lockType));
            transaction.sleep();
        }
        if(tl.lockOwners.contains(transaction) == false && tl.lockType == LockType.Shared && lockType == LockType.Shared){
            tl.lockOwners.add(transaction);
        }
        if(tl.lockOwners.contains(transaction) == false && tl.lockType == LockType.Shared && lockType == LockType.Exclusive){
            tl.requestersQueue.offer(new Request(transaction, lockType));
            transaction.sleep();
        }
        if(tl.lockOwners.contains(transaction) && tl.lockType == LockType.Shared
                && tl.lockOwners.size() == 1 && lockType == LockType.Exclusive){
            tl.lockType = lockType;
            //transaction.wake();
        }else if(tl.lockOwners.contains(transaction) && tl.lockType == LockType.Shared
                && tl.lockOwners.size() > 1 && lockType == LockType.Exclusive){
            tl.requestersQueue.addFirst(new Request(transaction, lockType));
            transaction.sleep();
            //tl.lockOwners.remove(transaction);
        }



    }

    /**
     * This method will return true if the requested lock is compatible. See
     * spec provides compatibility conditions.
     * @param tableName of requested table
     * @param transaction requesting the lock
     * @param lockType of the requested lock
     * @return true if the lock being requested does not cause a conflict
     */
    private boolean compatible(String tableName, Transaction transaction, LockType lockType) {
        //TODO: HW5 Implement



        if(tableToTableLock.containsKey(tableName) == false){
            return true;
        }
        TableLock tl = tableToTableLock.get(tableName);

        if(tl.lockOwners.contains(transaction) == false && tl.lockType == LockType.Exclusive){
            return false;
        }
        if(tl.lockOwners.contains(transaction) == false && tl.lockType == LockType.Shared && lockType == LockType.Shared){
            return true;
        }
        if(tl.lockOwners.contains(transaction) == false && tl.lockType == LockType.Shared && lockType == LockType.Exclusive){
            return false;
        }
        if(tl.lockOwners.contains(transaction) && tl.lockType == LockType.Shared
                && tl.lockOwners.size() == 1 && lockType == LockType.Exclusive){
            return true;
        }
        return false;

    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param tableName of table being released
     */
    public void release(Transaction transaction, String tableName) throws IllegalArgumentException{
        //TODO: HW5 Implement
        if(transaction.getStatus() == Transaction.Status.Aborting ||
            tableToTableLock.containsKey(tableName) == false ||
            tableToTableLock.get(tableName).lockOwners.contains(transaction) == false){
            throw new IllegalArgumentException();
        }
        TableLock tl = tableToTableLock.get(tableName);
        tl.lockOwners.remove(transaction);
        Transaction tran = null;
        if(tl.lockOwners.size()!=0){
            for(Transaction t : tl.lockOwners){
                tran = t;
                break;
            }
        }
        if(tl.lockOwners.size() == 1 && tl.lockType == LockType.Shared
                && tl.requestersQueue.isEmpty() == false
            && tl.requestersQueue.peek().lockType == LockType.Exclusive
                && tl.requestersQueue.peek().transaction.equals(tran)){
            tl.requestersQueue.poll();
            tl.lockType = LockType.Exclusive;

        }else if(tl.lockOwners.size()>=1 && tl.lockType == LockType.Shared){
            Iterator<Request> iter = tl.requestersQueue.iterator();
            while(iter.hasNext()){
                Request req = iter.next();
                if(req.lockType == LockType.Shared){
                    tl.lockOwners.add(req.transaction);
                    req.transaction.wake();
                    iter.remove();
                }
            }
        }else if(tl.lockOwners.isEmpty() && tl.requestersQueue.isEmpty()){
            tableToTableLock.remove(tableName);
        } else if(tl.lockOwners.size() == 0){
            Request rq = tl.requestersQueue.poll();
            tl.lockOwners.add(rq.transaction);
            tl.lockType = rq.lockType;
            rq.transaction.wake();
            //System.out.println(tl);
            if(rq.lockType == LockType.Shared){
                tl.lockType = LockType.Shared;
                Iterator<Request> iter = tl.requestersQueue.iterator();
                while(iter.hasNext()){
                    Request req1 = iter.next();
                    if(req1.lockType == LockType.Shared){
                        tl.lockOwners.add(req1.transaction);
                        iter.remove();
                        req1.transaction.wake();
                    }
                }
            }
        }

    }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the table tableName.
     * @param transaction holding lock
     * @param tableName of locked table
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, String tableName, LockType lockType) {
        //TODO: HW5 Implement
        if(tableToTableLock.containsKey(tableName) == false) return false;
        TableLock tl = tableToTableLock.get(tableName);
        if(tl.lockOwners.contains(transaction) == false){
            //System.out.println(tl.lockOwners+" "+tl.lockOwners.size()+" "+tl.lockType);
            return false;
        }
        if(tl.lockType!=lockType) return false;
        return true;
    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will abort if it has
     * a lower priority (higher timestamp) than all conflicting transactions.
     * If t1 has a higher priority, it will wait on the requesters queue.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void waitDie(String tableName, Transaction transaction, LockType lockType) {
        //TODO: HW5 Implement
        int count = 0;
        //if(compatible(tableName, transaction, lockType) == false){
            TableLock tl = tableToTableLock.get(tableName);
            for(Transaction tran : tl.lockOwners){
                if(transaction.getTimestamp() < tran.getTimestamp()){
                    transaction.sleep();
                    tl.requestersQueue.offer(new Request(transaction, lockType));
                    break;
                }else if(transaction.getTimestamp() > tran.getTimestamp()){
                    count++;
                }
            }
            if(count == tl.lockOwners.size()){
                transaction.abort();
            }
        //}

    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will wait if it has
     * a lower priority (higher timestamp) than conflicting transactions. If t1
     * has a higher priority than every conflicting transaction, it will abort
     * all the lock holders and acquire the lock.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void woundWait(String tableName, Transaction transaction, LockType lockType) {
        //TODO: HW5 Implement
        //if(compatible(tableName, transaction, lockType) == false){
            TableLock tl = tableToTableLock.get(tableName);
            int count = 0;
            for(Transaction tran : tl.lockOwners){
                if(transaction.getTimestamp() < tran.getTimestamp()){
                    count++;
                }
            }
            if(count == tl.lockOwners.size()){
                for(Transaction tran : tl.lockOwners){
                    tran.abort();
                    Iterator<Request> iter = tl.requestersQueue.iterator();
                    while(iter.hasNext()){
                        Request req = iter.next();
                        if(req.transaction.equals(tran)){
                            iter.remove();
                        }
                    }
                }
                tl.lockOwners = new HashSet<>();
                tl.lockOwners.add(transaction);
                tl.lockType = lockType;
                transaction.wake();
            }else{
                transaction.sleep();
                tl.requestersQueue.offer(new Request(transaction, lockType));
            }
        //}
    }

    /**
     * Contains all information about the lock for a specific table. This
     * information includes lock type, lock owner(s), and lock requestor(s).
     */
    private class TableLock {
        private LockType lockType;
        private HashSet<Transaction> lockOwners;
        private LinkedList<Request> requestersQueue;

        public TableLock(LockType lockType) {
            this.lockType = lockType;
            this.lockOwners = new HashSet<Transaction>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requestor queue for a specific table
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }
    }
}