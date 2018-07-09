import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hash table that is keyed on the resource
 * being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {

    public enum LockType {
        S,
        X,
        IS,
        IX
    }

    private HashMap<Resource, ResourceLock> resourceToLock;
    private static boolean[][] matrix;

    public LockManager() {
        this.resourceToLock = new HashMap<Resource, ResourceLock>();
        matrix = new boolean[4][4];
        matrix[LockType.S.ordinal()][LockType.S.ordinal()] = true;
        matrix[LockType.S.ordinal()][LockType.X.ordinal()] = false;
        matrix[LockType.S.ordinal()][LockType.IS.ordinal()] = true;
        matrix[LockType.S.ordinal()][LockType.IX.ordinal()] = false;
        matrix[LockType.X.ordinal()][LockType.S.ordinal()] = false;
        matrix[LockType.X.ordinal()][LockType.X.ordinal()] = false;
        matrix[LockType.X.ordinal()][LockType.IS.ordinal()] = false;
        matrix[LockType.X.ordinal()][LockType.IX.ordinal()] = false;
        matrix[LockType.IS.ordinal()][LockType.S.ordinal()] = true;
        matrix[LockType.IS.ordinal()][LockType.X.ordinal()] = false;
        matrix[LockType.IS.ordinal()][LockType.IS.ordinal()] = true;
        matrix[LockType.IS.ordinal()][LockType.IX.ordinal()] = true;
        matrix[LockType.IX.ordinal()][LockType.S.ordinal()] = false;
        matrix[LockType.IX.ordinal()][LockType.X.ordinal()] = false;
        matrix[LockType.IX.ordinal()][LockType.IS.ordinal()] = true;
        matrix[LockType.IX.ordinal()][LockType.IX.ordinal()] = true;
    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue.
     * @param transaction that is requesting the lock
     * @param resource that the transaction wants
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, Resource resource, LockType lockType)
            throws IllegalArgumentException {
        // HW5: To do
        if(holds(transaction, resource, lockType) || transaction.getStatus().equals(Transaction.Status.Waiting)){
            throw new IllegalArgumentException();
        }


        if(!this.resourceToLock.containsKey(resource)){
            resourceToLock.put(resource, new ResourceLock());
        }
        ResourceLock rl = resourceToLock.get(resource);

        for(Request req : rl.lockOwners){
            if(req.transaction.equals(transaction)){
                if(req.lockType.equals(LockType.X) && lockType.equals(LockType.S)){
                    throw new IllegalArgumentException();
                }
                if(req.lockType.equals(LockType.IX) && lockType.equals(LockType.IS)){
                    throw new IllegalArgumentException();
                }
            }
        }
        if(lockType.equals(LockType.IS) || lockType.equals(LockType.IX)){
            if(resource.getResourceType().equals(Resource.ResourceType.PAGE)){
                throw new IllegalArgumentException();
            }
        }
        if( resource.getResourceType().equals(Resource.ResourceType.PAGE) ){
            if(lockType.equals(LockType.S) && !holds(transaction, ((Page)resource).getTable(), LockType.IS )){
                throw new IllegalArgumentException();
            }
            if(lockType.equals(LockType.X) && !holds(transaction,((Page)resource).getTable(), LockType.IX)){
                throw new IllegalArgumentException();
            }
        }

        if(compatible(resource, transaction, lockType)){
            rl.lockOwners.add(new Request(transaction, lockType));
        }else{
            rl.requestersQueue.add(new Request(transaction, lockType));
            transaction.sleep();
        }
        return;
    }

    /**
     * Checks whether the a transaction is compatible to get the desired lock on the given resource
     * @param resource the resource we are looking it
     * @param transaction the transaction requesting a lock
     * @param lockType the type of lock the transaction is request
     * @return true if the transaction can get the lock, false if it has to wait
     */
    private boolean compatible(Resource resource, Transaction transaction, LockType lockType) {
        // HW5: To do
        if(!resourceToLock.containsKey(resource)){
            return true;
        }
        ResourceLock rLock = resourceToLock.get(resource);
        for(Request req : rLock.lockOwners){
            if(matrix[req.lockType.ordinal()][lockType.ordinal()] == false){
                return false;
            }
        }

        return true;
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param resource of Resource being released
     */
    public void release(Transaction transaction, Resource resource) throws IllegalArgumentException{
        // HW5: To do
        if(transaction.getStatus().equals(Transaction.Status.Waiting)){
            throw new IllegalArgumentException();
        }
        if(!resourceToLock.containsKey(resource)){
            return;
        }
        ResourceLock resLock = resourceToLock.get(resource);
        Request findReq = null;
        for(Request req : resLock.lockOwners){
            if(req.transaction.equals(transaction)){
                findReq = req;
                break;
            }
        }
        if(findReq == null){
            throw new IllegalArgumentException();
        }

        if(resource.getResourceType().equals(Resource.ResourceType.TABLE)){
            for(Resource res : ((Table)resource).getPages() ){
                if(holds(transaction, res)){
                    throw new IllegalArgumentException();
                }
            }
        }

        resLock.lockOwners.remove(findReq);
        promote(resource);
        return;
    }

    /**
     * This method will grant mutually compatible lock requests for the resource
     * from the FIFO queue.
     * @param resource of locked Resource
     */
     private void promote(Resource resource) {
         // HW5: To do
         ResourceLock resLock = resourceToLock.get(resource);
         while(resLock.requestersQueue.size()!=0){
             Request req = resLock.requestersQueue.pollFirst();
             if(compatible(resource, req.transaction, req.lockType)){
                 req.transaction.wake();
                 acquire(req.transaction,resource, req.lockType);

                 break;
             }else{
                 resLock.requestersQueue.add(req);
             }
         }
         return;
     }

     public boolean holds(Transaction transaction, Resource resource){
         if(!resourceToLock.containsKey(resource)){
             return false;
         }
         ResourceLock reqLock = resourceToLock.get(resource);
         for(Request req : reqLock.lockOwners){
             if(req.transaction.equals(transaction)){
                 return true;
             }
         }
         return false;
     }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the resource.
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, Resource resource, LockType lockType) {
        // HW5: To do
        if(!resourceToLock.containsKey(resource)){
            return false;
        }
        ResourceLock reqLock = resourceToLock.get(resource);
        for(Request req : reqLock.lockOwners){
            if(req.transaction.equals(transaction)){
                if(req.lockType.equals(lockType)){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Contains all information about the lock for a specific resource. This
     * information includes lock owner(s), and lock requester(s).
     */
    private class ResourceLock {
        private ArrayList<Request> lockOwners;
        private LinkedList<Request> requestersQueue;

        public ResourceLock() {
            this.lockOwners = new ArrayList<Request>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requester queue for a specific resource
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }

        @Override
        public String toString() {
            return String.format(
                    "Request(transaction=%s, lockType=%s)",
                    transaction, lockType);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof Request) {
                Request otherRequest  = (Request) o;
                return otherRequest.transaction.equals(this.transaction) && otherRequest.lockType.equals(this.lockType);
            } else {
                return false;
            }
        }
    }
}
