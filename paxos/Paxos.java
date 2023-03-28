package paxos;
import java.io.Serializable;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import static paxos.State.*;

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable{
    ReentrantLock lock;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // Your data here
    private Map<Integer, PaxosInstance> seq2paxIns;
    private List<Integer> done;
    private Queue<seqValPair> seq2val = new ConcurrentLinkedDeque<>();
    private int propHelper;
    private int seq;
    private Serializable val;


    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports){
        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.lock = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);

        // Your initialization code here
        this.seq2paxIns = new ConcurrentHashMap<>();
        this.done = new ArrayList<>(Arrays.asList(new Integer[peers.length]));
        this.propHelper = this.me - this.peers.length;

        for (int i = 0; i < this.peers.length; i++) {
            this.done.set(i,-1);
        }
        // register peers, do not modify this part
        try{
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public class PaxosInstance {
        int n_p;
        int n_a;
        Serializable v_a;
        State state;

        public PaxosInstance(){
            this.n_p = -1;
            this.n_a = -1;
            this.v_a = null;
            this.state = State.Pending;
        }

    }
    public class seqValPair{
        public int seq;
        public Serializable val;
        public seqValPair(int seq, Serializable val){
            this.seq=seq;
            this.val=val;
        }
    }

    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time resp and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;
        PaxosRMI stub;
        try{
            Registry registry=LocateRegistry.getRegistry(this.ports[id]);
            stub=(PaxosRMI) registry.lookup("Paxos");
            if(rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if(rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if(rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }

    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find resp if/when agreement
     * is reached.
     */
    public synchronized void Start(int seq, Serializable value){
        this.lock.lock();
        this.seq2val.add(new seqValPair(seq,value));
        new Thread(this).start();
        this.lock.unlock();
    }

    @Override
    public void run(){
        seqValPair svp = this.seq2val.poll();
        int seq = svp.seq;
        Serializable val = svp.val;

        proposer(seq, val);

    }

    public void proposer(int seq, Serializable val){
        this.seq2paxIns.put(seq, new PaxosInstance());
        while(!this.isDead()){
            int n = this.peers.length + this.propHelper;
            this.propHelper = this.peers.length + this.propHelper;
            int responseCount = 0;
            int highest_n_a = -1;
            Serializable highest_v_a = null;
            Request prepare = new Request("Prepare", seq, n, val);
            for(int i = 0; i < this.ports.length; i++){
                Response resp;
                if(i != this.me) {
                    resp = Call("Prepare", prepare, i);
                }
                else {
                    resp = this.Prepare(prepare);
                }
                if(resp != null && resp.type.equals("Promise")){
                    responseCount++;
                    if(resp.n_a > highest_n_a){
                        highest_n_a = resp.n_a;
                        highest_v_a = resp.v_a;
                    }
                }
            }
//            if(responseCount < 1 + this.peers.length / 2)
//                continue;
            if(responseCount >= (this.peers.length / 2) + 1){
                responseCount = 0;
                Serializable vprime;
                if(highest_v_a!=null){
                    vprime = highest_v_a;
                }
                else{
                    vprime = val;
                }
                Request acceptRequest = new Request("Accept", seq, n, vprime);
                for(int i = 0; i < this.ports.length; i++) {
                    Response resp;
                    if(i != this.me) {
                        resp = Call("Accept", acceptRequest, i);
                    }
                    else {
                        resp = this.Accept(acceptRequest);
                    }
                    if (resp != null && resp.type.equals("Accept")) {
                        responseCount++;
                    }
                }
                if(responseCount >= (this.peers.length / 2) + 1){
                    for(int i = 0; i < this.ports.length; i++) {
                        Response resp;
                        if (i != me) {
                            resp = Call("Decide", acceptRequest, i);
                        }
                        else {
                            resp = this.Decide(acceptRequest);
                        }
                        if(resp != null) {
                            this.done.set(i, resp.highestDone);
                        }
                    }
                    return;
                }
            }

        }
    }

    // RMI handler
    public Response Prepare(Request req){
        this.lock.lock();
        if(!this.seq2paxIns.containsKey(req.seq)) {
            PaxosInstance paxosInstance = new PaxosInstance();
            Response resp = new Response("Promise", paxosInstance.n_a, paxosInstance.v_a, -1);
            this.lock.unlock();
            return resp;
        }
        if(this.seq2paxIns.containsKey(req.seq)) {
            PaxosInstance paxosInstance = this.seq2paxIns.get(req.seq);
            if(paxosInstance.n_p < req.n_p){
                paxosInstance.n_p = req.n_p;
                this.seq2paxIns.put(req.seq, paxosInstance);
                Response resp = new Response("Promise", paxosInstance.n_a, paxosInstance.v_a, -1);
                this.lock.unlock();
                return resp;
            }
        }
        this.lock.unlock();
        return new Response("Ignore");
    }

    public Response Accept(Request req){
        this.lock.lock();
        if(!this.seq2paxIns.containsKey(req.seq)) {
            PaxosInstance paxosInstance = new PaxosInstance();
            Response resp = new Response("Accept", paxosInstance.n_a, paxosInstance.v_a, -1);
            this.lock.unlock();
            return resp;
        }
        if(this.seq2paxIns.containsKey(req.seq)) {
            PaxosInstance paxosInstance = this.seq2paxIns.get(req.seq);
            if(paxosInstance.n_p <= req.n_p){
                paxosInstance.n_p = req.n_p;
                paxosInstance.n_a = req.n_p;
                paxosInstance.v_a = req.v_a;
                this.seq2paxIns.put(req.seq, paxosInstance);
                Response resp = new Response("Accept", paxosInstance.n_a, paxosInstance.v_a, -1);
                this.lock.unlock();
                return resp;
            }
        }
        this.lock.unlock();
        return new Response("DontAccept");
    }

    public Response Decide(Request req){
        this.lock.lock();
        if(!this.seq2paxIns.containsKey(req.seq)) {
            this.seq2paxIns.put(req.seq, new PaxosInstance());
        }
        PaxosInstance paxosInstance = this.seq2paxIns.get(req.seq);
        paxosInstance.state = Decided;
        paxosInstance.n_a = req.n_p;
        paxosInstance.v_a = req.v_a;
        int highestDone = this.done.get(this.me);
        Response resp = new Response("DecideResponse", paxosInstance.n_a, paxosInstance.v_a, highestDone);
        this.lock.unlock();
        return resp;
    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     * 
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        this.lock.lock();
        if(seq > this.done.get(this.me)) {
            this.done.set(this.me, seq);
        }
        this.lock.unlock();
    }

    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        int max = Integer.MIN_VALUE;
        this.lock.lock();
        for(int key : this.seq2paxIns.keySet()){
            if(key>max){
                max = key;
            }
        }
        this.lock.unlock();
        return max;
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().

     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.

     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.

     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min(){
        this.lock.lock();
        int min = Integer.MAX_VALUE;
        for(int seq: this.done){
            if(seq < min)
                min = seq;
        }
        for(Map.Entry<Integer,PaxosInstance> entry : this.seq2paxIns.entrySet())
            if(entry.getKey() < min && entry.getValue().state == Decided)
                this.seq2paxIns.remove(entry.getKey());
        this.lock.unlock();
        return min + 1;
    }

    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq){
        this.lock.lock();
        if(seq < this.Min()){
            this.lock.unlock();
            return new retStatus(Forgotten,null);
        }
        PaxosInstance paxosInstance;
        if(this.seq2paxIns.containsKey((seq))) {
            paxosInstance = this.seq2paxIns.get(seq);
        }
        else {
            paxosInstance = new PaxosInstance();
        }
        retStatus resp = new retStatus(paxosInstance.state, paxosInstance.v_a);
        this.lock.unlock();
        return resp;
    }

    /**
     * helper class for Status() return
     */
    public class retStatus{
        public State state;
        public Object v;
        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }
    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }
    public boolean isunreliable(){
        return this.unreliable.get();
    }

}