package kvpaxos;
import paxos.Paxos;
import paxos.State;
// You are allowed to call Paxos.Status to check if agreement was made.
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.locks.ReentrantLock;

public class Server implements KVPaxosRMI {

    ReentrantLock mutex;
    Registry registry;
    Paxos px;
    int me;

    String[] servers;
    int[] ports;
    KVPaxosRMI stub;

    // Your definitions here
    ConcurrentHashMap<String, Integer> kvMap;
    ConcurrentHashMap<Integer, Op> paxosLog;
    AtomicInteger seqHelper;


    public Server(String[] servers, int[] ports, int me){
        this.me = me;
        this.servers = servers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.px = new Paxos(me, servers, ports);

        // Your initialization code here
        this.kvMap = new ConcurrentHashMap<String, Integer>();
        this.paxosLog = new ConcurrentHashMap<Integer, Op>();
        this.seqHelper = new AtomicInteger(0);


        try{
            System.setProperty("java.rmi.server.hostname", this.servers[this.me]);
            registry = LocateRegistry.getRegistry(this.ports[this.me]);
            stub = (KVPaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("KVPaxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    // RMI handlers
    public Response Get(Request req){
        // Your code here
    	Op reqOperation = req.operation;
    	int seq = getNextSeqNo(reqOperation);
    	px.Start(seq, reqOperation);
    	Op resp = wait(seq);
    	mutex.lock();
    	paxosLog.put(seq, resp);
    	mutex.unlock();
    	checkLog();
    	if(resp.equals(reqOperation)) {
    		Op retOp = new Op("Get", reqOperation.ClientSeq, reqOperation.key, kvMap.get(reqOperation.key));
    		Response r = new Response(retOp);
    		return r;
    	}
    	else 
			return new Response(null);
        // else {
        //     Op retOp = new Op("Get", reqOperation.ClientSeq, reqOperation.key, null);
        //     Response r = new Response(retOp);
        //     return r
        // }
    }

    public Response Put(Request req){
        // Your code here
        Op reqOperation = req.operation;
    	int seq = getNextSeqNo(reqOperation);
    	if(seq < 0) {
            return new Response(null);
        }
    	px.Start(seq, reqOperation);
    	Op resp = wait(seq);
    	mutex.lock();
    	paxosLog.put(seq, resp);
    	mutex.unlock();
    	return new Response(reqOperation);

    }

    public int getNextSeqNo(Op req) {
        int seq = seqHelper.get();
    	Paxos.retStatus status = px.Status(seq);
    	while(status != null) {
    		if(status.state == State.Decided) {
    			Op value = (Op)status.v;
        		if(value.ClientSeq == req.ClientSeq && 
						value.op.equals(req.op) && 
						value.key.equals(req.key)) {
                    return -1;
                }
    			paxosLog.put(seq, value);
				seq = seqHelper.incrementAndGet();
    			status = px.Status(seq);
    		}
    		else {
				seq = seqHelper.getAndIncrement();
    			break;
    		}
    	}
    	return seq;
    }

	public void checkLog() {

		for(int seq : paxosLog.keySet()) {
			Op operation = paxosLog.get(seq);

			if(operation.op.equals("Put")){

				mutex.lock();
				kvMap.put(operation.key, operation.value);
				mutex.unlock();
			}
			px.Done(seq);
		}
		paxosLog.clear();

	}
    
    public Op wait(int seq) {
    	int to = 10;
    	while(true) {
    		Paxos.retStatus ret = px.Status(seq);
    		if(ret.state == State.Decided) {
    			return Op.class.cast(ret.v);
    		}
    		try {
    			Thread.sleep(to);
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    		if(to < 1000) {
    			to = 2 * to;
    		}
    	}
    }



}
