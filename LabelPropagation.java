import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LabelPropagation {
    
    private Vector<Node> nodeList;
    private Vector<Integer> nodeOrder;
    
    public LabelPropagation() {}
    
    public void readEdges(int numNodes, String file) throws IOException {
	BufferedReader br = new BufferedReader(new FileReader(file));
	
	nodeList = new Vector<Node>(numNodes);
	nodeOrder = new Vector<Integer>(numNodes);
	for (int i=0; i<=numNodes; i++) {
	    nodeList.add(new Node(i,i));
	    nodeOrder.add(Integer.valueOf(i));
	}
	System.out.println("Added " + numNodes + " nodes.");
	
	String line = br.readLine();
	while (line!=null) {
	    String[] parts = line.split("\t");
	    
	    int source = Integer.valueOf(parts[0]);
	    int target = Integer.valueOf(parts[1]);
	    
	    //System.out.println("Source is" + source);
	    //System.out.println("Target is" + target);
	    
	    nodeList.get(source).addNeighbor(target);
	    nodeList.get(target).addNeighbor(source);
	    line=br.readLine();
	}
	
	System.out.println("All edges read.");
	br.close();
    }
    
    public void writeMemberships(String file) throws IOException {
	
	System.out.println("Writing membership.");
	
	FileOutputStream fso = new FileOutputStream(file);
	OutputStreamWriter fileWriter = new OutputStreamWriter(fso,Charset.forName("UTF-8"));
	
	Node n;
	for (int i=0; i<nodeList.size(); i++) {
	    n=nodeList.get(i);
	    fileWriter.write(n.getId()+" "+n.getLabel()+"\n");
	}
	
	System.out.println("Membership list written.");
	
	fileWriter.close();
	fso.close();
    }
    
    
    public void readMemberships(String file) throws IOException {
	System.out.println("Reading memberships.");
	
	BufferedReader br = new BufferedReader(new FileReader(file));
	
	String line = br.readLine();
	while (line!=null) {
	    String[] parts = line.split(" ");
	    
	    int nodeId = Integer.valueOf(parts[0]);
	    int label = Integer.valueOf(parts[1]);
	    
	    nodeList.get(nodeId).setLabel(label);
	    
	    line=br.readLine();
	}
	
	System.out.println("Memberships loaded from file.");

	br.close();
	
    }
    
    
    public void writeMembershipsSmart(String file) throws IOException {
	
	System.out.println("Writing membership smart.");
	
	
	Map<Integer,Integer> labelMap = new HashMap<Integer,Integer>();
	int labelCount=0;
	for (int i=0; i<nodeList.size(); i++) {
	    int label = nodeList.get(i).getLabel();
	    Integer val =  labelMap.get(Integer.valueOf(label));
	    if (val==null) {
		labelCount++;
		labelMap.put(Integer.valueOf(label), Integer.valueOf(labelCount));
	    }
	}
	System.out.println("Found " + labelCount + " communities.");
	
	FileOutputStream fso = new FileOutputStream(file);
	OutputStreamWriter fileWriter = new OutputStreamWriter(fso,Charset.forName("UTF-8"));
	
	Node n;
	for (int i=0; i<nodeList.size(); i++) {
	    n=nodeList.get(i);
	    fileWriter.write(n.getId()+" "+labelMap.get(Integer.valueOf(n.getLabel())).intValue() +"\n");
	}
	
	System.out.println("Smart membership list written.");
	
	fileWriter.close();
	fso.close();
    }
    
    
    public void findCommunities(String basepath, int numThreads) throws InterruptedException, ExecutionException, IOException {
	
	/*memberships = new Vector<Integer>(nodeList.size());
	  for (int i=0; i<nodeList.size(); i++) {
	        memberships.set(i, Integer.valueOf(i));
		}*/
	
	ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);

	Vector<LabelPropagationWorker> workers = new Vector<LabelPropagationWorker>(numThreads);
	for (int j=1; j<=numThreads; j++) {
	    workers.add(new LabelPropagationWorker(nodeList));
	}
	
	int iter=0;
	int nodesChanged=100;
	while (nodesChanged>0) {
	    nodesChanged=0;
	    
	    System.out.println("Running " + (++iter) + " iteration at " + System.currentTimeMillis() + ".");
	    
	    Collections.shuffle(nodeOrder);//DO NOT SHUFFLE nodeList
	    
	    for (int i=0; i<nodeList.size(); i+=numThreads) {
		for (int j=0; j<numThreads; j++) {
		    if ((j+i)<nodeList.size()) {
			workers.get(j).setNodeToProcess(nodeOrder.get(i+j).intValue());
		    } else {
			workers.get(j).setNodeToProcess(-1);
		    }
		}
		List<Future<Boolean>> results = threadPool.invokeAll(workers);
		
		for (int j=0; j<results.size(); j++) {
		    Boolean r = results.get(j).get();
		    if (r!=null && r.booleanValue()==true) {
			nodesChanged++;
			if (nodesChanged==1) System.out.println("Another pass will be needed.");
			break;
		    }
		}
	    }
	    
	    
	    //Pass complete
	    if (basepath!=null) {
		//writeMemberships(basepath+"iter" + iter +"memberships.txt");
		System.out.println(nodesChanged + " nodes were changed in the last iteration.");
	    }
	    
	}
	
	System.out.println("Detection complete!");
	threadPool.shutdown();
	
    }
    


    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
	LabelPropagation lp = new LabelPropagation();
	
	int numNodes = 1971238; //Number of nodes in the network
	int numThreads= 8; //Number of threads to use
	
	long startTime = System.nanoTime();
	//input is "edgelist" format "id id" sorted by first id (ids are sequentially numbered 1 to numNodes inclusive)
	lp.readEdges(numNodes, "a.txt");
	lp.findCommunities("base_output_path",numThreads); //directory to save current list of communities to after each pass as well as final output files
	lp.writeMemberships("membership.txt");
	lp.writeMembershipsSmart("memberships_renumbered.txt");
	
	long estimatedTime = System.nanoTime() - startTime;
	System.out.println("Elapsed Time is "+estimatedTime);
    }
}
