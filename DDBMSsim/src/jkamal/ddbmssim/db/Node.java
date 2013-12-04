/**
 * @author Joarder Kamal
 * 
 * Node represents physical machine
 */

package jkamal.ddbmssim.db;

import java.util.Set;
import java.util.TreeSet;

public class Node implements Comparable<Node> {		
	private int node_id;
	private String node_label;				
	private Set<Partition> node_partitions;
	private static int NODE_MAX_CAPACITY = 10; // 1000GB = 1TB -- 1000 Partitions // 1TB Data (in Size) Or, equivalently 1000 Partitions can be stored in a single node.
	
	public Node(int id, int label) {
		this.setNode_id(id);
		this.setNode_label("N"+label);		
		this.setNode_partitions(new TreeSet<Partition>());
		//Node.setNODE_MAX_CAPACITY(10); 		
	}

	public int getNode_id() {
		return node_id;
	}

	public void setNode_id(int node_id) {
		this.node_id = node_id;
	}

	public String getNode_label() {
		return node_label;
	}

	public void setNode_label(String node_label) {
		this.node_label = node_label;
	}

	public static long getNODE_MAX_CAPACITY() {
		return NODE_MAX_CAPACITY;
	}

	public static void setNODE_MAX_CAPACITY(int max_capacity) {
		NODE_MAX_CAPACITY = max_capacity;
	}

	public Set<Partition> getNode_partitions() {
		return node_partitions;
	}

	public void setNode_partitions(Set<Partition> node_partitions) {
		this.node_partitions = node_partitions;
	}
	
	@Override
	public int compareTo(Node node) {		
		int compare = ((int)this.node_id < (int)node.node_id) ? -1: ((int)this.node_id > (int)node.node_id) ? 1:0;
		return compare;
	}
}