/**
 * @author Joarder Kamal
 * 
 * A Database Server can contain one or more tenant Databases
 */

package jkamal.ddbmssim.db;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import jkamal.ddbmssim.main.DBMSSimulator;

public class DatabaseServer {
	private int dbs_id;
	private String dbs_name;
	private Set<Node> dbs_nodes;
	private Database dbs_tenants;
	
	public DatabaseServer(int id, String name, int nodes) {
		this.setDbs_id(id);
		this.setDbs_name(name);				
		this.setDbs_nodes(new TreeSet<Node>());		
		//this.setDbs_tenant();
		
		for(int i = 1; i <= nodes; i++)			
			this.getDbs_nodes().add(new Node(i));
	}
	
	public DatabaseServer(DatabaseServer dbs) {
		this.setDbs_id(dbs.getDbs_id());
		this.setDbs_name(dbs.getDbs_name());
		
		TreeSet<Node> clone_nodes = new TreeSet<Node>();
		for(Node node : dbs.getDbs_nodes()) {
			Node clone_node = new Node(node);
			clone_nodes.add(clone_node);
		}
		this.setDbs_nodes(clone_nodes);
				
		this.setDbs_tenant(new Database(dbs.getDbs_tenant()));		
	}

	public int getDbs_id() {
		return dbs_id;
	}

	public void setDbs_id(int dbs_id) {
		this.dbs_id = dbs_id;
	}

	public String getDbs_name() {
		return dbs_name;
	}

	public void setDbs_name(String dbs_name) {
		this.dbs_name = dbs_name;
	}

	public Set<Node> getDbs_nodes() {
		return dbs_nodes;
	}

	public void setDbs_nodes(Set<Node> dbs_nodes) {
		this.dbs_nodes = dbs_nodes;
	}
	
	public Node getDbs_node(int id) {
		Node node;
		Iterator<Node> iterator = this.getDbs_nodes().iterator();
		while(iterator.hasNext()) {
			node = iterator.next();
			if(node.getNode_id() == id)
				return node;
		}
		
		return null;
	}

	public Database getDbs_tenant() {
		return dbs_tenants;
	}

	public void setDbs_tenant(Database dbs_tenants) {
		this.dbs_tenants = dbs_tenants;
	}
	
	public void updateNodeLoad() {
		double combined_partition_load = 0.0d;
		double node_load = 0.0d;
		
		for(Node node : this.getDbs_nodes()) {
			combined_partition_load = 0.0d;
			for(int pid : node.getNode_partitions())				
				combined_partition_load += this.getDbs_tenant().getPartition(pid).getPartition_dataSet().size();
			
			node_load = ((double)combined_partition_load/(double)DBMSSimulator.NODE_MAX_CAPACITY)*100;
			node_load = (node_load/100)*100;
			
			node.setNode_load(node_load);
		}
	}
	
	public void show() {
		// DBS Details
		System.out.println("[OUT] Database Server Details===");
		System.out.println("      Database Server: "+this.getDbs_name());
		System.out.println("      Number of Nodes: "+this.getDbs_nodes().size());
		
		Set<Integer> overloadedPartition = new TreeSet<Integer>();
		int comma = -1;
		
		// Node Details
		System.out.println("[OUT] Node Details===");
		for(Node node : this.getDbs_nodes()) {						
			System.out.println("    --"+node.toString()
			+" | Load "
			+node.getNode_load()+"%"+"[In:"+node.getNode_inflow()+"][Out:"+node.getNode_outflow()+"]");
			
			for(int partition_id : node.getNode_partitions()) {				
				Partition partition = this.getDbs_tenant().getPartition(partition_id);
				
				System.out.println("    ----"+partition.toString());
				//partition.show();
				
				if(partition.isPartition_overloaded())
					overloadedPartition.add(partition.getPartition_globalId());	
			}
			
		}		
		
		if(overloadedPartition.size() != 0) {			
			System.out.print("[ALM] Overloaded Partition: ");
			
			comma = overloadedPartition.size();
			
			for(Integer pid : overloadedPartition) {
				System.out.print("P"+pid);
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;
			}
			
			System.out.print("\n");
		}
	}
}