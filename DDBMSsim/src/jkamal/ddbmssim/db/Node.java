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
	private Set<Integer> node_partitions;	
	private double node_load;
	private int node_inflow;
	private int node_outflow;	
	private int node_total_data;
	
	public Node(int id) {
		this.setNode_id(id);
		this.setNode_label("N"+id);		
		this.setNode_partitions(new TreeSet<Integer>());
		this.setNode_load(0.0d);
		this.setNode_inflow(0);
		this.setNode_outflow(0);
		this.setNode_total_data(0);
	}
	
	public Node(Node node){
		this.setNode_id(node.getNode_id());
		this.setNode_label(node.getNode_label());
		
		TreeSet<Integer> clone_partitions = new TreeSet<Integer>();
		for(Integer p : node.getNode_partitions())
			clone_partitions.add(p);
		this.setNode_partitions(clone_partitions);
		
		this.setNode_load(node.getNode_load());
		this.setNode_inflow(node.getNode_inflow());
		this.setNode_outflow(node.getNode_outflow());
		this.setNode_total_data(node.getNode_total_data());
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

	public double getNode_load() {
		return node_load;
	}

	public void setNode_load(double node_size) {
		this.node_load = node_size;
	}

	public Set<Integer> getNode_partitions() {
		return node_partitions;
	}

	public void setNode_partitions(Set<Integer> node_partitions) {
		this.node_partitions = node_partitions;
	}
	
	public int getNode_inflow() {
		return node_inflow;
	}

	public void setNode_inflow(int node_inflow) {
		this.node_inflow = node_inflow;
	}

	public int getNode_outflow() {
		return node_outflow;
	}

	public void setNode_outflow(int node_outflow) {
		this.node_outflow = node_outflow;
	}

	public int getNode_total_data() {
		return node_total_data;
	}

	public void setNode_total_data(int node_total_data) {
		this.node_total_data = node_total_data;
	}
	
	public void incNode_totalData(int val){		
		this.setNode_total_data((this.getNode_total_data() + val));
	}
	
	public void decNode_totalData(int val){		
		this.setNode_total_data((this.getNode_total_data() - val));
	}
	
	public void incNode_totalData(){		
		int val = this.getNode_total_data();
		this.setNode_total_data(++val);
	}
	
	public void decNode_totalData(){
		int val = this.getNode_total_data();
		this.setNode_total_data(--val);
	}
		
	public void incNode_inflow(int val){		
		this.setNode_inflow((this.getNode_inflow() + val));
	}
	
	public void decNode_inflow(int val){		
		this.setNode_inflow((this.getNode_inflow() - val));
	}
	
	public void incNode_inflow(){		
		int val = this.getNode_inflow();
		this.setNode_inflow(++val);
	}
	
	public void decNode_inflow(){
		int val = this.getNode_inflow();
		this.setNode_inflow(--val);
	}
	
	public void incNode_outflow(int val){		
		this.setNode_outflow((this.getNode_outflow() + val));
	}
	
	public void decNode_outflow(int val){		
		this.setNode_outflow((this.getNode_outflow() - val));
	}
	
	public void incNode_outflow(){		
		int val = this.getNode_outflow();
		this.setNode_outflow(++val);
	}
	
	public void decNode_outflow(){
		int val = this.getNode_outflow();
		this.setNode_outflow(--val);
	}
	
	@Override
	public String toString() {
		return (this.getNode_label()+"(#P["+this.getNode_partitions().size()+"])");
	}

	@Override
	public int compareTo(Node node) {		
		int compare = ((int)this.node_id < (int)node.node_id) ? -1: ((int)this.node_id > (int)node.node_id) ? 1:0;
		return compare;
	}
}