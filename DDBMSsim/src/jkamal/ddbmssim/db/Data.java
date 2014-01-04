/**
 * @author Joarder Kamal
 * 
 * The term "Data" has been considered as a row for a Relation DBMS and a single data item for a Key-Value Data Store
 */

package jkamal.ddbmssim.db;

import java.util.Set;
import java.util.TreeSet;

public class Data implements Comparable<Data> {
	private int data_id;
	private String data_label;
	private String data_value;
	private float data_size;
	private int data_frequency;
	private int data_weight;
	private String data_pk;
	private boolean data_isMoveable;
	
	// Workload Attributes
	private int data_ranking;
	private double data_cumulative_probability;
	private double data_normalised_cumulative_probability;
	
	// Transaction Attributes
	private Set<Integer> data_transactions_involved;
	
	// HyperGraph and Graph Partitioning Attributes
	private int data_hmetis_cluster_id;
	private int data_chmetis_cluster_id;
	private int data_metis_cluster_id;
	private int data_shadow_id;
	private boolean data_hasShadowId;
	private int data_virtual_node_id;
	
	// Partition Attributes		
	private int data_partition_id;			// Currently residing (Roaming/Home) Partition Id
	private int data_home_partition_id;		// Original Home Partition Id	
	// Node Attributes
	private int data_node_id;				// Currently residing (Roaming/Home) Node Id
	private int data_home_node_id;			// Original Home Node Id
	// Roaming Attributes
	private boolean data_isRoaming;							
	
	// Default Constructor
	public Data(int id, int pid, int nid, boolean roaming) {
		this.setData_id(id); // default data id = -1 means undefined.
		this.setData_label("d"+id);
		this.setData_value(this.getData_label());
		this.setData_size(1); // 1.0 = 1 MegaBytes
		this.setData_frequency(0);
		this.setData_weight(0);
		this.setData_pk("pk");
		this.setData_isMoveable(false);	
		
		this.setData_ranking(0);
		this.setData_cumulativeProbability(0.0);
		this.setData_normalisedCumulativeProbability(0.0);
				
		this.setData_transactions_involved(new TreeSet<Integer>());
		
		this.setData_hmetisClusterId(-1);
		this.setData_chmetisClusterId(-1);
		this.setData_metisClusterId(-1);
		this.setData_shadowId(-1);
		this.setData_hasShadowId(false);
		this.setData_virtual_node_id(-1);
		
		this.setData_partitionId(pid); // default partition id = -1 means undefined.
		this.setData_homePartitionId(pid);
		
		this.setData_nodeId(nid);
		this.setData_homeNodeId(nid);
			
		this.setData_isRoaming(roaming);
	}
	
	// Copy Constructor
	public Data(Data data) {
		this.setData_id(data.getData_id());
		this.setData_label(data.getData_label());
		this.setData_value(data.getData_value());
		this.setData_size(data.getData_size());
		this.setData_frequency(data.getData_frequency());
		this.setData_weight(data.getData_weight());
		this.setData_pk(data.getData_pk());
		this.setData_isMoveable(data.isData_isMoveable());		
		
		this.setData_ranking(data.getData_ranking());
		this.setData_cumulativeProbability(data.getData_cumulativeProbability());
		this.setData_normalisedCumulativeProbability(data.getData_normalisedCumulativeProbability());
		
		Set<Integer> clone_data_transaction_involved = new TreeSet<Integer>();
		for(Integer tr_id : data.getData_transactions_involved()) {
			clone_data_transaction_involved.add(tr_id);
		}
		this.setData_transactions_involved(clone_data_transaction_involved);
		
		this.setData_hmetisClusterId(data.getData_hmetisClusterId());
		this.setData_chmetisClusterId(data.getData_chmetisClusterId());
		this.setData_shadowId(data.getData_shadowId());
		this.setData_hasShadowId(data.isData_hasShadowId());
		this.setData_virtual_node_id(data.getData_virtual_node_id());
				
		this.setData_partitionId(data.getData_partitionId());				
		this.setData_homePartitionId(data.getData_homePartitionId());
		
		this.setData_nodeId(data.getData_nodeId());
		this.setData_homeNodeId(data.getData_homeNodeId());

		this.setData_isRoaming(data.isData_isRoaming());
	}

	public int getData_id() {
		return data_id;
	}

	public void setData_id(int data_id) {
		this.data_id = data_id;
	}

	public String getData_label() {
		return data_label;
	}

	public void setData_label(String data_label) {
		this.data_label = data_label;
	}

	public String getData_value() {
		return data_value;
	}

	public int getData_frequency() {
		return data_frequency;
	}

	public void setData_frequency(int data_frequency) {
		this.data_frequency = data_frequency;
	}

	public int getData_weight() {
		return data_weight;
	}

	public void setData_weight(int data_weight) {
		this.data_weight = data_weight;
	}
	
	public void setData_value(String data_value) {
		this.data_value = data_value;
	}

	public float getData_size() {
		return data_size;
	}

	public void setData_size(float data_size) {
		this.data_size = data_size;
	}

	public String getData_pk() {
		return data_pk;
	}

	public void setData_pk(String data_pk) {
		this.data_pk = data_pk;
	}

	public int getData_ranking() {
		return data_ranking;
	}

	public void setData_ranking(int data_ranking) {
		this.data_ranking = data_ranking;
	}

	public double getData_cumulativeProbability() {
		return data_cumulative_probability;
	}

	public void setData_cumulativeProbability(double cumulative_probability) {
		this.data_cumulative_probability = cumulative_probability;
	}

	public double getData_normalisedCumulativeProbability() {
		return data_normalised_cumulative_probability;
	}

	public void setData_normalisedCumulativeProbability(double data_normalised_cdf) {
		this.data_normalised_cumulative_probability = data_normalised_cdf;
	}

	public Set<Integer> getData_transactions_involved() {
		return data_transactions_involved;
	}

	public void setData_transactions_involved(
			Set<Integer> data_transaction_involved) {
		this.data_transactions_involved = data_transaction_involved;
	}

	public boolean isData_isRoaming() {
		return data_isRoaming;
	}

	public void setData_isRoaming(boolean data_isRoaming) {
		this.data_isRoaming = data_isRoaming;
	}

	public int getData_partitionId() {
		return data_partition_id;
	}

	public void setData_partitionId(int data_partition_id) {
		this.data_partition_id = data_partition_id;
	}

	public int getData_homePartitionId() {
		return data_home_partition_id;
	}

	public void setData_homePartitionId(int data_home_partition_id) {
		this.data_home_partition_id = data_home_partition_id;
	}

	public int getData_nodeId() {
		return data_node_id;
	}

	public void setData_nodeId(int data_node_id) {
		this.data_node_id = data_node_id;
	}

	public int getData_homeNodeId() {
		return data_home_node_id;
	}

	public void setData_homeNodeId(int data_home_node_id) {
		this.data_home_node_id = data_home_node_id;
	}

	public int getData_shadowId() {
		return data_shadow_id;
	}

	public void setData_shadowId(int data_shadow_id) {
		this.data_shadow_id = data_shadow_id;
	}

	public boolean isData_hasShadowId() {
		return data_hasShadowId;
	}

	public void setData_hasShadowId(boolean data_hasShadowId) {
		this.data_hasShadowId = data_hasShadowId;
	}
	
	public int getData_virtual_node_id() {
		return data_virtual_node_id;
	}

	public void setData_virtual_node_id(int data_virtual_node_id) {
		this.data_virtual_node_id = data_virtual_node_id;
	}

	public int getData_hmetisClusterId() {
		return data_hmetis_cluster_id;
	}

	public void setData_hmetisClusterId(int data_hmetis_cluster_id) {
		this.data_hmetis_cluster_id = data_hmetis_cluster_id;
	}

	public int getData_chmetisClusterId() {
		return data_chmetis_cluster_id;
	}

	public void setData_chmetisClusterId(int data_chmetis_cluster_id) {
		this.data_chmetis_cluster_id = data_chmetis_cluster_id;
	}

	public int getData_metisClusterId() {
		return data_metis_cluster_id;
	}

	public void setData_metisClusterId(int data_metis_cluster_id) {
		this.data_metis_cluster_id = data_metis_cluster_id;
	}

	public boolean isData_isMoveable() {
		return data_isMoveable;
	}

	public void setData_isMoveable(boolean data_isMoveable) {
		this.data_isMoveable = data_isMoveable;
	}
	
	public void incData_frequency(int data_frequency) {	
		this.setData_frequency(++data_frequency);
	}
	
	public void incData_frequency() {
		int data_frequency = this.getData_frequency();
		this.setData_frequency(++data_frequency);
	}
	
	public void calculateData_weight() {
		this.setData_weight(this.getData_frequency() * this.getData_ranking());
	}

	@Override
	public String toString() {		
		if(this.isData_isRoaming())
			return (this.data_label+"|"+this.data_pk+"("+this.getData_weight()+"/"+this.getData_ranking()+"/"+this.getData_frequency()
					+"|P"+this.data_partition_id+"("+this.data_home_partition_id
					+")/N"+this.data_node_id+"("+this.data_home_node_id+"))");// @C("+this.data_hmetis_cluster_id+") @h("+this.data_shadow_hmetis_id+")");
		else
			return (this.data_label+"|"+this.data_pk+"("+this.getData_weight()+"/"+this.getData_ranking()+"/"+this.getData_frequency()
					+"|P"+this.data_partition_id
					+"/N"+this.data_node_id+")");// @C("+this.data_hmetis_cluster_id+") //@h("+this.data_shadow_hmetis_id+")");
	}
		
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof Data)) {
			return false;
		}
		
		Data data = (Data) object;
		return this.getData_label().equals(data.getData_label());
	}

	@Override
	public int hashCode() {
		return this.getData_label().hashCode();
	}

	@Override
	public int compareTo(Data data) {		
		return (((int)this.getData_id() < (int)data.getData_id()) ? -1 : ((int)this.getData_id() > (int)data.getData_id()) ? 1:0);
		
	}
}