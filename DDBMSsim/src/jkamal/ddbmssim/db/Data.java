/**
 * @author Joarder Kamal
 * 
 * The term "Data" has been considered as a row for a Relation DBMS and a single data item for a Key-Value Data Store
 */

package jkamal.ddbmssim.db;

public class Data implements Comparable<Data> {
	private int data_id;
	private String data_label;
	private String data_value;
	private double data_size;
	//private int data_frequency;
	private int data_weight;
	private int data_pk;
	private boolean data_isMoveable;

	private String data_primary_key;
	private String data_foreign_key;
	
	// Workload Attributes
	private double data_zipf_probability;
	private double data_cumulative_zipf_probability;
	private double data_normalised_cumulative_zipf_probability;
	private double data_popularity;
	private double data_cumulative_beta_probability;
	
	// HyperGraph and Graph Partitioning Attributes
	private int data_hmetis_cluster_id;
	private int data_chmetis_cluster_id;
	private int data_metis_cluster_id;
	private int data_shadow_id;
	private boolean data_hasShadowId;
	private int data_virtual_node_id;
	
	// Table Attributes
	private int data_table_id;
	
	// Partition Attributes
	private int data_local_partition_id;
	private int data_global_partition_id;			// Currently residing (Roaming/Home) Partition Id
	private int data_home_partition_id;		// Original Home Partition Id	
	// Node Attributes
	private int data_node_id;				// Currently residing (Roaming/Home) Node Id
	private int data_home_node_id;			// Original Home Node Id
	// Roaming Attributes
	private boolean data_isRoaming;							
	
	// Default Constructor
	public Data(int id, String pk, String fk, int lpid, int gpid, int tid, int nid, boolean roaming) {
		this.setData_id(id); // default data id = -1 means undefined.
		this.setData_label("d"+id);
		this.setData_value(this.getData_label());
		this.setData_size(0); // Values in MegaBytes
		//this.setData_frequency(0);
		this.setData_weight(1);
		this.setData_pk(-1);
		this.setData_isMoveable(false);	
		
		this.setData_primary_key(pk);
		this.setData_foreign_key(fk);
		
		this.setData_zipfProbability(0.0);
		this.setData_cumulativeZipfProbability(0.0);
		this.setData_normalisedCumulativeZipfProbability(0.0);
				
		//this.setData_transactions_involved(new TreeSet<Integer>());
		
		this.setData_hmetisClusterId(-1);
		this.setData_chmetisClusterId(-1);
		this.setData_metisClusterId(-1);
		this.setData_shadowId(-1);
		this.setData_hasShadowId(false);
		this.setData_virtual_node_id(-1);
		
		this.setData_table_id(tid);
		
		this.setData_localPartitionId(lpid);
		this.setData_globalPartitionId(gpid); // default partition id = -1 means undefined.
		this.setData_homePartitionId(gpid);
		
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
		//this.setData_frequency(data.getData_frequency());
		this.setData_weight(data.getData_weight());
		this.setData_pk(data.getData_pk());
		this.setData_isMoveable(data.isData_isMoveable());		
		
		this.setData_zipfProbability(data.getData_zipfProbability());
		this.setData_cumulativeZipfProbability(data.getData_cumulativeZipfProbability());
		this.setData_normalisedCumulativeZipfProbability(data.getData_normalisedCumulativeZipfProbability());		
		
		this.setData_hmetisClusterId(data.getData_hmetisClusterId());
		this.setData_chmetisClusterId(data.getData_chmetisClusterId());
		this.setData_shadowId(data.getData_shadowId());
		this.setData_hasShadowId(data.isData_hasShadowId());
		this.setData_virtual_node_id(data.getData_virtual_node_id());
				
		this.setData_table_id(data.getData_table_id());
		
		this.setData_localPartitionId(data.getData_localPartitionId());
		this.setData_globalPartitionId(data.getData_globalPartitionId());				
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

	public int getData_weight() {
		return data_weight;
	}

	public void setData_weight(int data_weight) {
		this.data_weight = data_weight;
	}
	
	public void setData_value(String data_value) {
		this.data_value = data_value;
	}

	public double getData_size() {
		return data_size;
	}

	public void setData_size(double data_size) {
		this.data_size = data_size;
	}

	public int getData_pk() {
		return data_pk;
	}

	public void setData_pk(int data_pk) {
		this.data_pk = data_pk;
	}

	public String getData_primary_key() {
		return data_primary_key;
	}

	public void setData_primary_key(String data_primary_key) {
		this.data_primary_key = data_primary_key;
	}

	public String getData_foreign_key() {
		return data_foreign_key;
	}

	public void setData_foreign_key(String data_foreign_key) {
		this.data_foreign_key = data_foreign_key;
	}

	public double getData_zipfProbability() {
		return data_zipf_probability;
	}

	public void setData_zipfProbability(double data_zipf_probability) {
		this.data_zipf_probability = data_zipf_probability;
	}

	public double getData_cumulativeZipfProbability() {
		return data_cumulative_zipf_probability;
	}

	public void setData_cumulativeZipfProbability(double cumulative_probability) {
		this.data_cumulative_zipf_probability = cumulative_probability;
	}

	public double getData_normalisedCumulativeZipfProbability() {
		return data_normalised_cumulative_zipf_probability;
	}

	public void setData_normalisedCumulativeZipfProbability(double data_normalised_cdf) {
		this.data_normalised_cumulative_zipf_probability = data_normalised_cdf;
	}

	/*public Set<Integer> getData_transactions_involved() {
		return data_transactions_involved;
	}

	public void setData_transactions_involved(
			Set<Integer> data_transaction_involved) {
		this.data_transactions_involved = data_transaction_involved;
	}*/

	public double getData_popularity() {
		return data_popularity;
	}

	public void setData_popularity(double data_popularity) {
		this.data_popularity = data_popularity;
	}

	public double getData_cumulative_beta_probability() {
		return data_cumulative_beta_probability;
	}

	public void setData_cumulative_beta_probability(
			double data_cumulative_beta_probability) {
		this.data_cumulative_beta_probability = data_cumulative_beta_probability;
	}

	public boolean isData_isRoaming() {
		return data_isRoaming;
	}

	public void setData_isRoaming(boolean data_isRoaming) {
		this.data_isRoaming = data_isRoaming;
	}

	public int getData_localPartitionId() {
		return data_local_partition_id;
	}

	public void setData_localPartitionId(int data_local_partition_id) {
		this.data_local_partition_id = data_local_partition_id;
	}

	public int getData_globalPartitionId() {
		return data_global_partition_id;
	}

	public void setData_globalPartitionId(int data_partition_id) {
		this.data_global_partition_id = data_partition_id;
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

	public int getData_table_id() {
		return data_table_id;
	}

	public void setData_table_id(int data_table_id) {
		this.data_table_id = data_table_id;
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
	
	/*public void incData_frequency(int data_frequency) {	
		this.setData_frequency(++data_frequency);
	}
	
	public void incData_frequency() {
		int data_frequency = this.getData_frequency();
		this.setData_frequency(++data_frequency);
	}*/
	
	/*public void calculateData_weight() {
		this.setData_weight(1);
	}*/

	@Override
	public String toString() {		
		if(this.isData_isRoaming())
			return (this.data_label+"|PK-"+this.data_pk//+"|"+this.getData_hmetisClusterId()
					+"|(LP-"+this.data_local_partition_id+")(GP-"+this.data_global_partition_id+")(HP-"+this.data_home_partition_id
					+")/N"+this.data_node_id+"("+this.data_home_node_id+"))");// @C("+this.data_hmetis_cluster_id+") @h("+this.data_shadow_hmetis_id+")");
		else
			return (this.data_label+"|PK-"+this.data_pk//+"|"+this.getData_hmetisClusterId()
					+"|GP-"+this.data_global_partition_id
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