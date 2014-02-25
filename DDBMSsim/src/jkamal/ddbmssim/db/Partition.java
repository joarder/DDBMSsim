/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.db;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class Partition implements Comparable<Partition> {
	private int partition_global_id;
	private int partition_id;
	private String partition_label;
	private int partition_table_id;	
	private int partition_node_id;
	public final static int PARTITION_MAX_CAPACITY = 1000; // in data rows
	
	private Set<Data> partition_data_set;
	private Map<Integer, Integer> partition_data_lookup_table;
	
	private int partition_home_data;
	private int partition_foreign_data;
	private int partition_roaming_data;

	private double partition_current_load;
	private boolean partition_overloaded;
	
	private int partition_inflow;
	private int partition_outflow;
	
	public Partition(int global_id, int p_id, int table_id, int node_id) {
		this.setPartition_globalId(global_id);
		this.setPartition_id(p_id);		
		this.setPartition_table_id(table_id);
		this.setPartition_node_id(node_id);
		this.setPartition_label("P"+p_id+"("+global_id+")-T"+table_id);
		
		this.setPartition_dataSet(new TreeSet<Data>());
		this.setPartition_dataLookupTable(new TreeMap<Integer, Integer>());
		
		this.setPartition_home_data(0);
		this.setPartition_foreign_data(0);
		this.setPartition_roaming_data(0);
		
		this.setPartition_current_load(0.0);
		this.setPartition_overloaded(false);
		
		this.setPartition_inflow(0);
		this.setPartition_outflow(0);		
	}	

	// Copy Constructor
	public Partition(Partition partition) {
		this.setPartition_globalId(partition.getPartition_globalId());
		this.setPartition_id(partition.getPartition_id());
		this.setPartition_label(partition.getPartition_label());
		this.setPartition_table_id(partition.getPartition_table_id());
		this.setPartition_node_id(partition.getPartition_nodeId());
		
		Set<Data> clonePartitionDataSet = new TreeSet<Data>();
		Data cloneData;
		for(Data data : partition.getPartition_dataSet()) {
			cloneData = new Data(data);
			clonePartitionDataSet.add(cloneData);
		}
		this.setPartition_dataSet(clonePartitionDataSet);
				
		Map<Integer, Integer> clone_data_lookup_table = new TreeMap<Integer, Integer>();
		for(Entry<Integer, Integer> entry : partition.getPartition_dataLookupTable().entrySet()) {
			clone_data_lookup_table.put(entry.getKey(), entry.getValue());
		}
		this.setPartition_dataLookupTable(clone_data_lookup_table);
		
		this.setPartition_home_data(partition.getPartition_home_data());
		this.setPartition_foreign_data(partition.getPartition_foreign_data());
		this.setPartition_roaming_data(partition.getPartition_roaming_data());
		
		this.setPartition_current_load(partition.getPartition_current_load());
		this.setPartition_overloaded(partition.isPartition_overloaded());
		
		this.setPartition_inflow(this.getPartition_inflow());
		this.setPartition_outflow(this.getPartition_outflow());
	}	

	public int getPartition_globalId() {
		return partition_global_id;
	}

	public void setPartition_globalId(int global_id) {
		this.partition_global_id = global_id;
	}

	public int getPartition_id() {
		return partition_id;
	}

	public void setPartition_id(int partition_id) {
		this.partition_id = partition_id;
	}

	public String getPartition_label() {
		return partition_label;
	}

	public void setPartition_label(String partition_label) {
		this.partition_label = partition_label;
	}

	public int getPartition_table_id() {
		return partition_table_id;
	}

	public void setPartition_table_id(int partition_table_id) {
		this.partition_table_id = partition_table_id;
	}

	public int getPartition_nodeId() {
		return partition_node_id;
	}

	public void setPartition_node_id(int partition_node_id) {
		this.partition_node_id = partition_node_id;
	}

	public Set<Data> getPartition_dataSet() {
		return this.partition_data_set;
	}

	public void setPartition_dataSet(Set<Data> partition_data_set) {
		this.partition_data_set = partition_data_set;
	}
	
	public Map<Integer, Integer> getPartition_dataLookupTable() {
		return partition_data_lookup_table;
	}

	public void setPartition_dataLookupTable(Map<Integer, Integer> data_lookup_table) {
		this.partition_data_lookup_table = data_lookup_table;
	}
	
	public int getPartition_home_data() {
		return partition_home_data;
	}

	public void setPartition_home_data(int partition_home_data) {
		this.partition_home_data = partition_home_data;
	}

	public int getPartition_foreign_data() {
		return partition_foreign_data;
	}

	public void setPartition_foreign_data(int partition_foreign_data) {
		this.partition_foreign_data = partition_foreign_data;
	}

	public int getPartition_roaming_data() {
		return partition_roaming_data;
	}

	public void setPartition_roaming_data(int partition_roaming_data) {
		this.partition_roaming_data = partition_roaming_data;
	}
		
	public double getPartition_current_load() {
		return partition_current_load;
	}

	public void setPartition_current_load(double partition_current_load) {
		this.partition_current_load = partition_current_load;
	}

	public boolean isPartition_overloaded() {
		return partition_overloaded;
	}

	public void setPartition_overloaded(boolean partition_overloaded) {
		this.partition_overloaded = partition_overloaded;
	}
	
	public int getPartition_inflow() {
		return partition_inflow;
	}

	public void setPartition_inflow(int partition_inflow) {
		this.partition_inflow = partition_inflow;
	}

	public int getPartition_outflow() {
		return partition_outflow;
	}

	public void setPartition_outflow(int partition_outflow) {
		this.partition_outflow = partition_outflow;
	}
	
	public void incPartition_inflow(int val){		
		this.setPartition_inflow((this.getPartition_inflow() + val));
	}
	
	public void decPartition_inflow(int val){		
		this.setPartition_inflow((this.getPartition_inflow() - val));
	}
	
	public void incPartition_inflow(){		
		int val = this.getPartition_inflow();
		this.setPartition_inflow(++val);
	}
	
	public void decPartition_inflow(){
		int val = this.getPartition_inflow();
		this.setPartition_inflow(--val);
	}

	public void incPartition_outflow(int val){		
		this.setPartition_outflow((this.getPartition_outflow() + val));
	}
	
	public void decPartition_outflow(int val){		
		this.setPartition_outflow((this.getPartition_outflow() - val));
	}
	
	public void incPartition_outflow(){		
		int val = this.getPartition_outflow();
		this.setPartition_outflow(++val);
	}
	
	public void decPartition_outflow(){
		int val = this.getPartition_outflow();
		this.setPartition_outflow(--val);
	}
	
	public void incPartition_home_data() {
		int home_data = this.getPartition_home_data();		
		this.setPartition_home_data(++home_data);
	}
	
	public void decPartition_home_data() {
		int home_data = this.getPartition_home_data();		
		this.setPartition_home_data(--home_data);
	}
	
	public void incPartition_foreign_data() {
		int foreign_data = this.getPartition_foreign_data();		
		this.setPartition_foreign_data(++foreign_data);
	}
	
	public void decPartition_foreign_data() {
		int foreign_data = this.getPartition_foreign_data();		
		this.setPartition_foreign_data(--foreign_data);
	}
	
	public void incPartition_roaming_data() {
		int roaming_data = this.getPartition_roaming_data();		
		this.setPartition_roaming_data(++roaming_data);
	}
	
	public void decPartition_roaming_data() {
		int roaming_data = this.getPartition_roaming_data();		
		this.setPartition_roaming_data(--roaming_data);
	}
	
	public void updatePartitionLoad() {
		int totalData = this.getPartition_dataSet().size();
		
		if(totalData > Partition.PARTITION_MAX_CAPACITY * 0.9)
			this.setPartition_overloaded(true);
		else 
			this.setPartition_overloaded(false);
		
		double percentage = ((double)totalData/Partition.PARTITION_MAX_CAPACITY) * 100.0;
		percentage = Math.round(percentage * 100.0) / 100.0;
		this.setPartition_current_load(percentage);
	}
	
	// Returns a Data object queried by it's Data Id from the Partition
	public Data getData(int data_id) {		
		for(Data data : this.getPartition_dataSet()) {
			if(data.getData_id() == data_id)				
				return data;
		}
		
		return null;
	}
		
	public void show() {
		int comma = this.getPartition_dataSet().size();
		
		System.out.print("       {");
		
		for(Data data : this.getPartition_dataSet()) {
			System.out.print(data.toString());
			
			if(comma != 1)
				System.out.print(", ");
			
			--comma;
		}
		
		System.out.println("}");
	}

	@Override
	public String toString() {
		if(this.getPartition_roaming_data() != 0 || this.getPartition_foreign_data() !=0)
			return (this.getPartition_label()
					+"["+this.getPartition_dataSet().size()
					+"|"+this.getPartition_current_load()+"%]-"
					+"R("+this.getPartition_roaming_data()+")/"
					+"F("+this.getPartition_foreign_data()+") - " 
					+"Inflow("+this.getPartition_inflow()+")|"
					+"Outflow("+this.getPartition_outflow()+")"
					);										
		else	
			return (this.getPartition_label()
					+"["+this.getPartition_dataSet().size()
					+"|"+this.getPartition_current_load()+"%]"
					);
	}
	
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof Partition)) {
			return false;
		}
		
		Partition partition = (Partition) object;
		return (this.getPartition_label().equals(partition.getPartition_label()));
	}

	@Override
	public int hashCode() {
		return (this.getPartition_label().hashCode());
	}

	@Override
	public int compareTo(Partition partition) {		
		return (((int)this.getPartition_globalId() < (int)partition.getPartition_globalId()) ? -1: 
			((int)this.getPartition_globalId() > (int)partition.getPartition_globalId()) ? 1:0);		
	}
}