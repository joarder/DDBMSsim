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
	private int partition_id;
	private String partition_label;
	private int partition_node_id;
	private int partition_capacity;		
	
	private Set<Data> partition_data_set;
	private Map<Integer, Integer> partition_data_lookup_table;
	
	private int partition_home_data;
	private int partition_foreign_data;
	private int partition_roaming_data;

	private double partition_current_load;
	private boolean partition_overloaded;
	
	public Partition(int partiton_id, String partition_label, int node_id, int partition_capacity) {
		this.setPartition_id(partiton_id);
		this.setPartition_label("P"+partition_label);
		this.setPartition_node_id(node_id);
		this.setPartition_capacity(partition_capacity);
		
		this.setPartition_dataSet(new TreeSet<Data>());
		this.setPartition_dataLookupTable(new TreeMap<Integer, Integer>());
		
		this.setPartition_home_data(0);
		this.setPartition_foreign_data(0);
		this.setPartition_roaming_data(0);
		
		this.setPartition_current_load(0.0);
		this.setPartition_overloaded(false);
	}	

	// Copy Constructor
	public Partition(Partition partition) {
		this.setPartition_id(partition.getPartition_id());
		this.setPartition_label(partition.getPartition_label());
		this.setPartition_node_id(partition.getPartition_nodeId());
		this.setPartition_capacity(partition.getPartition_capacity());				
		
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

	public int getPartition_nodeId() {
		return partition_node_id;
	}

	public void setPartition_node_id(int partition_node_id) {
		this.partition_node_id = partition_node_id;
	}

	public int getPartition_capacity() {
		return partition_capacity;
	}

	public void setPartition_capacity(int partition_capacity) {
		this.partition_capacity = partition_capacity;
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
	
	// Returns the current Partition Id for a queried Data Id
	public int lookupPartitionId_byDataId(int data_id) {
		int partition_id = -1;
		
		for(Entry<Integer, Integer> entry : this.getPartition_dataLookupTable().entrySet()) {
			if(entry.getKey() == data_id)
				return entry.getValue();
		}
		
		return partition_id;
	}
	
	// Returns a Data object queried by it's Data Id from the Partition
	public Data getData_byDataId(int data_id) {		
		for(Data data : this.getPartition_dataSet()) {
			if(data.getData_id() == data_id)				
				return data;
		}
		
		return null;
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
	
	public void getCurrentLoad() {
		int totalData = this.getPartition_dataSet().size();
		
		if(totalData > this.getPartition_capacity()*0.9)//.getPartition_size_overloaded())
			this.setPartition_overloaded(true);
		else 
			this.setPartition_overloaded(false);
		
		double percentage = ((double)totalData/this.getPartition_capacity()) * 100.0;
		percentage = Math.round(percentage * 100.0) / 100.0;
		this.setPartition_current_load(percentage);
	}
	
	public void show() {
		int comma = this.getPartition_dataSet().size();
		
		System.out.print("       {");
		
		for(Data data : this.getPartition_dataSet()) {
			System.out.print(data.getData_id());
			
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
					+"["+this.getPartition_dataSet().size()+"|"+this.getPartition_current_load()+"%]-" //+"/"+this.getPartition_capacity()
					+"R("+this.getPartition_roaming_data()+")/"
					+"F("+this.getPartition_foreign_data()+")");										
		else	
			return (this.getPartition_label()
					+"["+this.getPartition_dataSet().size()+"|"+this.getPartition_current_load()+"%]"); //getPartition_capacity()
					//+"H("+this.getPartition_dataSet().size()+")");
	}
	
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof Partition)) {
			return false;
		}
		
		Partition partition = (Partition) object;
		return (getPartition_label().equals(partition.getPartition_label()));
	}

	@Override
	public int hashCode() {
		return (this.getPartition_label().hashCode());
	}

	@Override
	public int compareTo(Partition partition) {		
		return (((int)this.getPartition_id() < (int)partition.getPartition_id()) ? -1: 
			((int)this.getPartition_id() > (int)partition.getPartition_id()) ? 1:0);		
	}
}