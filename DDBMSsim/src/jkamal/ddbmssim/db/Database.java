/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.db;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

public class Database {
	private int db_id;
	private String db_name;
	private int db_tenant;	
	private int db_partition_size;
	private Set<Partition> db_partitions;		
	private Map<Integer, Set<Integer>> db_nodes;		
	
	public Database(int id, String name, int tenant_id, String model, double partition_size) {
		this.setDb_id(id);
		this.setDb_name(name);
		this.setDb_tenant(tenant_id);
		this.setDb_partition_size((int)(partition_size * 1000)); // Partition Size Range (1 ~ 1000 GB), 1 GB = 1000 Data Objects of equal size
		this.setDb_partitions(new TreeSet<Partition>());
		this.setDb_nodes(new TreeMap<Integer, Set<Integer>>());
	}	
	
	// Copy Constructor
	public Database(Database db) {
		this.setDb_id(db.getDb_id());
		this.setDb_name(db.getDb_name());
		this.setDb_tenant(db.getDb_tenant());		
		this.setDb_partition_size(db.getDb_partition_size());
		
		Set<Partition> cloneDbPartitions = new TreeSet<Partition>();
		Partition clonePartition;
		for(Partition partition : db.getDb_partitions()) {
			clonePartition = new Partition(partition);
			cloneDbPartitions.add(clonePartition);
		}		
		this.setDb_partitions(cloneDbPartitions);
		
		Map<Integer, Set<Integer>> clone_db_nodes = new TreeMap<Integer, Set<Integer>>();		
		for(Entry<Integer, Set<Integer>> entry : db.getDb_nodes().entrySet()) {
			Set<Integer> clone_partitions = new TreeSet<Integer>();
			for(Integer partition_id : entry.getValue()) {
				//System.out.println("@debug >> N="+entry.getKey()+" | P="+partition_id);
				clone_partitions.add(partition_id);
			}
			
			clone_db_nodes.put(entry.getKey(), clone_partitions);
		}
		this.setDb_nodes(clone_db_nodes);
	}

	public int getDb_id() {
		return db_id;
	}

	public void setDb_id(int db_id) {
		this.db_id = db_id;
	}

	public String getDb_name() {
		return db_name;
	}

	public void setDb_name(String db_name) {
		this.db_name = db_name;
	}

	public int getDb_tenant() {
		return db_tenant;
	}

	public void setDb_tenant(int db_tenant) {
		this.db_tenant = db_tenant;
	}
	
	public Set<Partition> getDb_partitions() {
		return db_partitions;
	}

	public void setDb_partitions(Set<Partition> db_partitions) {
		this.db_partitions = db_partitions;
	}
	
	
	public Map<Integer, Set<Integer>> getDb_nodes() {
		return db_nodes;
	}

	public void setDb_nodes(Map<Integer, Set<Integer>> db_nodes) {
		this.db_nodes = db_nodes;
	}

	public int getDb_partition_size() {
		return db_partition_size;
	}

	public void setDb_partition_size(int db_partition_size) {
		this.db_partition_size = db_partition_size;
	}

	public boolean insert() {
		boolean success = false;
		
		
		return success;
	}
	
	public boolean update() {
		boolean success = false;
		
		
		return success;
	}
	
	// Searches for a specific Data by it's Id
	public Data search(int data_id) {
		for(Partition partition : this.getDb_partitions()) {
			int partition_id = partition.lookupPartitionId_byDataId(data_id);
			//System.out.println("@debug >> searching d"+data_id+" | Found in P"+partition_id);
			
			if(partition_id != -1)				
				return(this.getPartition(partition_id).getData_byDataId(data_id));
		}			
		
		return null;
	}
	
	public boolean delete(int data_id) {
		boolean success = false;
		
		
		return success;
	}
	
	public Partition getPartition(int partition_id) {
		for(Partition partition : this.getDb_partitions()) {						
			if(partition.getPartition_id() == partition_id) 
				return partition;
		}
		
		return null;
	}
	
	public Set<Integer> getNodePartitions(int node_id) {
		Set<Integer> node_partitions = new TreeSet<Integer>();
		
		for(Entry<Integer, Set<Integer>> entry : this.getDb_nodes().entrySet()) {
			if(entry.getKey() == node_id) {
				for(Integer partition_id : entry.getValue()) {
					node_partitions.add(this.getPartition(partition_id).getPartition_id());
				}
				
				break;
			}
		}
		
		return node_partitions;
	}
	
	public void show() {		
		System.out.println("[OUT] ==== Database Details ====");
		System.out.println("      Database: "+this.getDb_name());
		System.out.println("      Number of Partitions: "+this.getDb_partitions().size());		
		System.out.println("[OUT] ==== Partition Table Details ====");		
		
		Set<Integer> overloadedPartition = new TreeSet<Integer>();
		int comma = -1;		
		for(Entry<Integer, Set<Integer>> entry : this.getDb_nodes().entrySet()) {
			System.out.println("    --N"+entry.getKey());//+" {");
				
			for(Integer partition_id : entry.getValue()) {
				Partition partition = this.getPartition(partition_id);
				partition.getCurrentLoad();
				
				System.out.println("    ----"+partition.toString());
				//partition.show();
				
				if(partition.isPartition_overloaded())
					overloadedPartition.add(partition.getPartition_id());
			}									
		}
		
		if(overloadedPartition.size() != 0) {
			System.out.println();
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