/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.db;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import jkamal.ddbmssim.main.DBMSSimulator;

public class Database implements Comparable<Database> {
	private int db_id;
	private String db_name;
	private int db_tenant;	
	private DatabaseServer db_dbs;
	private int db_data_numbers;
	private int db_partitions;
	private String db_partitioing;
	private Set<Table> db_tables;	
	private Map<Integer, Integer> db_table_partition_map;
	private Map<Integer, Integer> db_partition_data_map;
	private PrintWriter workload_log;
	private PrintWriter node_log;
	private PrintWriter partition_log;
	private int db_miner_id;
	
	public Database(int id, String name, int tenant_id, DatabaseServer dbs, String model) {
		this.setDb_id(id);
		this.setDb_name(name);
		this.setDb_tenant(tenant_id);
		this.setDb_dbs(dbs);
		this.setDb_data_numbers(0);
		this.setDb_partitions(0);
		this.setDb_partitioing(model);
		this.setDb_tables(new TreeSet<Table>());		
		this.setDb_table_partition_map(new HashMap<Integer, Integer>());
		this.setDb_partition_data_map(new HashMap<Integer, Integer>());
	}	
	
	// Copy Constructor
	public Database(Database db) {
		this.setDb_id(db.getDb_id());
		this.setDb_name(db.getDb_name());
		this.setDb_tenant(db.getDb_tenant());	
		//this.setDb_dbs(db.getDb_dbs());
		this.setDb_data_numbers(db.getDb_data_numbers());		
		this.setDb_partitions(db.getDb_partitions());
		
		Set<Table> cloneDbTables = new TreeSet<Table>();		
		for(Table table : db.getDb_tables())
			cloneDbTables.add(new Table(table));
		this.setDb_tables(cloneDbTables);
		
		Map<Integer, Integer> clone_db_table_partitions = new HashMap<Integer, Integer>();		
		for(Entry<Integer, Integer> entry : db.getDb_table_partition_map().entrySet())			
			clone_db_table_partitions.put(entry.getKey(), entry.getValue());			
		this.setDb_table_partition_map(clone_db_table_partitions);
		
		Map<Integer, Integer> clone_db_partition_data = new HashMap<Integer, Integer>();		
		for(Entry<Integer, Integer> entry : db.getDb_partition_data_map().entrySet())			
			clone_db_partition_data.put(entry.getKey(), entry.getValue());			
		this.setDb_partition_data_map(clone_db_partition_data);		
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
	
	public DatabaseServer getDb_dbs() {
		return db_dbs;
	}

	public void setDb_dbs(DatabaseServer db_dbs) {
		this.db_dbs = db_dbs;
	}

	public int getDb_data_numbers() {
		return db_data_numbers;
	}

	public void setDb_data_numbers(int db_data_numbers) {
		this.db_data_numbers = db_data_numbers;
	}
	
	public void updateDb_data_numbers() {
		int data_count = 0;
		for(Table table : this.getDb_tables()) {
			for(Partition partition : table.getTbl_partitions()) {
				data_count += partition.getPartition_dataSet().size();
			}
		}
		
		this.setDb_data_numbers(data_count);
	}

	public String getDb_partitioing() {
		return db_partitioing;
	}

	public void setDb_partitioing(String db_partitioing) {
		this.db_partitioing = db_partitioing;
	}

	public int getDb_partitions() {
		return db_partitions;
	}

	public void setDb_partitions(int db_partitions) {
		this.db_partitions = db_partitions;
	}

	public Set<Table> getDb_tables() {
		return this.db_tables;
	}

	public void setDb_tables(Set<Table> db_tables) {
		this.db_tables = db_tables;
	}

	public Map<Integer, Integer> getDb_table_partition_map() {
		return db_table_partition_map;
	}

	public void setDb_table_partition_map(Map<Integer, Integer> db_table_partitions) {
		this.db_table_partition_map = db_table_partitions;
	}

	public Map<Integer, Integer> getDb_partition_data_map() {
		return db_partition_data_map;
	}

	public void setDb_partition_data_map(Map<Integer, Integer> db_partition_data) {
		this.db_partition_data_map = db_partition_data;
	}

	public PrintWriter getWorkload_log() {
		return workload_log;
	}

	public void setWorkload_log(PrintWriter workload_log) {
		this.workload_log = workload_log;
	}

	public PrintWriter getNode_log() {
		return node_log;
	}

	public void setNode_log(PrintWriter node_log) {
		this.node_log = node_log;
	}

	public PrintWriter getPartition_log() {
		return partition_log;
	}

	public void setPartition_log(PrintWriter partition_log) {
		this.partition_log = partition_log;
	}
		
	public int getDb_miner_id() {
		return db_miner_id;
	}

	public void setDb_miner_id(int db_miner_id) {
		this.db_miner_id = db_miner_id;
	}

	// Create a new Data object and attach it with the designated Table and Partition within a given Database
	public Data insertData(int table_id, int data_id) {
		Table table = this.getTable(table_id);
		
		// Generate Partition Id
		int partition_id = (data_id % this.getDb_dbs().getDbs_nodes().size())+1;
		Partition partition = table.getPartition(partition_id);
		
		// Create a new Data Row Object
		Data data = new Data(table, data_id, partition.getPartition_id(), partition.getPartition_globalId(), partition.getPartition_nodeId(), false);				
		data.setData_pk(table.getTbl_id());				
		data.setData_size(DBMSSimulator.TPCC_DATA_ROW_SIZE[partition.getPartition_table_id() - 1]);				
		
		// Put an entry into the Partition Data lookup table 
		this.getDb_partition_data_map().put(data.getData_id(), partition.getPartition_globalId());
		
		// Put an entry into the Partition Data lookup table and add in the Data object into the Partition Data Set
		partition.getPartition_dataLookupTable().put(data.getData_id(), partition.getPartition_globalId());
		partition.getPartition_dataSet().add(data);
		partition.updatePartitionLoad();
		
		// Increment Data counts at Node, Database and Table level
		this.getDb_dbs().getDbs_node(partition.getPartition_nodeId()).incNode_totalData();		
		table.setTbl_data_count(data.getData_primary_key());		
		
		return data;
	}
	
	public void deleteData(int table_id, int data_id) {
		Table table = this.getTable(table_id);
		
		Data _data = table.getData(this, data_id);
		Partition _partition = table.getPartition(_data.getData_localPartitionId());
		
		// Remove the entry from the Partition Data lookup table and remove the Data object from the Partition Data Set
		_partition.getPartition_dataLookupTable().put(_data.getData_id(), _partition.getPartition_globalId());
		_partition.getPartition_dataSet().remove(_data);
		_partition.updatePartitionLoad();						
		
		// Decrement Data counts at Node and Table level
		this.getDb_dbs().getDbs_node(_partition.getPartition_nodeId()).decNode_totalData();
		int count = table.getTbl_data_count();
		table.setTbl_data_count(--count);
		//System.out.println("\t\t@ Deleting d"+data_id+" from "+_partition.getPartition_label());
	}
	
	// Getting the dependency information for the target Table
	public ArrayList<Integer> getLinkedTables(Table table) {
		ArrayList<Integer> linkedTables = new ArrayList<Integer>();
		
		for(int i = 0; i < DBMSSimulator.TPCC_SCHEMA[table.getTbl_id() - 1].length; i++) {
			int link = DBMSSimulator.TPCC_SCHEMA[table.getTbl_id() - 1][i];					
			
			if(link == 1) {
				linkedTables.add(i+1); // i+1 = Table id
				table.getTbl_foreign_tables().add(i+1);
			}
		}
		
		return linkedTables;
	}
	
	// For Primary Table
	public String createPrimaryKey(int table_id, int data_id) {
		return (Integer.toString(table_id)+"-"+Integer.toString(data_id));
	}
	
	// For Dependent Table
	public String createPrimaryKey(LinkedList<Integer> linkSet, int data_id) {
		String primary_key = "";
		int adder = linkSet.size();
		
		for(Integer i : linkSet) {
			primary_key += Integer.toString(i);
			
			if(adder != 1)
				primary_key += "-";
			
			--adder;
		}
		
		return primary_key;
	}
	
	// For Secondary Table
	public String createForeignKey(int table_id, LinkedList<Integer> linkSet) {
		String foreign_key = Integer.toString(table_id)+"-";
		int adder = linkSet.size();
		
		for(Integer i : linkSet) {
			foreign_key += Integer.toString(i);
			
			if(adder != 1)
				foreign_key += "-";
			
			--adder;
		}
		
		return foreign_key;
	}
	
	public String[] getTableKeys(int table_type, int table_id, int data_id, LinkedList<Integer> linkSet) {
		String[] keys = new String[2];
		
		switch(table_type) {
			case 0: // Primary table
				keys[0] = this.createPrimaryKey(table_id, data_id); //primary_key
				keys[1] = Integer.toString(Integer.MAX_VALUE); // No Foreign Key
				break;
			case 1: // Secondary table
				keys[0] = this.createPrimaryKey(table_id, data_id); //primary_key
				keys[1] = this.createForeignKey(table_id, linkSet);
				break;
			case 2: // Dependent table						
				keys[0] = this.createPrimaryKey(linkSet, data_id); //primary_key
				keys[1] = Integer.toString(Integer.MAX_VALUE); // No Foreign Key
				break;
		}
		
		return keys;
	}
	
	// Searches for a specific Data by it's Id
	public Data getData(int data_id) {
		int global_partition_id = this.getDb_partition_data_map().get(data_id);		
		Partition partition = this.getPartition(global_partition_id);
		//System.out.println("@ "+data_id+"|"+partition.toString());
		Data data = partition.getData(data_id);
		//System.out.println("@ "+data.toString());
							
		return data;
	}		
	
	// Search by global partition id from the Database level
	public Partition getPartition(int global_partition_id) {
		int table_id = this.getDb_table_partition_map().get(global_partition_id);
		Table table = this.getTable(table_id);
				
		return (this.getPartition(table, global_partition_id));
	}
	
	// Search by global partition id within a specific Table from the Database level
	public Partition getPartition(Table table, int global_partition_id) { 		
		for(Partition partition : table.getTbl_partitions()) {						
			if(partition.getPartition_globalId() == global_partition_id) 
				return partition;
		}		
		
		return null;
	}
	
	public Table getTable(int table_id) {
		for(Table table : this.getDb_tables()) {						
			if(table.getTbl_id() == table_id) 
				return table;
		}
		
		return null;
	}
	
	// Returns the Partition set associated with the target Node
	public Set<Integer> getNodePartitions(int node_id) {		
		for(Node node : this.getDb_dbs().getDbs_nodes()) {
			if(node.getNode_id() == node_id)
				return node.getNode_partitions();
		}
		
		return null;
	}
	
	public void show() {		
		System.out.println("[OUT] ==== Database Details ====");
		System.out.println("      Database: "+this.getDb_name());
		System.out.println("      Number of Tables: "+this.getDb_tables().size());		
		System.out.println("[OUT] ==== Table Partition Details ====");		
		
		Set<Integer> overloadedPartition = new TreeSet<Integer>();
		int comma = -1;
		
		for(Table table : this.getDb_tables()) {
			System.out.println("    --"+table.toString());
			
			for(Partition partition : table.getTbl_partitions()) {
				System.out.println("    ----"+partition.toString());
				partition.show();
				
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
	
	@Override
	public String toString() {
		return ("Database: "+this.getDb_name()+", Id: "+this.getDb_id()+", Tenant Id: "+this.getDb_tenant());
	}
	
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof Database)) {
			return false;
		}
		
		Database db = (Database) object;
		return (this.getDb_name().equals(db.getDb_name()));
	}

	@Override
	public int hashCode() {
		return (this.getDb_name().hashCode());
	}

	@Override
	public int compareTo(Database db) {		
		return (((int)this.getDb_id() < (int)db.getDb_id()) ? -1: 
			((int)this.getDb_id() > (int)db.getDb_id()) ? 1:0);		
	}
}