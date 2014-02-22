/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.db;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import jkamal.ddbmssim.main.DBMSSimulator;

public class Database implements Comparable<Database> {
	private int db_id;
	private String db_name;
	private int db_tenant;	
	private DatabaseServer db_dbs;
	private int db_data_numbers;
	private int db_partition_size;
	private int db_partitions;
	private String db_partitioing;
	private Set<Table> db_tables;
	private Map<Integer, Set<Integer>> db_nodes;
	private double[] db_normalised_cumalitive_zipf_probability;
	private PrintWriter workload_log;
	private PrintWriter node_log;
	private PrintWriter partition_log;
	
	public Database(int id, String name, int tenant_id, DatabaseServer dbs, String model, double partition_size) {
		this.setDb_id(id);
		this.setDb_name(name);
		this.setDb_tenant(tenant_id);
		this.setDb_dbs(dbs);
		this.setDb_data_numbers(0);
		this.setDb_partitionSize((int)(partition_size * 1000)); // Partition Size Range (1 ~ 1000 GB), 1 GB = 1000 Data Objects of equal size
		this.setDb_partitions(0);
		this.setDb_partitioing(model);
		this.setDb_tables(new TreeSet<Table>());
		this.setDb_nodes(new TreeMap<Integer, Set<Integer>>());
		this.setDb_normalisedCumalitiveZipfProbabilityArray(new double[6051]);
	}	
	
	// Copy Constructor
	public Database(Database db) {
		this.setDb_id(db.getDb_id());
		this.setDb_name(db.getDb_name());
		this.setDb_tenant(db.getDb_tenant());	
		this.setDb_dbs(db.getDb_dbs());
		this.setDb_data_numbers(db.getDb_data_numbers());
		this.setDb_partitionSize(db.getDb_partitionSize());		
		this.setDb_partitions(db.getDb_partitions());
		
		Set<Table> cloneDbTables = new TreeSet<Table>();
		Table cloneTable;
		for(Table table : db.getDb_tables()) {
			cloneTable = new Table(table);
			cloneDbTables.add(cloneTable);
		}		
		this.setDb_tables(cloneDbTables);
		
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
		
		double[] clone_db_normalised_cumalitive_zipf_probability = new double[6051];
		int i = 0;
		for(double d : db.getDb_normalisedCumalitiveZipfProbabilityArray()) {
			clone_db_normalised_cumalitive_zipf_probability[i] = d;
			++i;
		}
		this.setDb_normalisedCumalitiveZipfProbabilityArray(clone_db_normalised_cumalitive_zipf_probability);
		
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

	public Map<Integer, Set<Integer>> getDb_nodes() {
		return db_nodes;
	}

	public void setDb_nodes(Map<Integer, Set<Integer>> db_nodes) {
		this.db_nodes = db_nodes;
	}

	public int getDb_partitionSize() {
		return db_partition_size;
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

	public void setDb_partitionSize(int db_partition_size) {
		this.db_partition_size = db_partition_size;
	}

	public double[] getDb_normalisedCumalitiveZipfProbabilityArray() {
		return db_normalised_cumalitive_zipf_probability;
	}

	public void setDb_normalisedCumalitiveZipfProbabilityArray(
			double[] db_normalised_cumalitive_zipf_probability) {
		this.db_normalised_cumalitive_zipf_probability = db_normalised_cumalitive_zipf_probability;
	}
		
	// Create a new Data object and attach it with the designated Table and Partition within a given Database
	public Data createNewDataObject(Table table, int data_id) {
		// Generate Partition Id
		int target_partition = (data_id % this.getDb_dbs().getDbs_nodes().size());
		Partition partition = table.getPartition(target_partition + 1);
		
		// Create a new Data Row Object
		Data data = new Data(table, data_id, partition.getPartition_id(), partition.getPartition_globalId(), partition.getPartition_nodeId(), false);				
		data.setData_pk(table.getTbl_id());				
		data.setData_size(DBMSSimulator.TPCC_DATA_ROW_SIZE[partition.getPartition_table_id() - 1]);				
		
		// Put an entry into the Partition Data lookup table and add in the Data object into the Partition Data Set
		partition.getPartition_dataLookupTable().put(data.getData_id(), partition.getPartition_globalId());
		partition.getPartition_dataSet().add(data);
		partition.updatePartitionLoad();
		
		// Increment Data counts at Node, Database and Table level
		this.getDb_dbs().getDbs_node(partition.getPartition_nodeId()).incNode_totalData();					
		this.setDb_data_numbers(data_id);
		int d = table.getTbl_data_count();
		table.setTbl_data_count(++d);
		
		if(data_id == 5270)
		System.out.println(">> "+data.toString());
		
		return data;
	}
	
	// Return the number of Data for the target Table
	public int getTableData(int table_id) {
		for(Table table : this.getDb_tables()) {
			if(table.getTbl_id() == table_id){System.out.println(">> keys = "+table.getTbl_data_count());
				return table.getTbl_data_count();}
		}
		
		return -1;
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
	public Data search(int data_id) {
		int target_partition = data_id % this.getDb_dbs().getDbs_nodes().size();		
		
		for(Table table : this.getDb_tables()) {			
			for(Data data : table.getPartition(target_partition + 1).getPartition_dataSet()) {
				if(data.getData_id() == data_id)
					return data;
			}
		}
		
		return null;
	}		
	
	public Partition getPartition(int global_partition_id) { // search by global partition id from the Database level
		for(Table table : this.getDb_tables()) {
			for(Partition partition : table.getTbl_partitions()) {						
				if(partition.getPartition_globalId() == global_partition_id) 
					return partition;
			}
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
	
	public Set<Integer> getNodePartitions(int node_id) {
		Set<Integer> node_partitions = new TreeSet<Integer>();
		
		for(Entry<Integer, Set<Integer>> entry : this.getDb_nodes().entrySet()) {
			if(entry.getKey() == node_id) {
				for(Integer partition_id : entry.getValue()) {
					node_partitions.add(this.getPartition(partition_id).getPartition_globalId());
				}
				
				break;
			}
		}
		
		return node_partitions;
	}
	
	public void show() {		
		System.out.println("[OUT] ==== Database Details ====");
		System.out.println("      Database: "+this.getDb_name());
		System.out.println("      Number of Partitions: "+this.getDb_partitionSize());		
		System.out.println("[OUT] ==== Partition Table Details ====");		
		
		Set<Integer> overloadedPartition = new TreeSet<Integer>();
		int comma = -1;		
		for(Entry<Integer, Set<Integer>> entry : this.getDb_nodes().entrySet()) {
			System.out.println("    --N"+entry.getKey()
					+"("+this.getDb_dbs().getDbs_node(entry.getKey()).getNode_size()+" MB)"
					+" | Inflow("+this.getDb_dbs().getDbs_node(entry.getKey()).getNode_inflow()+")|"
					+" | Outflow("+this.getDb_dbs().getDbs_node(entry.getKey()).getNode_outflow()+")"
					);
				
			for(Integer partition_id : entry.getValue()) {
				Partition partition = this.getPartition(partition_id);
				partition.updatePartitionLoad();
				
				System.out.println("    ----"+partition.toString());
				//partition.show();
				
				if(partition.isPartition_overloaded())
					overloadedPartition.add(partition.getPartition_globalId());
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