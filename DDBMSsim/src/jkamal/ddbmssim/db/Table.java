/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import jkamal.ddbmssim.main.DBMSSimulator;
import org.apache.commons.collections4.MapIterator;
import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.commons.collections4.map.MultiValueMap;

public class Table implements Comparable<Table>{
	private int tbl_id;
	private int tbl_db_id;
	private String tbl_name;
	private int tbl_type;
	private int tbl_data_count;
	private double tbl_size;
	private Set<Partition> tbl_partitions;
	private Set<Integer> tbl_foreign_tables;
	private MultiValueMap<Integer, Integer> tbl_data_map_s; // For the secondary tables
	private MultiKeyMap<Integer, Integer> tbl_data_map_d; // For the dependent tables
	private HashMap<Integer, Integer> tbl_data_pid_map;
	private HashMap<Integer, Integer> tbl_pid_data_map;
	private int[] tbl_data_rank;
	
	public Table(int id, int type, int db_id, String name) {
		this.setTbl_id(id);
		this.setTbl_db_id(db_id);
		this.setTbl_name(name);
		this.setTbl_type(type);
		this.setTbl_data_count(0);
		this.setTbl_size(0.0d);
		this.setTbl_partitions(new TreeSet<Partition>());
		
		if(this.getTbl_type() != 0)
			this.setTbl_foreign_tables(new TreeSet<Integer>());
		
		if(this.getTbl_type() != 2) {
			if(this.getTbl_type() == 1) {
				this.setTbl_data_map_s(new MultiValueMap<Integer, Integer>());
			}
		} else {
			this.setTbl_data_map_d(new MultiKeyMap<Integer, Integer>());
		}		
		
		this.setTbl_data_pid_map(new HashMap<Integer, Integer>());
		this.setTbl_pid_data_map(new HashMap<Integer, Integer>());
		
		if(this.getTbl_id() == 1)
			this.setTbl_data_rank(new int[(DBMSSimulator.TPCC_WAREHOUSE) + 1]);
		else if(this.getTbl_id() == 2)
			this.setTbl_data_rank(new int[((int) (100000 * DBMSSimulator.TPCC_Scale)) + 1]);
	}
	
	// Copy Constructor
	public Table(Table table) {
		this.setTbl_id(table.getTbl_id());
		this.setTbl_db_id(table.getTbl_db_id());
		this.setTbl_name(table.getTbl_name());
		this.setTbl_type(table.getTbl_type());
		this.setTbl_data_count(table.getTbl_data_count());
		this.setTbl_size(table.getTbl_size());
		
		Set<Partition> clone_partitions = new TreeSet<Partition>();		
		for(Partition clonePartition : table.getTbl_partitions())
			clone_partitions.add(new Partition(clonePartition));
		this.setTbl_partitions(clone_partitions);
		
		if(this.getTbl_type() != 0) {
			Set<Integer> clone_foreign_tables = new TreeSet<Integer>();		
			for(Integer foreign_table : table.getTbl_foreign_tables())
				clone_foreign_tables.add(foreign_table);
			this.setTbl_foreign_tables(clone_foreign_tables);
		}
		
		if(this.getTbl_type() != 2) {
			if(this.getTbl_type() == 1) {
				MultiValueMap<Integer, Integer> clone_data_map_s = new MultiValueMap<Integer, Integer>();
				for(Object key : table.getTbl_data_map_s().keySet())
					clone_data_map_s.put((Integer) key, table.getTbl_data_map_s().get(key));
				
				this.setTbl_data_map_s(clone_data_map_s);
			}			
		} else {
			MultiKeyMap<Integer, Integer> clone_data_map_d = new MultiKeyMap<Integer, Integer>();
			MapIterator<MultiKey<? extends Integer>, Integer> it = table.getTbl_data_map_d().mapIterator();
			while(it.hasNext()) {
				it.next();
			    @SuppressWarnings("unchecked")
				MultiKey<Integer> mk = (MultiKey<Integer>) it.getKey();
			    clone_data_map_d.put(mk.getKey(0), mk.getKey(1), it.getValue());		    
			}
			this.setTbl_data_map_d(clone_data_map_d);
		}
		
		HashMap<Integer, Integer> clone_data_pid_map = new HashMap<Integer, Integer>();
		for(Entry<Integer, Integer> entry : table.getTbl_data_pid_map().entrySet())
			clone_data_pid_map.put(entry.getKey(), entry.getValue());
		this.setTbl_data_pid_map(clone_data_pid_map);
		
		HashMap<Integer, Integer> clone_pid_data_map = new HashMap<Integer, Integer>();
		for(Entry<Integer, Integer> entry : table.getTbl_pid_data_map().entrySet())
			clone_pid_data_map.put(entry.getKey(), entry.getValue());
		this.setTbl_pid_data_map(clone_pid_data_map);
		
		if(this.getTbl_id() == 1 || this.getTbl_id() == 2) {
			int[] clone_data_rank = new int[table.getTbl_data_rank().length];
			for(int i = 0; i < table.getTbl_data_rank().length; i++)
				clone_data_rank[i] = table.getTbl_data_rank()[i];		
			this.setTbl_data_rank(clone_data_rank);
		}
	}
	
	public int getTbl_id() {
		return tbl_id;
	}

	public void setTbl_id(int tbl_id) {
		this.tbl_id = tbl_id;
	}

	public int getTbl_db_id() {
		return tbl_db_id;
	}

	public void setTbl_db_id(int tbl_db_id) {
		this.tbl_db_id = tbl_db_id;
	}

	public String getTbl_name() {
		return tbl_name;
	}

	public void setTbl_name(String tbl_name) {
		this.tbl_name = tbl_name;
	}

	public int getTbl_data_count() {
		return tbl_data_count;
	}

	public void setTbl_data_count(int tbl_data_count) {
		this.tbl_data_count = tbl_data_count;
	}

	public double getTbl_size() {
		return tbl_size;
	}

	public void setTbl_size(double tbl_size) {
		this.tbl_size = tbl_size;
	}

	public int getTbl_type() {
		return tbl_type;
	}

	public void setTbl_type(int tbl_type) {
		this.tbl_type = tbl_type;
	}

	public Set<Partition> getTbl_partitions() {
		return tbl_partitions;
	}

	public void setTbl_partitions(Set<Partition> tbl_partitions) {
		this.tbl_partitions = tbl_partitions;
	}
	
	public Set<Integer> getTbl_foreign_tables() {
		return tbl_foreign_tables;
	}

	public void setTbl_foreign_tables(Set<Integer> tbl_foreign_tables) {
		this.tbl_foreign_tables = tbl_foreign_tables;
	}

	public MultiValueMap<Integer, Integer> getTbl_data_map_s() {
		return tbl_data_map_s;
	}

	public void setTbl_data_map_s(MultiValueMap<Integer, Integer> tbl_data_map_s) {
		this.tbl_data_map_s = tbl_data_map_s;
	}

	public MultiKeyMap<Integer, Integer> getTbl_data_map_d() {
		return tbl_data_map_d;
	}

	public void setTbl_data_map_d(MultiKeyMap<Integer, Integer> tbl_data_map_d) {
		this.tbl_data_map_d = tbl_data_map_d;
	}

	public HashMap<Integer, Integer> getTbl_data_pid_map() {
		return tbl_data_pid_map;
	}

	public void setTbl_data_pid_map(HashMap<Integer, Integer> tbl_data_id_map) {
		this.tbl_data_pid_map = tbl_data_id_map;
	}
	
	public HashMap<Integer, Integer> getTbl_pid_data_map() {
		return tbl_pid_data_map;
	}

	public void setTbl_pid_data_map(HashMap<Integer, Integer> tbl_id_data_map) {
		this.tbl_pid_data_map = tbl_id_data_map;
	}

	public int[] getTbl_data_rank() {
		return tbl_data_rank;
	}

	public void setTbl_data_rank(int[] tbl_data_rank) {
		this.tbl_data_rank = tbl_data_rank;
	}

	// Returns the primary key of the corresponding data from the rank table 
	public ArrayList<Integer> getTableData(int rank) {
		ArrayList<Integer> dataIdList = new ArrayList<Integer>();
		int data_id = this.getTbl_data_pid_map().get(this.getTbl_data_rank()[rank]);
		dataIdList.add(data_id);
		
		return dataIdList;
	}

	@SuppressWarnings("unchecked")
	public ArrayList<Integer> getTableData(ArrayList<Integer> keyList) {
		ArrayList<Integer> dataList = new ArrayList<Integer>();
		int rand_selection = 0;
		ArrayList<Integer> data_id = new ArrayList<Integer>();
		int d = -1;
		boolean _null = false;
		
		switch(this.getTbl_type()) {
			case 1:
				for(Integer i : keyList)
					dataList.addAll((Collection<? extends Integer>) getTbl_data_map_s().get(i));
				
				if(dataList.size() > 1) {
					rand_selection = DBMSSimulator.random.nextInt(dataList.size());					
					d = dataList.get(rand_selection);					
				} else
					d = dataList.get(0);				
				
				break;
			case 2:
				if(this.getTbl_data_map_d().get(keyList.get(0), keyList.get(1)) == null)
					_null = true;
				else					
					d = this.getTbl_data_map_d().get(keyList.get(0), keyList.get(1));				
				
				break;
		}
		
		if(_null)
			data_id.add(-1);
		else
			data_id.add(this.getTbl_data_pid_map().get(d));
		
		data_id.add(d);
		
		return data_id;
	}
	
	public Data getData(Database db, int data_id) {
		int local_partition_id = (data_id % db.getDb_dbs().getDbs_nodes().size()) + 1;
		Partition partition = this.getPartition(local_partition_id);
		Data data = partition.getData(data_id);
		
		return data;
	}		
	
	public Partition getPartition(int partition_id) {// search by local partition id from the Table level			
		for(Partition partition : this.getTbl_partitions()) {						
			if(partition.getPartition_id() == partition_id) 
				return partition;
		}	
		
		return null;
	}
	
	public void updateTableLoad() {
		double sum = 0.0d;
		for(Partition partition : this.getTbl_partitions()) {
			sum += partition.getPartition_dataSet().size();
		}
		
		this.setTbl_size(sum);
	}

	public void show() {
		
	}
	
	@Override
	public String toString() {
		return ("T"+this.getTbl_id()+"<"+this.getTbl_name()+">");
	}
	
	@Override
	public boolean equals(Object tbl) {
		if (!(tbl instanceof Table)) {
			return false;
		}
		
		Table table = (Table) tbl;
		return (this.getTbl_name().equals(table.getTbl_name()));
	}

	@Override
	public int hashCode() {
		return (this.getTbl_name().hashCode());
	}

	@Override
	public int compareTo(Table table) {		
		return (((int)this.getTbl_id() < (int)table.getTbl_id()) ? -1: 
			((int)this.getTbl_id() > (int)table.getTbl_id()) ? 1:0);		
	}	
}