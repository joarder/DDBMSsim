/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.db;

import java.util.Set;
import java.util.TreeSet;

public class Table implements Comparable<Table>{
	private int tbl_id;
	private int tbl_db_id;
	private String tbl_name;
	private Set<Partition> tbl_partitions;
	
	public Table(int id, int db_id, String name) {
		this.setTbl_id(id);
		this.setTbl_db_id(db_id);
		this.setTbl_name(name);
		this.setTbl_partitions(new TreeSet<Partition>());
	}
	
	// Copy Constructor
	public Table(Table table) {
		this.setTbl_id(table.getTbl_id());
		this.setTbl_db_id(table.getTbl_db_id());
		this.setTbl_name(table.getTbl_name());
		
		Set<Partition> clone_partitions = new TreeSet<Partition>();		
		for(Partition clonePartition : table.getTbl_partitions())
			clone_partitions.add(clonePartition);
		this.setTbl_partitions(clone_partitions);		
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

	public Set<Partition> getTbl_partitions() {
		return tbl_partitions;
	}

	public void setTbl_partitions(Set<Partition> tbl_partitions) {
		this.tbl_partitions = tbl_partitions;
	}
	
	public Partition getPartition(int partition_id) {		
		for(Partition partition : this.getTbl_partitions()) {						
			if(partition.getPartition_id() == partition_id) 
				return partition;
		}	
		
		return null;
	}

	public void show() {
		
	}
	
	@Override
	public String toString() {
		return (this.getTbl_name());
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