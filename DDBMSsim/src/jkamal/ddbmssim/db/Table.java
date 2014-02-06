/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.db;

import java.util.Set;

public class Table implements Comparable<Table>{
	private int tbl_id;
	private String tbl_name;
	private int tbl_db_id;
	private Set<Data> tbl_data_set;
	private Set<Integer> tbl_partitions;
	
	public Table() {
		
	}
	
	public Table(Table table) {
		
	}
	
	public int getTbl_id() {
		return tbl_id;
	}

	public void setTbl_id(int tbl_id) {
		this.tbl_id = tbl_id;
	}

	public String getTbl_name() {
		return tbl_name;
	}

	public void setTbl_name(String tbl_name) {
		this.tbl_name = tbl_name;
	}

	public int getTbl_db_id() {
		return tbl_db_id;
	}

	public void setTbl_db_id(int tbl_db_id) {
		this.tbl_db_id = tbl_db_id;
	}

	public Set<Data> getTbl_data_set() {
		return tbl_data_set;
	}

	public void setTbl_data_set(Set<Data> tbl_data_set) {
		this.tbl_data_set = tbl_data_set;
	}

	public Set<Integer> getTbl_partitions() {
		return tbl_partitions;
	}

	public void setTbl_partitions(Set<Integer> tbl_partitions) {
		this.tbl_partitions = tbl_partitions;
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