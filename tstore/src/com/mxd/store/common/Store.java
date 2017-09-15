package com.mxd.store.common;

import java.io.Serializable;
import java.util.List;
public class Store implements Serializable{
	
	private static final long serialVersionUID = 765401815416356193L;

	private String name;
	
	private List<Column> columns;
	
	public Store() {
		super();
	}
	public Store(String name, List<Column> columns) {
		super();
		this.name = name;
		this.columns = columns;
	}

	public String getName() {
		return name;
	}

	public List<Column> getColumns() {
		return columns;
	}
	
	public int getStoreUnitSize(){
		int ret = 0;
		for (Column column : this.columns) {
			ret+=column.getLength();
			if(column.getType()==Column.Type.STRING){
				ret+=4;
			}
		}
		return ret;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
	@Override
	public String toString() {
		return "Store [name=" + name + ", columns=" + columns + "]";
	}
}
