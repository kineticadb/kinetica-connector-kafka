package com.kinetica.kafka;

import java.util.HashMap;

import com.gpudb.Type.Column;

/*
 *  A KineticaFieldMapper Utility to keep track of Schema Evolution impact on underlying Kinetica table  
 */
public class KineticaFieldMapper {

	private String tableName;
	private Integer version;
	private HashMap<String, Column> missing;
	private HashMap<String, Column> mapped;
	private HashMap<String, String> lookup;
	
	/**
	 * Create a new blank KineticaFieldMapper instance
	 */
	public KineticaFieldMapper() {
		this.missing = new HashMap<String, Column>();
		this.mapped = new HashMap<String, Column>();
		this.lookup = new HashMap<String, String>();
	}

	/**
	 * Create a new KineticaFieldMapper instance
	 * 
	 * @param tableName Kinetica table to be populated with Kafka record
	 * @param version   Kafka record schema version (may be null)
	 */
	public KineticaFieldMapper(String tableName, Integer version) {
		this.tableName = tableName;
		this.version = version;
		this.missing = new HashMap<String, Column>();
		this.mapped = new HashMap<String, Column>();
		this.lookup = new HashMap<String, String>();
	}
	
	/**
	 * 
	 * @return Kinetica table to be populated with Kafka record
	 */
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	/**
	 * 
	 * @return schema version of Kafka record
	 */
	public Integer getVersion() {
		return version;
	}
	public void setVersion(Integer version) {
		this.version = version;
	}
	/**
	 * 
	 * @return a map of table columns missing from Kafka record
	 */
	public HashMap<String, Column> getMissing() {
		return missing;
	}
	public void setMissing(HashMap<String, Column> missing) {
		this.missing = missing;
	}
	/**
	 * 
	 * @return a map of columns mapped to Kinetica table
	 */
	public HashMap<String, Column> getMapped() {
		return mapped;
	}
	public void setMapped(HashMap<String, Column> mapped) {
		this.mapped = mapped;
	}

	/**
	 * 
	 * @return a map of Kinetica table columns mapped to Kafka fields
	 */
	public HashMap<String, String> getLookup() {
		return lookup;
	}
	public void setLookup(HashMap<String, String> lookup) {
		this.lookup = lookup;
	}
	
	
}
