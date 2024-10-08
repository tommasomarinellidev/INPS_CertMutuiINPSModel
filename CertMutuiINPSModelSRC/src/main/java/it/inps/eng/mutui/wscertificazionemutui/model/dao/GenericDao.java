package it.inps.eng.mutui.wscertificazionemutui.model.dao;

import java.sql.Connection;

import org.apache.log4j.Logger;

public abstract class GenericDao {
	
	protected Connection mutuifinConn = null;
	protected Logger logger = Logger.getRootLogger();
	
	
	public GenericDao(Connection mutuifinConn) {
		super();
		this.mutuifinConn = mutuifinConn;
		this.logger = Logger.getRootLogger();
	}

}
