package it.inps.eng.mutui.wscertificazionemutui.model.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class ConnectionUtil {
	
	Logger logger = Logger.getRootLogger();

	private static ConnectionUtil INSTANCE;
	
	protected Connection conn = null;
	protected String DB2owner;
	
			
	private ConnectionUtil() {}
	
	
	public static ConnectionUtil getInstance() {
		if (INSTANCE == null) INSTANCE = new ConnectionUtil();
		return INSTANCE;
	}

	
	
	private Properties getProperties(){
		// Carica le properties dal file
		Properties p = new Properties();
		try {
			//p.load(new FileInputStream(OneriDeducibiliHandler.getBasePath()+OneriDeducibili.dbPropFile));
		}
		catch(Exception e) {
			logger.log(Level.ERROR, e);
		}
		return p;
	}
	
	private Connection getConnection(String driver, String url, String usr, String pwd){
		
		Connection conn = null;
		
		try {
			
			// 1. Driver
			if("".equals(driver)) throw new Exception("driver connessione mancante");
			Class.forName(driver);
			
			// 2. URL
			if("".equals(url)) throw new Exception("url connessione mancante");
			
			// 3. User & Pwd
			if(!"".equals(usr)) {
				if("".equals(pwd)) throw new Exception("password connessione mancante");

				// Connette con coppia User/Pwd	
				conn = DriverManager.getConnection(url, usr, pwd);				
			}
			else {
				// Connessione senza User/Pwd
				conn = DriverManager.getConnection(url);
			}
			
			// Imposta l'autocommit a false, commit manuali
			conn.setAutoCommit(false);
			logger.debug("Connessione stabilita.");
		}
		catch(Exception e) {
			logger.error("Errore recupero connessione", e);
		}

		return conn;
		
	}

	public Connection getMutuiConnection(){

		Properties p = getProperties();
		
		String driver = p.getProperty("it.inps.mutuifin.driver", "").trim();
		String url = p.getProperty("it.inps.mutuifin.url", "").trim();
		String usr = p.getProperty("it.inps.mutuifin.username", "");
		String pwd = p.getProperty("it.inps.mutuifin.password", "");
		
		Connection conn = getConnection(driver, url, usr, pwd);

		return conn;
		
	}
	
	public Connection getVegaConnection(){
		
		Properties p = getProperties();
		
		String driver = p.getProperty("it.inps.vega.driver", "").trim();
		String url = p.getProperty("it.inps.vega.url", "").trim();
		String usr = p.getProperty("it.inps.vega.username", "");
		String pwd = p.getProperty("it.inps.vega.password", "");
		
		Connection conn = getConnection(driver, url, usr, pwd);

		return conn;
		
	}
	
	public Connection getMutuiSpdaiConnection(){

		Properties p = getProperties();
		
		String driver = p.getProperty("it.inps.mutuispdai.driver", "").trim();
		String url = p.getProperty("it.inps.mutuispdai.url", "").trim();
		String usr = p.getProperty("it.inps.mutuispdai.username", "");
		String pwd = p.getProperty("it.inps.mutuispdai.password", "");
		
		Connection conn = getConnection(driver, url, usr, pwd);

		return conn;
		
	}	
	
}
