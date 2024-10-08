package it.inps.eng.mutui.wscertificazionemutui.model.dao.vega;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import it.inps.eng.mutui.wscertificazionemutui.model.dao.exceptions.DaoException;
import it.inps.eng.wscertificazionemutui.common.beans.AnagraficaIntestatario;
import it.inps.eng.wscertificazionemutui.common.beans.ConfigurazioneApplicazione;
import it.inps.eng.wscertificazionemutui.common.beans.MutuoBean;
import it.inps.eng.wscertificazionemutui.common.beans.Residenza;
import it.inps.eng.wscertificazionemutui.common.utils.FormalCheckUtils;
import it.inps.eng.wscertificazionemutui.common.utils.StringUtils;



public class VegaDao {

	private Connection vegaConn = null;
	
	private Logger logger = Logger.getRootLogger();
	
	public static final String DESCRIZIONE_DG_CERTIFICAZIONI_FISCALI = "DESCRIZIONE_DG_CERTIFICAZIONI_FISCALI";
	public VegaDao(Connection vegaConn) {
		
		super();
		this.vegaConn = vegaConn;
		this.logger = Logger.getRootLogger();
	}
	

	
	public AnagraficaIntestatario getDatiBeneficiarioPdf(String matricola, String cf) throws DaoException {

		AnagraficaIntestatario dbf = new AnagraficaIntestatario(matricola, cf);
		

		if(FormalCheckUtils.isEmptyString(matricola))
			matricola = "";

		try {
			String sql = "SELECT CODCAP AS CAP, COMUNE_RES AS COMUNE, PROVRES AS PROV, COGNOME, NOME, CODFIS, INDIR, NUMEROCIV, SEDEATTUALE FROM V_MT_PERSONALE";
			int suffix = Integer.parseInt(matricola.substring(matricola.length()-2));

			if(suffix>=40) sql = "SELECT CODCAP AS CAP, COMUNE_RESIDENZA AS COMUNE, PRO AS PROV, COGNOME, NOME, CODFIS, INDIR, NUMEROCIV, SEDEATTUALE FROM V_MT_EREDIPERS";
			if((suffix<40) && (suffix>0)) sql = "SELECT CODCAP AS CAP, COMUNE_RES AS COMUNE, PR_RES AS PROV, COGNOME, NOME, CODFIS, INDIR, NUMEROCIV, SEDEATTUALE FROM V_MT_EREDI";

			sql += " WHERE MATRANAG = ?";

			logger.info("getDatiBeneficiarioPdf : " + sql);
			PreparedStatement ps = vegaConn.prepareStatement(sql);
			ps.setString(1,matricola);

			ResultSet rs = ps.executeQuery();
			if(rs.next()) {
				dbf.setCognome((rs.getString("cognome")!=null) ? rs.getString("cognome") : "");
				dbf.setNome((rs.getString("nome")!=null) ? rs.getString("nome") : "");
				Residenza resid = new Residenza();
				resid.setCap((rs.getString("cap")!=null) ? rs.getString("cap") : "");
				resid.setCitta((rs.getString("comune")!=null) ? rs.getString("comune") : "");
				resid.setPrSede((rs.getString("prov")!=null) ? rs.getString("prov") : "");
				
				resid.setCodSede((rs.getString("sedeattuale")!=null) ? rs.getString("sedeattuale") : "");
				resid.setIndirizzo((rs.getString("indir")!=null) ? rs.getString("indir") : "");
				resid.setNumeroCiv((rs.getString("numerociv")!=null) ? rs.getString("numerociv") : "");
				dbf.setResidenza(resid);
			}
			rs.close();
			ps.close();
			return dbf;

		}catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}

	
	public String getIntestazioneSede(String codicesap) throws DaoException {
		String result = "";
		
		try {				
			String sql = "select *, [DES_SEDE_LUNGA] as sede from v_mt_sedi where codicesap = ?";
			PreparedStatement ps = vegaConn.prepareStatement(sql);
			ps.setString(1,codicesap);
			ResultSet rs = ps.executeQuery();

			if(rs.next()) {

				//modifica 23/02/2024 EX
				//Il beneficiario è "INPS -Direzione Generale" 
				//E non "Inps - Direzione Centrale Risorse Umane - Area Servizi Al Cliente Interno" 
				if(rs.getString("COD REGIONE").equalsIgnoreCase("00")) return "Direzione centrale Risorse umane";
				//if(rs.getString("COD REGIONE").equalsIgnoreCase("00")) return "INPS - Direzione Generale";
				 
                else if(rs.getString("codsedreg").equalsIgnoreCase(codicesap)) {
//				                	String tipo="Direzione regionale ";
//				    				String sede=StringUtils.notNull(rs.getString("regione").trim());
				                	String tipo=StringUtils.notNull(rs.getString("DES_TIPO").trim());
				    				String sede=StringUtils.notNull(rs.getString("sede").trim());
				    				logger.info("1 DES_TIPO : " + rs.getString("DES_TIPO").trim());
				    				logger.info("1 SEDE : " + rs.getString("sede").trim());
				    				result = StringUtils.tipoSedeResult(tipo, sede);
                                   
                                        }
                        else {
                        	if(codicesap.substring(codicesap.length()-2).equalsIgnoreCase("00")){
                        		//result = "Direzione provinciale
                        		String tipo=StringUtils.notNull(rs.getString("DES_TIPO").trim());
                				String sede=StringUtils.notNull(rs.getString("sede").trim());
                				logger.info("2 DES_TIPO : " + rs.getString("DES_TIPO").trim());
			    				logger.info("2 SEDE : " + rs.getString("sede").trim());
                				result = StringUtils.tipoSedeResult(tipo, sede);
                        	}
                    else {
                        String codiceProv = codicesap.substring(0,4)+"00";
                        Statement st = vegaConn.createStatement();
                        ResultSet rsP = st.executeQuery("select [DES_TIPO], [DES_SEDE_LUNGA] as sede from v_mt_sedi where codicesap = '"+codiceProv+"'");
                        if(rsP.next()){ 
                        	
                        	String tipo=StringUtils.notNull(rsP.getString("DES_TIPO").trim());
                			String sede=StringUtils.notNull(rsP.getString("sede").trim());
                			logger.info("3 DES_TIPO : " + rs.getString("DES_TIPO").trim());
		    				logger.info("3 SEDE : " + rs.getString("sede").trim());
                			result = StringUtils.tipoSedeResult(tipo, sede);
                        }
                        	rsP.close();
                    }
                }
            }
			rs.close();
		ps.close();
		return result;
		
	}catch(Exception e) {
		throw new DaoException(e);
	}
	finally {
		//con.close();
	}
	
}
	
	
	
	public boolean isDirGenerale(String codicesap) throws DaoException {
		boolean result = false;
		try {				
								
			String sql = "select *, [COD REGIONE] as codReg from v_mt_sedi where codicesap = ?";
			PreparedStatement ps = vegaConn.prepareStatement(sql);
			ps.setString(1,codicesap);
			ResultSet rs = ps.executeQuery();

			if(rs.next()) {
				if(rs.getString("codReg").equalsIgnoreCase("00")) {
					result = true;
				}
			}
		
			rs.close();
		ps.close();
		return result;
		
		}catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}
	
//	public List<AnagraficaIntestatario> getMatricoleMutui(String codiceFis) throws DaoException {
//
//		List<AnagraficaIntestatario> res = new ArrayList<AnagraficaIntestatario>();
//
//		try {
//			String sql =  " select u.matranag as matricola, u.CODFIS, u.nome, u.cognome "
//					+ " from "
//					+ " (SELECT matranag, CODFIS, nome, cognome "
//					+ " FROM V_MT_PERSONALE  "
//					+ " Union "
//					+ " SELECT matranag, CODFIS,nome, cognome "
//					+ " FROM V_MT_EREDI "
//					+ " Union "
//					+ " SELECT matranag, CODFIS,nome, cognome "
//					+ " FROM V_MT_EREDIPERS "
//					+ " ) u "
//					+ " where u.CODFIS ='"+codiceFis+"'";
//			
////			String sql =  " select CF1.ANNO_CERT, CF1.ANNO_MUTUO, CF1.PROG_MUTUO, CF1.COD_LET, DESC_LET, CF1.CODFIS_DEC, CF1.FL_RETT, "
////					+ " case when FL_RETT = 0 then ''  else 'Certificazione sostitutiva' END AS DESC_RETT, CF1.ID "  
////					+ " from CERTIFICAZIONI_FISCALI CF1  inner join(select ANNO_CERT, CODFIS_BEN, ANNO_MUTUO, PROG_MUTUO,max(DT_ORA_INSER)as DT_ORA_INSER " 
////					+ " from CERTIFICAZIONI_FISCALI where CODFIS_BEN ='"+codiceFis+"' group by ANNO_CERT, ANNO_MUTUO, PROG_MUTUO, CODFIS_BEN) CF2 " 
////					+ " on CF1.ANNO_MUTUO = CF2.ANNO_MUTUO and CF1.PROG_MUTUO = CF2.PROG_MUTUO and CF1.CODFIS_BEN = CF2.CODFIS_BEN and CF1.DT_ORA_INSER = CF2.DT_ORA_INSER inner  "
////					+ " join TipoLettera TL on TL.COD_LET = CF1.COD_LET where FL_VISIBLE = 1 and CF1.ANNO_CERT>2017 order by CF1.ANNO_CERT, CF1.ANNO_MUTUO, CF1.PROG_MUTUO, CF1.COD_LET";
//
//			PreparedStatement ps = vegaConn.prepareStatement(sql);
//
//			logger.debug("getMatricoleMutui query" + sql);
//			ResultSet rs = ps.executeQuery();
//
//			while(rs.next()){
//				AnagraficaIntestatario p = new AnagraficaIntestatario(rs.getString("matricola"),codiceFis);
//				p.setNome(rs.getString("nome"));
//				p.setCognome(rs.getString("cognome"));
//				res.add(p);
//			}
//			rs.close();	
//			ps.close();
//			return res;
//		}			
//		catch(SQLException e) {
//			throw new DaoException(e);
//		}
//		finally {
//			//con.close();
//		}
//
//	}

	
	
}
