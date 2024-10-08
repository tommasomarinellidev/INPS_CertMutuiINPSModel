package it.inps.eng.mutui.wscertificazionemutui.model.dao.mutuifin.dettagliocertificazioni;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


import it.inps.eng.mutui.wscertificazionemutui.model.dao.GenericDao;
import it.inps.eng.mutui.wscertificazionemutui.model.dao.exceptions.DaoException;
import it.inps.eng.wscertificazionemutui.common.beans.certificazionefiscale.DettaglioCertificazioni;

public class DettaglioCertificazioniDao extends GenericDao{
	


	public DettaglioCertificazioniDao(Connection mutuifinConn) {
		
		super(mutuifinConn);
	}
	
	
	public boolean insert (DettaglioCertificazioni dettCert) throws DaoException {
		
		String sqlInsDettCert = "insert into DETTAGLIO_CERTIFICAZIONI (" +
				"AnnoCertificazione, " + 
				"DataEsecuzione, " +
				"AnnoMutuo, " +
				"ProgressivoMutuo, " +
				"Matricola, " +
				"QuotaCapitaleCertFisc, " +
				"InteressiPassiviCertFisc, " +
				"InteressiPreammCertFisc, " +
				"SpeseIstruttoriaCertFisc, " +
				"ImpostaSostitutivaCertFisc, " +
				"PercentualeIntestazione, " +
				"TipoBeneficiario, cod_let, DataInizioPeriodo, DataFinePeriodo) "
				+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";			
		
		logger.info("sqlInsDettCert:" + sqlInsDettCert + "(" + dettCert.getAnnoCertificazione() + ", " +
				dettCert.getDataEsecuzione() + ", " +
				dettCert.getAnnoMutuo() + ", " + 
				dettCert.getProgressivoMutuo() + ", " + 
				dettCert.getMatricola() + ", " + 
				dettCert.getQuotaCapitaleCertFisc() + ", " + 
				dettCert.getInteressiPassiviCertFisc() + ", " + 
				dettCert.getInteressiPreammCertFisc() + ", " +
				dettCert.getSpeseIstruttoriaCertFisc() + ", " + 
				dettCert.getImpostaSostitutivaCertFisc() + ", " + 
				dettCert.getPercentualeIntestazione() + ", " +
				dettCert.getTipoBeneficiario() + ", " +
				dettCert.getCodLet() + ", " +
				dettCert.getDataInizioPeriodo() + ", " +
				dettCert.getDataFinePeriodo() + ") ");
		
		PreparedStatement ps = null;
		PreparedStatement ps1 = null;
		
		String sql = "select Matricola from DETTAGLIO_CERTIFICAZIONI d where AnnoCertificazione='"+ dettCert.getAnnoCertificazione() +"' "
				+ "and DataEsecuzione='"+dettCert.getDataEsecuzione() + "' and AnnoMutuo='"+dettCert.getAnnoMutuo()+"' and ProgressivoMutuo='"+dettCert.getProgressivoMutuo()+"' "
						+ "and Matricola='"+dettCert.getMatricola()+"' and cod_let='"+dettCert.getCodLet()+"' and DataInizioPeriodo='"+dettCert.getDataInizioPeriodo()+"'";
	
		try {
			ps1 = mutuifinConn.prepareStatement(sql);
			 ResultSet rs1 = ps1.executeQuery();
			 
			if(rs1.next()){
				rs1.close();
				ps1.close();
				return false;
			}	    
			
			ps = mutuifinConn.prepareStatement(sqlInsDettCert);
		
			ps.setString(1, dettCert.getAnnoCertificazione()); 
			ps.setTimestamp(2, dettCert.getDataEsecuzione());
			ps.setString(3, dettCert.getAnnoMutuo());
			ps.setString(4, dettCert.getProgressivoMutuo()); 
			ps.setString(5, dettCert.getMatricola()); 
			ps.setDouble(6, dettCert.getQuotaCapitaleCertFisc()); 
			ps.setDouble(7, dettCert.getInteressiPassiviCertFisc());
			ps.setDouble(8, dettCert.getInteressiPreammCertFisc());
			ps.setDouble(9, dettCert.getSpeseIstruttoriaCertFisc()); 
			ps.setDouble(10, dettCert.getImpostaSostitutivaCertFisc()); 
			ps.setDouble(11, dettCert.getPercentualeIntestazione()); 
			ps.setString(12, dettCert.getTipoBeneficiario());
			ps.setString(13, dettCert.getCodLet());
			ps.setTimestamp(14, dettCert.getDataInizioPeriodo());
			ps.setTimestamp(15, dettCert.getDataFinePeriodo());
			ps.executeUpdate();
			ps.close();
		}catch(SQLException e) {
			throw new DaoException(e);
		}finally {
				//con.close();
		}
		return true;
	}
}
