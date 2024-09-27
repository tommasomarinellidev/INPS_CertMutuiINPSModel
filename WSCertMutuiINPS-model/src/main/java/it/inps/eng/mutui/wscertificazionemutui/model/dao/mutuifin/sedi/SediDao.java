package it.inps.eng.mutui.wscertificazionemutui.model.dao.mutuifin.sedi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import it.inps.eng.mutui.wscertificazionemutui.model.dao.GenericDao;
import it.inps.eng.mutui.wscertificazionemutui.model.dao.exceptions.DaoException;
import it.inps.eng.mutui.wscertificazionemutui.model.dao.vega.VegaDao;
import it.inps.eng.mutui.wscertificazionemutui.model.utils.ConnectionUtil;
import it.inps.eng.wscertificazionemutui.common.beans.Sedi;
import it.inps.eng.wscertificazionemutui.common.utils.FormalCheckUtils;
import it.inps.eng.wscertificazionemutui.common.utils.StringUtils;



public class SediDao  extends GenericDao{



	public SediDao(Connection mutuifinConn) {

		super(mutuifinConn);
	}

	

	public String getDirettoreSede(String codiceSede) throws DaoException {

		String direttoreSede = "";

		try {
			// EXPRIVIA MOD
//			if(codiceSede.startsWith("00")){
//				direttoreSede="Giovanni Di Monde";
//			}
//			else {
				String sql = "EXEC GetDirettoreSede @CodiceSede = '"+codiceSede+"'";
				PreparedStatement ps = mutuifinConn.prepareStatement(sql);
				ResultSet rs = ps.executeQuery();

				if(rs.next()){
					direttoreSede=rs.getString("Nome").trim()+" "+rs.getString("Cognome").trim();
				}	    
				rs.close();
				ps.close();
//			}
			return direttoreSede;
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}
	
	//EXPRIVIA MOD
	public String getDirettoreSedeCertificazione(String codiceSede, String annoMese) throws DaoException {

		String direttoreSede = "";

		try {
			// EXPRIVIA MOD

				String sql = "EXEC GetDirettoreSedexCertificazioni @CodiceSede = '"+codiceSede+"' , @data ='"+annoMese+"'";
				System.out.println("+++getDirettoreSedeCertificazione: "+ sql);
				PreparedStatement ps = mutuifinConn.prepareStatement(sql);
				ResultSet rs = ps.executeQuery();

				if(rs.next()){
					direttoreSede=rs.getString("Nome").trim()+" "+rs.getString("Cognome").trim();
				}	    
				rs.close();
				ps.close();

			return direttoreSede;
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}
	
	public String getDenominazioneDirettoreGenerale() throws DaoException {
		String denominazione = null;
		try {				
			
			String sql = "select valore from ConfigurazioneApplicazione where chiave = 'DirettoreGenerale'";
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			if(rs.next()) {
				denominazione = rs.getString("valore");
			}
		
			rs.close();
		ps.close();
		return denominazione;
		
		}catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	
	public String getDirettoreMatricola(String codiceSede, String annoMese) throws DaoException {

		String direttoreMatricola = "";

		try {
			// EXPRIVIA MOD

			String sql = "EXEC GetDirettoreSedexCertificazioni @CodiceSede = '"+codiceSede+"' , @data ='"+annoMese+"'";
			System.out.println("+++getDirettoreSedeCertificazione: "+ sql);
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			if(rs.next()){
				direttoreMatricola=rs.getString("Matricola").trim()+";"+rs.getString("tipoSede").trim();
			}	    
			rs.close();
			ps.close();

			return direttoreMatricola;
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}
	
	public String getRegioneDelMutuo(String codiceSede, String annoMese, Connection vegaCon) throws DaoException {
		if(vegaCon == null) //inizializza la connessione
			vegaCon = ConnectionUtil.getInstance().getVegaConnection();
			String result = "";			
			try {				
				String sql = "select * from v_mt_sedi where codicesap = ?";
				PreparedStatement ps = vegaCon.prepareStatement(sql);
				ps.setString(1,codiceSede);
				ResultSet rs = ps.executeQuery();
				if(rs.next()) {
					result = rs.getString("COD REGIONE");
	            }
				rs.close();
			ps.close();
			return result;
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}
    		
    public String getSedeParentDelMutuo(String regione, Connection vegaCon) throws DaoException {
		if(vegaCon == null) //inizializza la connessione
			vegaCon = ConnectionUtil.getInstance().getVegaConnection();
			String result = "";			
			try {				
				String sql = "select * from v_mt_sedi where tipo in ('80','81') AND [COD REGIONE]=?";
				PreparedStatement ps = vegaCon.prepareStatement(sql);
				ps.setString(1,regione);
				ResultSet rs = ps.executeQuery();
				if(rs.next()) {
					result = rs.getString("CODICESAP");			                
	            }
				rs.close();
			ps.close();
			return result;
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	//EXPRIVIA MOD
	public String getDirettoreSedeDescrizione(String codiceSede) throws DaoException {

		String descrizioneAttivita = "";

		try {
				String sql = "EXEC GetDirettoreSede @CodiceSede = '"+codiceSede+"'";
				PreparedStatement ps = mutuifinConn.prepareStatement(sql);
				ResultSet rs = ps.executeQuery();

				if(rs.next()){
					descrizioneAttivita=rs.getString("descrizioneAttivita");
				}	    
				rs.close();
				ps.close();
//			}
			return descrizioneAttivita;
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}
	
	//EXPRIVIA MOD
	public String getNomeSede(String codiceSede) throws DaoException {

		String nomeSede = "";

		try {
			// EXPRIVIA MOD
//			if(codiceSede.startsWith("00")){
//				direttoreSede="Giovanni Di Monde";
//			}
//			else {
				String sql = "EXEC GetDirettoreSede @CodiceSede = '"+codiceSede+"'";
				PreparedStatement ps = mutuifinConn.prepareStatement(sql);
				ResultSet rs = ps.executeQuery();

				if(rs.next()){
					nomeSede=rs.getString("descrizioneTipoSede");
				}	    
				rs.close();
				ps.close();
//			}
			return nomeSede;
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}

	

	public List<String> getSedeTipoPagam(String annoMutuo, String progMutuo, String matricola) throws DaoException {

		List<String> sbList = new ArrayList<String>();	
		if(FormalCheckUtils.isEmptyString(matricola))
			matricola = "";

		try {
			String sql = "EXEC GetSedeMutuo @ANNO_MUTUO='"+annoMutuo+"', @PROG_MUTUO='"+progMutuo+"', @MTR='"+matricola+"'";
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			if(rs.next()){
				sbList.add(0,rs.getString("SEDE"));
				sbList.add(1,rs.getString("TP_PGM"));
			}

			rs.close();	
			ps.close();	
			return sbList;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}
	
	public String getDenominazioneAreaDCRU() throws DaoException {
		String denominazione = null;
		try {				
			
			String sql = "select valore from ConfigurazioneApplicazione where chiave = 'DenominazioneAreaDCRU'";
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			if(rs.next()) {
				denominazione = rs.getString("valore");
			}
		
			rs.close();
		ps.close();
		return denominazione;
		
		}catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}
	
	public String getDirigenteAreaDCRU() throws DaoException {
		String denominazione = null;
		try {				
			
			String sql = "select valore from ConfigurazioneApplicazione where chiave = 'DirigenteAreaDCRU'";
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			if(rs.next()) {
				denominazione = rs.getString("valore");
			}
		
			rs.close();
		ps.close();
		return denominazione;
		
		}catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}
	
	public Sedi selezionaSede(String sede) throws DaoException {


		try {
			// statement
			PreparedStatement ps = mutuifinConn.prepareStatement("SELECT * FROM SEDI WHERE SEDE = ?");
			// cerca la sede
			ps.setString(1, sede);

			Sedi s = new Sedi();

			ResultSet rs =  ps.executeQuery();
			// se sede trovata restituisci i valori presenti su DB sedi

			if (rs.next())
			{
				s.setSede(rs.getString("SEDE"));
				s.setInd(rs.getString("IND"));
				s.setCap(rs.getString("CAP"));
				s.setCitta(rs.getString("CITTA"));
				s.setPr(rs.getString("PR"));
				s.setTel(rs.getString("TEL"));
				s.setFax(rs.getString("FAX"));
				s.setAbi(rs.getString("ABI"));
				s.setCab(rs.getString("CAB"));
				s.setCin(rs.getString("CIN"));
				s.setCc(rs.getString("CC"));
				s.setDeno_banca(rs.getString("DENO_BANCA"));
				s.setDeno_agenzia(rs.getString("DENO_AGENZIA"));
				s.setInd_agenzia(rs.getString("IND_AGENZIA"));
				s.setCap_agenzia(rs.getString("CAP_AGENZIA"));  
				s.setCitta_agenzia(rs.getString("CITTA_AGENZIA"));
				s.setPr_agenzia(rs.getString("PR_AGENZIA"));
				s.setEmail(rs.getString("EMAIL"));
				s.setNomeDirettore(rs.getString("NOM_DIR"));					
				s.setCodicePaese(rs.getString("CODICE_PAESE"));
				s.setCinEuro(rs.getString("CIN_EURO"));
				s.setIban(rs.getString("IBAN"));		        		        		        

			}

			rs.close();
			ps.close();		
			return s;
		}
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}


}
