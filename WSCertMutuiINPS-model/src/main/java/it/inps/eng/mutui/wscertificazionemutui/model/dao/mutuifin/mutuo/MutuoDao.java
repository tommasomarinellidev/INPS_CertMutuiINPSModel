package it.inps.eng.mutui.wscertificazionemutui.model.dao.mutuifin.mutuo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import it.inps.eng.mutui.wscertificazionemutui.model.dao.GenericDao;
import it.inps.eng.mutui.wscertificazionemutui.model.dao.exceptions.DaoException;
import it.inps.eng.wscertificazionemutui.common.beans.MutuoDaCertificare;
import it.inps.eng.wscertificazionemutui.common.utils.DateUtils;



public class MutuoDao  extends GenericDao{



	public MutuoDao(Connection mutuifinConn) {

		super(mutuifinConn);
	}



	/**
	 * Metodo utilizzato sia per reperire la lista delle certificazioni sostituendo null ai parametri di input
	 * sia per reperire i dati della singola certificazione, passando valorizzati i parametri di input
	 * @param annoMutuo
	 * @param progMutuo
	 * @param sede
	 * @return
	 * @throws DaoException
	 */
	public List<MutuoDaCertificare> genMassivaSingolaCertFisc(String annoMutuo, String progMutuo, String sede) throws DaoException{

		List<MutuoDaCertificare> mdcList = new ArrayList<MutuoDaCertificare>();


		String anno_corrente=DateUtils.getNormalizatedDate(new Date(),"yyyy");
		String stringSede="";
		String stringAnnoProgMutuo="";
		if(!"".equals(sede) && sede!=null){
			stringSede="' and rSede.SEDE='"+sede+"'";
		}

		if(!"".equals(annoMutuo) && annoMutuo!=null && !"".equals(progMutuo) && progMutuo!=null){
			stringAnnoProgMutuo=" and m.ANNO_MUTUO='"+annoMutuo+"'and m.PROG_MUTUO='"+progMutuo+"' ";
		}


		try{

			String sql="SELECT DISTINCT m.ANNO_MUTUO, m.PROG_MUTUO, m.ST_MUTUO, m.TP_MUTUO, m.IMP_MUTUO, "
					+"m.DT_STI, m.DT_CHS, tm.FLAG_GEST_ACCONTI, tm.TP_AGENZIA_ENTRATE "
					+"FROM  "
					+"Mutuo m inner join  "
					+"rate r on m.ANNO_MUTUO = r.ANNO_MUTUO and m.PROG_MUTUO = r.PROG_MUTUO "
					+"inner join "
					+"(select r2.ANNO_MUTUO, r2.PROG_MUTUO, r2.SEDE, r2.TP_PGM "
					+"from rate r2 inner join "
					+"(select ANNO_MUTUO, PROG_MUTUO, MAX(PROG_RT) as PROG_RT "
					+"from Rate  "
					+"where ST_RT <> '00' "
					+"group by ANNO_MUTUO, PROG_MUTUO) r1 "
					+"on r2.ANNO_MUTUO = r1.ANNO_MUTUO and r2.PROG_MUTUO = r1.PROG_MUTUO and r2.PROG_RT = r1.PROG_RT "
					+") rSede "
					+"on rSede.ANNO_MUTUO = r.ANNO_MUTUO and rSede.PROG_MUTUO = r.PROG_MUTUO "
					+"inner join TipoMutuo tm on tm.TP_MUTUO = m.TP_MUTUO "
					+"WHERE year(r.DT_PGM_RT)='"+anno_corrente+"' and DetrazioneFiscale = 1  "+stringAnnoProgMutuo+stringSede+" "
					+"ORDER BY m.ANNO_MUTUO, m.PROG_MUTUO ";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuoDaCertificare mdc = new MutuoDaCertificare();
				mdc.setAnnoMutuo(rs.getString("ANNO_MUTUO"));
				mdc.setProgMutuo(rs.getString("PROG_MUTUO"));
				mdc.setStMutuo(rs.getString("ST_MUTUO"));
				mdc.setImpMutuo(rs.getDouble("IMP_MUTUO"));
				mdc.setTpAgenziaEntrate(rs.getString("TP_AGENZIA_ENTRATE")); 
				mdc.setFlagGestAcconti(rs.getBoolean("FLAG_GEST_ACCONTI"));
				mdc.setDtSti(rs.getDate("DT_STI"));
				mdc.setDtChs(rs.getDate("DT_CHS"));
				mdc.setTpMutuo(rs.getInt("TP_MUTUO"));

				mdcList.add(mdc);
			}

			rs.close();
			ps.close();	

			return mdcList;
		}
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
		
		

	}


	public String getDescrizioneCert(String tpMutuo) throws DaoException {

		String descCert = "";
		if(tpMutuo.length()==1){
			tpMutuo="0"+tpMutuo;
		}

		try {
			String sql = "select desc_cert from TipoMutuo tm where tm.tp_mutuo = '"+tpMutuo+"'";
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			if(rs.next()){
				descCert=rs.getString("desc_cert");
			}	    
			rs.close();
			ps.close();
			return descCert;
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}
	
	public boolean isTitolareMutuoDeceduto(String annoMutuo, String progMutuo) throws DaoException{
		boolean result = false;
		try{

			String sql = "SELECT * from dbo.StoriaBeneficiari where ANNO_MUTUO ='" + annoMutuo.trim()
					+ "' and PROG_MUTUO = '" + progMutuo.trim() + "' and TP_BEN = 'E' ";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			result = false;
			while(rs.next()){
				result= true;
			}

			rs.close();
			ps.close();	

			
		}
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
		
		return result;
	}
	
	public boolean existingMoreEredi(String annoMutuo, String progMutuo) throws DaoException {
		boolean result = false;
		try {

			String sql = "SELECT * from dbo.StoriaBeneficiari where ANNO_MUTUO ='" + annoMutuo.trim()
					+ "' and PROG_MUTUO = '" + progMutuo.trim() + "' and TP_BEN = 'E'  and FLAG_INT = 'N'";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			result = false;
			while (rs.next()) {
				result = true;
			}

			rs.close();
			ps.close();

		} catch (Exception e) {
			throw new DaoException(e);
		} finally {
			// con.close();
		}
		return result;
	}

	public String[] getCognomeNomeIntestatarioDeceduto(String annoMutuo, String progMutuo) throws DaoException {
		String result[] = { "", "" };
		try {
			String sql = "SELECT   titolare.nome, titolare.cognome from dbo.Beneficiari titolare "
					+ "left join dbo.StoriaBeneficiari st on titolare.MTR = st.MTR   " + "where st.ANNO_MUTUO = '"
					+ annoMutuo.trim() + "' and st.PROG_MUTUO = '" + progMutuo.trim() + "' "
					+ "and TP_BEN = 'T' and st.TP_EVN= '07' and st.PERC =0 and right(st.mtr,2) <'40' ";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			while (rs.next()) {
				result[0] = rs.getString("cognome");
				result[1] = rs.getString("nome");

			}

			rs.close();
			ps.close();

		} catch (Exception e) {
			throw new DaoException(e);
		} finally {

		}
		return result;

	}
	
	public String getCodiceFiscaleIntestatarioDeceduto(String annoMutuo, String progMutuo) throws DaoException {
		String result = "";
		try {
			String sql = "SELECT " + 
					"anag.CODFIS  " + 
					"from " + 
					"	dbo.Beneficiari titolare " + 
					"left join dbo.StoriaBeneficiari st on " + 
					"	titolare.MTR = st.MTR " + 
					"left join dbo.V_ANAGRAFICA  anag on " + 
					"titolare.MTR  COLLATE SQL_Latin1_General_CP1_CI_AS =  anag.MATRANAG  COLLATE SQL_Latin1_General_CP1_CI_AS " + 
					"where " + 
					"	st.ANNO_MUTUO = '"+ annoMutuo.trim() + "' " + 
					"	and st.PROG_MUTUO = '" + progMutuo.trim() + "' " + 
					"	and TP_BEN = 'T' " + 
					"	and st.TP_EVN = '07' " + 
					"	and st.PERC = 0 " + 
					"	and right(st.mtr, 2) <'40'";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			while (rs.next()) {
				result= rs.getString("CODFIS");
			}

			rs.close();
			ps.close();

		} catch (Exception e) {
			throw new DaoException(e);
		} finally {

		}
		return result;

	}
	
	public Date getDataMorteIntestatarioDeceduto(String annoMutuo, String progMutuo) throws DaoException {
		Date result = null;
		try {
			String sql = "SELECT " + 
					"	titolare.DT_MOR  " + 
					"from " + 
					"	dbo.Beneficiari titolare " + 
					"left join dbo.StoriaBeneficiari st on " + 
					"	titolare.MTR = st.MTR " + 
					"where " + 
					"	st.ANNO_MUTUO =  '"+ annoMutuo.trim() + "' " +  
					"	and st.PROG_MUTUO = '" + progMutuo.trim() + "' " +  
					"	and TP_BEN = 'T' " + 
					"	and st.TP_EVN = '07' " + 
					"	and st.PERC = 0 " + 
					"	and right(st.mtr, 2) <'40'";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			while (rs.next()) {
				result= rs.getDate("DT_MOR");
			}

			rs.close();
			ps.close();

		} catch (Exception e) {
			throw new DaoException(e);
		} finally {

		}
		return result;

	}
	
	public String getTipoMutuo(String annoMutuo, String progMutuo) throws DaoException {
		String result = null;
		try {
			String sql = "select tpMutuo.* " + 
					"	from dbo.TipoMutuo tpMutuo " + 
					"	left join dbo.Mutuo m  " + 
					"	on m.TP_MUTUO = tpMutuo.TP_MUTUO  " + 
					"	where " + 
					"	m.ANNO_MUTUO =  '"+ annoMutuo.trim() + "' " +  
					"	and m.PROG_MUTUO = '" + progMutuo.trim() + "' " ; 

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			while (rs.next()) {
				result= rs.getString("DESC");
			}

			rs.close();
			ps.close();

		} catch (Exception e) {
			throw new DaoException(e);
		} finally {

		}
		return result;

	}
	

}
