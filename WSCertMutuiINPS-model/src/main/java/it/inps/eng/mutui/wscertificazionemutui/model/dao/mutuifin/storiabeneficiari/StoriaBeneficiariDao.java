package it.inps.eng.mutui.wscertificazionemutui.model.dao.mutuifin.storiabeneficiari;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import it.inps.eng.mutui.wscertificazionemutui.model.dao.GenericDao;
import it.inps.eng.mutui.wscertificazionemutui.model.dao.exceptions.DaoException;
import it.inps.eng.wscertificazionemutui.common.beans.StoriaBeneficiariMutui;
import it.inps.eng.wscertificazionemutui.common.utils.FormalCheckUtils;



public class StoriaBeneficiariDao  extends GenericDao{

	
	public StoriaBeneficiariDao(Connection mutuifinConn) {

		super(mutuifinConn);
	}




	public List<StoriaBeneficiariMutui> getStoriaBeneficiariMutui(String annoMutuo, String progMutuo, String annoCert, String matricola, String statoElab) throws DaoException {

		List<StoriaBeneficiariMutui> sbList = new ArrayList<StoriaBeneficiariMutui>();
		StoriaBeneficiariMutui sb = null;

		if(FormalCheckUtils.isEmptyString(matricola))
			matricola = "";

		try {
			String sql = "EXEC StoriaBeneficiariMutui @AnnoCert='"+annoCert+"', @AnnoMutuoInput='"+annoMutuo+"', @ProgMutuoInput='"+progMutuo+"', @matricolaInput='"+matricola+"', @Eredi = '"+statoElab+"' ";
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			System.out.println("sql: "+sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				sb = new StoriaBeneficiariMutui();
				sb.setAnno_mutuo(annoMutuo);
				sb.setProg_mutuo(progMutuo);
				sb.setMtr(rs.getString("mtr"));
				sb.setDtDecInizio(rs.getDate("dt_dec"));
				sb.setDtDecFine(rs.getDate("dt_dec_fine"));
				sb.setDtOraInser(rs.getTimestamp("dt_ora_inser"));
				sb.setStEvn(rs.getString("st_evn"));
				sb.setTpEvn(rs.getString("tp_evn"));
				sb.setPerc(rs.getDouble("perc"));
				sb.setTpBen(rs.getString("tp_ben"));
				sb.setFlagInt(rs.getString("flag_int"));
				sb.setPercDetr(rs.getDouble("perc_Detr"));
				sb.setDtMor(rs.getDate("dt_mor"));
				sb.setCodFisc(rs.getString("codfis"));
				sb.setAnnoCert(annoCert);
				sbList.add(sb);

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



	@SuppressWarnings("deprecation")
	public boolean isEventoAccollo(StoriaBeneficiariMutui sb) throws DaoException {
		
		try {
			String sql =  "select * from StoriaBeneficiari where anno_Mutuo='" + sb.getAnno_mutuo() +"'and prog_Mutuo='" + sb.getProg_mutuo()+"' "
						//+ "and mtr='" + sb.getMtr() + "' "
						+ " and tp_evn = '16' and st_evn<> '00' order by dt_dec";
			logger.info("ACCOLLO: "+ sql);
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			if(rs.next()){
				logger.info("rs.getDate(\"dt_dec\"): "+ rs.getDate("dt_dec"));
				if(rs.getDate("dt_dec").getYear()+1900 == Integer.parseInt(sb.getAnnoCert())) {
					return true;
				}
			}
			return false;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}
	
}
