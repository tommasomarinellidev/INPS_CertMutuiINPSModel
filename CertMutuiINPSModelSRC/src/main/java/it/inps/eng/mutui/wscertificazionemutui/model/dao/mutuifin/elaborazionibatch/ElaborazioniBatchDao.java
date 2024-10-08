package it.inps.eng.mutui.wscertificazionemutui.model.dao.mutuifin.elaborazionibatch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import it.inps.eng.mutui.wscertificazionemutui.model.dao.GenericDao;
import it.inps.eng.mutui.wscertificazionemutui.model.dao.exceptions.DaoException;



public class ElaborazioniBatchDao  extends GenericDao{



	public ElaborazioniBatchDao(Connection mutuifinConn) {

		super(mutuifinConn);
	}




	public String isPresenteCertificazioneCompetenza(String annoCert) throws DaoException {

		String result= "";
		String certFiscale="09"; //TP_ELAB
		//String annoDataSistema="2019"; //AM_ELAB
		//DateFormat dateFormat = new SimpleDateFormat("yyyy");
		//long dataMilli = System.currentTimeMillis();
		//int year = 1900 + (new Date(dataMilli).getYear()) -1;
		String annoDataSistema = annoCert + "01";

		try {
			String sql = "SELECT dt_ul_agg from ElaborazioniBatch where tp_elab= '" + certFiscale + "'  and am_elab = '" + annoDataSistema + "'";

			System.out.println("++isPresenteCertificazioneCompetenza: "+sql);
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();


			if(rs.next()) {
				long dt_ul_agg=rs.getDate("dt_ul_agg").getTime();
				//			if(dataMilli< dt_ul_agg){
				//				result="ko";
				//			}
				//			else{
				result=String.valueOf(dt_ul_agg);
				//			}

			}
			else {
				result="disabled";

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




}
