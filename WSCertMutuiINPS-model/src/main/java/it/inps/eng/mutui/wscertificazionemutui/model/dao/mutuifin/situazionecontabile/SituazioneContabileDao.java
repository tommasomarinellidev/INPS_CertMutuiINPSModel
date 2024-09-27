package it.inps.eng.mutui.wscertificazionemutui.model.dao.mutuifin.situazionecontabile;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import it.inps.eng.mutui.wscertificazionemutui.model.dao.GenericDao;
import it.inps.eng.mutui.wscertificazionemutui.model.dao.exceptions.DaoException;
import it.inps.eng.wscertificazionemutui.common.beans.certificazionefiscale.Rimborsi;
import it.inps.eng.wscertificazionemutui.common.utils.DateUtils;



public class SituazioneContabileDao  extends GenericDao{


	public SituazioneContabileDao(Connection mutuifinConn) {

		super(mutuifinConn);
	}



	public List<Rimborsi> getAllRimborsiAnniPrec(String annoMutuo, String progMutuo, Integer annoMeseInizio, Integer annoMeseFine, Date dataInizioPeriodo, Date dataFinePeriodo, String tpMov, String matricola) throws DaoException{

		if("01".equals(tpMov)){
			tpMov="'"+tpMov+"',";
		}
		else{
			tpMov="";
		}

		String and="";
		if(matricola!=null && !"".equals(matricola)){
			and="and sc.MTR = '"+matricola+"' ";
		}


		ArrayList<Rimborsi> rimborsiList= new ArrayList<Rimborsi>();
		try {
			String sql="select tm.TP_MOV as tp_mov, tm.[DESC] as [desc], sum(IMP_MOV) as IMP_MOV "
					+"from SituazioneContabile sc "
					+"inner join TipoMovimento tm on sc.TP_MOV = tm.TP_MOV "
					+"where ANNO_MUTUO = '"+annoMutuo+"' and PROG_MUTUO = '"+progMutuo+"' and ST_MOV <> '00'  "
					+"and DT_PGM >= '"+dataInizioPeriodo+"' and DT_PGM <= '"+dataFinePeriodo+"' "
					+"and tm.TP_MOV in ("+ tpMov +" '02','03','04','10') "
					+"and (CAU_MOV IS NULL OR CAU_MOV <> '06' OR (AM_RIF >= "+annoMeseInizio+" and AM_RIF <= "+annoMeseFine+")) "
					+ and
					+"group by tm.TP_MOV, tm.[DESC] "
					+"order by 1  ";

			logger.info("getAllRimborsiAnniPrec query    " + sql);
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			String tp_mov="";
			String desc="";
			Double imp_mov=null;

			while(rs.next()){
				Rimborsi rimborso=new Rimborsi();
				imp_mov=rs.getDouble("IMP_MOV");
				desc=rs.getString("desc"); 
				tp_mov=rs.getString("tp_mov");
				rimborso.setImp_mov(imp_mov);
				rimborso.setDesc(desc);
				rimborso.setTp_mov(tp_mov);

				rimborsiList.add(rimborso);
			}

			rs.close();
			ps.close();
			return rimborsiList;

		}
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}


	public List<Rimborsi> getAccontiRimborsiAnniPrec(String annoMutuo, String progMutuo, String annoCert, Date dataInizioPeriodo, Date dataFinePeriodo) throws DaoException{

		ArrayList<Rimborsi> rimborsiList= new ArrayList<Rimborsi>();
		try {
			String sql="select left(AM_RIF,4) as ANNO, -sum(IMP_MOV) as IMP_MOV " 
					+"from SituazioneContabile sc "
					+"inner join TipoMovimento tm on sc.TP_MOV = tm.TP_MOV "
					+"where ANNO_MUTUO = '"+annoMutuo+"' and PROG_MUTUO = '"+progMutuo+"' and ST_MOV <> '00'  "
					+"and DT_PGM >= '"+dataInizioPeriodo+"' and DT_PGM <= '"+dataFinePeriodo+"' "
					+"and tm.TP_MOV = '02' "
					+"and CAU_MOV = '06' and left(AM_RIF,4)  <> '"+annoCert+"' "
					+"group by  left(AM_RIF,4)" ;


			logger.info("getAccontiRimborsiAnniPrec : " +sql);
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();

			String anno="";
			Double imp_mov=null;

			while(rs.next()){
				Rimborsi rimborso=new Rimborsi();
				imp_mov=rs.getDouble("IMP_MOV");
				anno=rs.getString("ANNO"); 
				rimborso.setImp_mov(imp_mov);
				rimborso.setAnno(anno);

				rimborsiList.add(rimborso);
			}

			rs.close();
			ps.close();
			return rimborsiList;
		}
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}


	public boolean isPresenteDifferenzaSituazioneContabile(Date dataCertificazione, String annoMutuo, String progMutuo, String annoCert, Date decInizio, Date decFine) throws DaoException {

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		try {
			java.sql.Date dataUltAgg = new java.sql.Date(DateUtils.addDays(new Date(), 1).getTime()); //DateUtils.utilDateToSqlDate("yyyy-MM-dd", dataCertificazione);
			java.sql.Date dDecInizio = DateUtils.utilDateToSqlDate("yyyy-MM-dd", decInizio);
			java.sql.Date dDecFine = DateUtils.utilDateToSqlDate("yyyy-MM-dd", decFine);

			Timestamp dataEsecuzioneCertMassiva = this.getDataEsecuzioneCertMassiva(annoCert);
			if(dataEsecuzioneCertMassiva!=null) 
				dataUltAgg = new java.sql.Date(dataEsecuzioneCertMassiva.getTime());

			String sql = "SELECT count(*) as num_righe from SituazioneContabile where year(dt_pgm) ='"+annoCert+"' "
					+ "and DT_UL_AGG >= CONVERT(DATETIME,'"+dateFormat.format(dataUltAgg)+"',102) "
					+ "and anno_mutuo='"+annoMutuo+"' and prog_mutuo='"+progMutuo+"' and DT_PGM >= CONVERT(DATETIME,'"+dateFormat.format(dDecInizio)+"',102) and DT_PGM <= CONVERT(DATETIME,'"+dateFormat.format(dDecFine)+"',102)";

			boolean trovato=false;

			logger.info("isPresenteDifferenzaSituazioneContabile query    " + sql);

			PreparedStatement st = mutuifinConn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			if(rs.next()) {
				Integer count = rs.getInt("num_righe");
				if(count>0)
					trovato=true;
				else
					trovato=false;

			}
			return trovato;

		}catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}	
	}



	private Timestamp getDataEsecuzioneCertMassiva(String annoCert) throws SQLException {

		Timestamp res = null;

		String query ="SELECT MAX_DATE = MAX( EO.DATAESECUZIONE ) " +
				"FROM ELABORAZIONI_ONERI EO " +
				"WHERE EO.TIPOLAVORAZIONE='CERT' AND EO.AnnoCertificazione = '" + annoCert + "' and StatoLavorazione = 'D' and Ente = '00' and Massiva = 'true'";

		logger.info("getDataEsecuzioneCertMassiva     " + query);

		PreparedStatement st = mutuifinConn.prepareStatement(query);
		ResultSet rs = st.executeQuery();

		if(rs.next())
			res = rs.getTimestamp("MAX_DATE");

		return res;
	}

}
