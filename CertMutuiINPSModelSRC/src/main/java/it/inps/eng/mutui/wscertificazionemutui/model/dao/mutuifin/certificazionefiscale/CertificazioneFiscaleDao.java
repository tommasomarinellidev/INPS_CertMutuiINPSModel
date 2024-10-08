package it.inps.eng.mutui.wscertificazionemutui.model.dao.mutuifin.certificazionefiscale;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import it.inps.eng.mutui.wscertificazionemutui.model.dao.GenericDao;
import it.inps.eng.mutui.wscertificazionemutui.model.dao.exceptions.DaoException;
import it.inps.eng.wscertificazionemutui.common.constants.CodiciCerificazione;
import it.inps.eng.wscertificazionemutui.common.beans.AnagraficaIntestatario;
import it.inps.eng.wscertificazionemutui.common.beans.ConfigurazioneApplicazione;
import it.inps.eng.wscertificazionemutui.common.beans.MutuiAssociatiCodFisBean;
import it.inps.eng.wscertificazionemutui.common.beans.PeriodoBean;
import it.inps.eng.wscertificazionemutui.common.beans.Rata;
import it.inps.eng.wscertificazionemutui.common.beans.ReportDettaglioMutuo;
import it.inps.eng.wscertificazionemutui.common.beans.ReportImportiEstinzione;
import it.inps.eng.wscertificazionemutui.common.beans.certificazionefiscale.CertFisBean;
import it.inps.eng.wscertificazionemutui.common.beans.certificazionefiscale.CessazionePdfObj;
import it.inps.eng.wscertificazionemutui.common.utils.FormalCheckUtils;
import it.inps.eng.wscertificazionemutui.common.utils.Formatter;
import it.inps.eng.wscertificazionemutui.common.utils.Utility;



public class CertificazioneFiscaleDao  extends GenericDao{



	public CertificazioneFiscaleDao(Connection mutuifinConn) {

		super(mutuifinConn);
	}


	public List<PeriodoBean> getPeriodo(String codiceFis) throws DaoException {

		List<PeriodoBean> res = new ArrayList<PeriodoBean>();

		try {
			String sql =  " select CF1.ANNO_CERT, CF1.ANNO_MUTUO, CF1.PROG_MUTUO, CF1.COD_LET, DESC_LET, CF1.CODFIS_DEC, CF1.FL_RETT, "
					+ " case when FL_RETT = 0 then ''  else 'Certificazione sostitutiva' END AS DESC_RETT, CF1.ID "  
					+ " from CERTIFICAZIONI_FISCALI CF1  inner join(select ANNO_CERT, CODFIS_BEN, ANNO_MUTUO, PROG_MUTUO,max(DT_ORA_INSER)as DT_ORA_INSER " 
					+ " from CERTIFICAZIONI_FISCALI where CODFIS_BEN ='"+codiceFis+"' group by ANNO_CERT, ANNO_MUTUO, PROG_MUTUO, CODFIS_BEN) CF2 " 
					+ " on CF1.ANNO_MUTUO = CF2.ANNO_MUTUO and CF1.PROG_MUTUO = CF2.PROG_MUTUO and CF1.CODFIS_BEN = CF2.CODFIS_BEN and CF1.DT_ORA_INSER = CF2.DT_ORA_INSER inner  "
					+ " join TipoLettera TL on TL.COD_LET = CF1.COD_LET where FL_VISIBLE = 1 and CF1.ANNO_CERT>2017 order by CF1.ANNO_CERT, CF1.ANNO_MUTUO, CF1.PROG_MUTUO, CF1.COD_LET";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getPeriodo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				PeriodoBean p = new PeriodoBean();
				p.setAnnoCert(rs.getString("ANNO_CERT"));
				p.setAnnoM(rs.getString("ANNO_MUTUO"));
				p.setProgM(rs.getString("PROG_MUTUO"));
				p.setCodLet(rs.getString("COD_LET"));
				p.setDescLet(rs.getString("DESC_LET"));
				p.setCodFisDec(rs.getString("CODFIS_DEC"));
				p.setFlRett(rs.getString("FL_RETT"));
				p.setDescRett(rs.getString("DESC_RETT"));
				p.setIdCF(rs.getString("ID"));
				res.add(p);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}

	public List<MutuiAssociatiCodFisBean> getMutui(String matricola) throws DaoException {

		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();

		try {
			
			String sql =  " select "
					+ " tbl.anno_mutuo+'/'+tbl.prog_mutuo As [Numero Domanda], tbl.anno_mutuo,tbl.prog_mutuo,tbl.matricola,tbl.cognome, "
					+ " tbl.nome,dbo.change_sededescr(sedi.codicesap,sedi.[DESL SEDE]) AS sedeDesc, tbl.sede, tbl.st_mutuo, "
					+ " tpm.[DESC] as [Tipologia Mutuo], tbl.voce as [Voce Contabile], tbl.tas as Tasso, tbl.PERC as [Percentuale di Intestazione],stm.[DESC] as [Stato Mutuo] , "
		
					+ " iif (substring(tbl.matricola,7,8) ='00', "
					+ " iif ((tbl.mot_cess is NULL or tbl.mot_cess=''),'In Servizio', iif(tbl.mot_cess='7', 'Deceduto', 'Cessato')), "
					+ " iif (substring(tbl.matricola,7,8) >='40', 'Erede','Portiere')) as [Stato Intestatario] "
		
					+ " from (select rtrim(ben.cognome) as cognome, "
					+ " rtrim(ben.nome) as nome, ben.mtr as matricola, ben.sede as sede, "
					+ " sb.anno_mutuo as anno_mutuo, sb.prog_mutuo as prog_mutuo, "
					+ " sc.tas, sb.perc, sb.dt_dec, m.voce, m.st_mutuo, sc.tp_evn, m.tp_mutuo, ben.mot_cess ,ben.desc_cess "
					+ " from storiabeneficiari sb inner join beneficiari ben on (sb.mtr = ben.mtr and sb.st_evn <> '00') "
					+ " inner join mutuo m on (m.anno_mutuo = sb.anno_mutuo and m.prog_mutuo = sb.prog_mutuo) "
					+ " inner join storiacondizioni sc on sc.anno_mutuo = sb.anno_mutuo "
					+ " and sc.prog_mutuo = sb.prog_mutuo and sc.dt_ora_inser = "
					+ " (SELECT MAX(dt_ora_inser) FROM StoriaCondizioni "
					+ " WHERE anno_mutuo = sc.anno_mutuo AND prog_mutuo = sc.prog_mutuo) "
					+ " where sb.dt_dec = (SELECT MAX(dt_dec) FROM StoriaBeneficiari "
					+ " WHERE anno_mutuo = m.anno_mutuo AND prog_mutuo = m.prog_mutuo and st_evn<>'00'))tbl "
					//inner join openquery ([sqlinpssvil04\sql04],'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE			
					
					//+ " inner join openquery ([sqlinpssvil04\\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE "	
					
					+ " inner join V_MT_SEDI sedi ON sedi.CODICESAP COLLATE "
					 
					
					+ " DATABASE_DEFAULT = tbl.sede COLLATE DATABASE_DEFAULT and tbl.matricola in ('"+matricola+"')  "
					+ " inner join TipoMutuo tpm on tpm.TP_MUTUO=tbl.TP_MUTUO "
					+ " inner join StatoMutuo stm on stm.ST_MUTUO=tbl.ST_MUTUO "
					+ " order by tbl.cognome, tbl.nome, sedeDesc ";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
				
				m.setAnnoMutuo(rs.getString("anno_mutuo"));
				m.setProgMutuo(rs.getString("prog_mutuo"));
				m.setMatricola(rs.getString("matricola"));
				m.setCognome(rs.getString("cognome"));
				m.setNome(rs.getString("nome"));				
				m.setSedeDesc(rs.getString("sedeDesc"));
				m.setSede(rs.getString("sede"));
				m.setDescTipologiaMutuo(rs.getString("Tipologia Mutuo"));
				m.setVoceContabile(rs.getString("Voce Contabile"));				
				m.setTasso(rs.getDouble("Tasso"));			
				m.setPercentualeDiIntestazione(rs.getDouble("Percentuale di Intestazione"));						
				m.setDescStatoMutuo(rs.getString("Stato Mutuo"));
				m.setStatoMutuo(rs.getString("st_mutuo"));
				m.setStatoIntestatario(rs.getString("Stato Intestatario"));
				
				res.add(m);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}

	
	public ReportImportiEstinzione getImportiRateEmesse(ReportImportiEstinzione rie, Rata rata) throws DaoException {

		ReportImportiEstinzione res = new ReportImportiEstinzione();

		try {
			String sql = "select sum(imp_rt) as tot_rata, sum(imp_capi_rt) as tot_capi, sum(imp_its_rt)as tot_its from rate where anno_mutuo = ? and prog_mutuo = ?  and prog_rt >= ? and st_rt in ('02', '03') group by imp_rt, imp_capi_rt, imp_its_rt";

			//System.out.println("804_Sett getImportiRateEmesse  sql: "+sql);
			//System.out.println("805_Sett rata.getAnno_mutuo(): "+rata.getAnno_mutuo());
			//System.out.println("806_Sett rata.getProg_mutuo(): "+rata.getProg_mutuo());
			//System.out.println("807_Sett rata.getNumeroRata().toString(): "+rata.getNumeroRata().toString());
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ps.setString(1,rata.getAnno_mutuo());
			ps.setString(2,rata.getProg_mutuo());
			ps.setString(3,rata.getNumeroRata().toString());
			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();
			if(rs.next()) {
				rie.setTotaleCapitaleRate(Formatter.formatDouble(rs.getDouble("tot_rata")));
				rie.setTotaleQuotaCapitale(Formatter.formatDouble(rs.getDouble("tot_capi")));
				rie.setTotaleQuotaInteressi(Formatter.formatDouble(rs.getDouble("tot_its")));
			}
			rs.close();
			ps.close();

			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	
	public List<MutuiAssociatiCodFisBean> getVisualizzaMutuiInEssere(String matricola) throws DaoException {

		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();

		try {
			
			String sql =  " select "
				+ " tbl.anno_mutuo+'/'+tbl.prog_mutuo As [Numero Domanda], tbl.anno_mutuo,tbl.prog_mutuo,tbl.matricola,tbl.cognome, "
				+ " tbl.nome,dbo.change_sededescr(sedi.codicesap,sedi.[DESL SEDE]) AS sedeDesc, tbl.sede, "
				+ " tpm.[DESC] as [Tipologia Mutuo], tbl.voce as [Voce Contabile], tbl.tas as Tasso, tbl.PERC as [Percentuale di Intestazione],stm.[DESC] as [Stato Mutuo] , "
	
				+ " iif (substring(tbl.matricola,7,8) ='00', "
				+ " iif ((tbl.mot_cess is NULL or tbl.mot_cess=''),'In Servizio', iif(tbl.mot_cess='7', 'Deceduto', 'Cessato')), "
				+ " iif (substring(tbl.matricola,7,8) >='40', 'Erede','Portiere')) as [Stato Intestatario] "
	
				+ " from (select rtrim(ben.cognome) as cognome, "
				+ " rtrim(ben.nome) as nome, ben.mtr as matricola, ben.sede as sede, "
				+ " sb.anno_mutuo as anno_mutuo, sb.prog_mutuo as prog_mutuo, "
				+ " sc.tas, sb.perc, sb.dt_dec, m.voce, m.st_mutuo, sc.tp_evn, m.tp_mutuo, ben.mot_cess ,ben.desc_cess "
				+ " from storiabeneficiari sb inner join beneficiari ben on (sb.mtr = ben.mtr and sb.st_evn <> '00') "
				+ " inner join mutuo m on (m.anno_mutuo = sb.anno_mutuo and m.prog_mutuo = sb.prog_mutuo) "
				+ " inner join storiacondizioni sc on sc.anno_mutuo = sb.anno_mutuo "
				+ " and sc.prog_mutuo = sb.prog_mutuo and sc.dt_ora_inser = "
				+ " (SELECT MAX(dt_ora_inser) FROM StoriaCondizioni "
				+ " WHERE anno_mutuo = sc.anno_mutuo AND prog_mutuo = sc.prog_mutuo) "
				+ " where sb.dt_dec = (SELECT MAX(dt_dec) FROM StoriaBeneficiari "
				+ " WHERE anno_mutuo = m.anno_mutuo AND prog_mutuo = m.prog_mutuo and st_evn<>'00'))tbl "
				//inner join openquery ([sqlinpssvil04\sql04],'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE			
				
				//+ " inner join openquery ([sqlinpssvil04\\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE "
				+ " inner join V_MT_SEDI  sedi ON sedi.CODICESAP COLLATE "	
				
				+ " DATABASE_DEFAULT = tbl.sede COLLATE DATABASE_DEFAULT and tbl.matricola in ('"+matricola+"')  "
				+ " AND tbl.ST_MUTUO='01' "
				+ " inner join TipoMutuo tpm on tpm.TP_MUTUO=tbl.TP_MUTUO "
				+ " inner join StatoMutuo stm on stm.ST_MUTUO=tbl.ST_MUTUO "
				+ " order by tbl.cognome, tbl.nome, sedeDesc ";


			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
				
				m.setNumeroDomanda(rs.getString("Numero Domanda"));
				m.setAnnoMutuo(rs.getString("anno_mutuo"));
				m.setProgMutuo(rs.getString("prog_mutuo"));
				m.setMatricola(rs.getString("matricola"));
				m.setCognome(rs.getString("cognome"));
				m.setNome(rs.getString("nome"));				
				m.setSedeDesc(rs.getString("sedeDesc"));
				m.setSede(rs.getString("sede"));
				m.setDescTipologiaMutuo(rs.getString("Tipologia Mutuo"));
				m.setVoceContabile(rs.getString("Voce Contabile"));				
				m.setTasso(rs.getDouble("Tasso"));			
				m.setPercentualeDiIntestazione(rs.getDouble("Percentuale di Intestazione"));						
				m.setDescStatoMutuo(rs.getString("Stato Mutuo"));
				m.setStatoIntestatario(rs.getString("Stato Intestatario"));
				
				res.add(m);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	public List<MutuiAssociatiCodFisBean> getVisualizzaMutuiNonInEssere(String matricola) throws DaoException {

		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();

		try {
//			String sql =  " select dbo.change_sededescr(sedi.codicesap,sedi.[DESL SEDE]) AS sedeDesc, tbl.*  "
//					+ " from "
//					+ " (select rtrim(ben.cognome) as cognome, rtrim(ben.nome) as nome, ben.mtr as matricola, "
//					+ " ben.sede as sede, sb.anno_mutuo as anno_mutuo, sb.prog_mutuo as prog_mutuo, "
//					+ " sc.tas, sb.perc, sb.dt_dec, m.voce, m.st_mutuo, sc.tp_evn, m.tp_mutuo, "
//					+ " ben.mot_cess ,ben.desc_cess "
//					+ " from storiabeneficiari sb "
//					+ " inner join beneficiari ben on (sb.mtr = ben.mtr and sb.st_evn <> '00') "
//					+ " inner join mutuo m on (m.anno_mutuo = sb.anno_mutuo and m.prog_mutuo = sb.prog_mutuo) "
//					+ " inner join storiacondizioni sc on sc.anno_mutuo = sb.anno_mutuo and sc.prog_mutuo = sb.prog_mutuo and sc.dt_ora_inser = "
//					+ " (SELECT MAX(dt_ora_inser) "
//					+ " FROM StoriaCondizioni "
//					+ " WHERE anno_mutuo = sc.anno_mutuo AND prog_mutuo = sc.prog_mutuo) "
//					+ " where sb.dt_dec = (SELECT MAX(dt_dec) "
//					+ " FROM StoriaBeneficiari "
//					+ " WHERE  anno_mutuo = m.anno_mutuo AND prog_mutuo = m.prog_mutuo and st_evn<>'00'))tbl "
//					//+ " inner join openquery ([sqlinpssvil04\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi "
//					+ " inner join openquery ([sqlinpssvil04\\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi  "
//					+ " ON sedi.CODICESAP COLLATE DATABASE_DEFAULT = tbl.sede COLLATE DATABASE_DEFAULT and tbl.matricola in ('"+matricola+"')   "
//					+ " AND ST_MUTUO<>'01' "
//					+ " order by cognome, nome, sedeDesc ";


			
			String sql =  " select "
					+ " tbl.anno_mutuo+'/'+tbl.prog_mutuo As [Numero Domanda], tbl.anno_mutuo,tbl.prog_mutuo,tbl.matricola,tbl.cognome, "
					+ " tbl.nome,dbo.change_sededescr(sedi.codicesap,sedi.[DESL SEDE]) AS sedeDesc, tbl.sede, "
					+ " tpm.[DESC] as [Tipologia Mutuo], tbl.voce as [Voce Contabile], tbl.tas as Tasso, tbl.PERC as [Percentuale di Intestazione],stm.[DESC] as [Stato Mutuo] , "
		
					+ " iif (substring(tbl.matricola,7,8) ='00', "
					+ " iif ((tbl.mot_cess is NULL or tbl.mot_cess=''),'In Servizio', iif(tbl.mot_cess='7', 'Deceduto', 'Cessato')), "
					+ " iif (substring(tbl.matricola,7,8) >='40', 'Erede','Portiere')) as [Stato Intestatario] "
		
					+ " from (select rtrim(ben.cognome) as cognome, "
					+ " rtrim(ben.nome) as nome, ben.mtr as matricola, ben.sede as sede, "
					+ " sb.anno_mutuo as anno_mutuo, sb.prog_mutuo as prog_mutuo, "
					+ " sc.tas, sb.perc, sb.dt_dec, m.voce, m.st_mutuo, sc.tp_evn, m.tp_mutuo, ben.mot_cess ,ben.desc_cess "
					+ " from storiabeneficiari sb inner join beneficiari ben on (sb.mtr = ben.mtr and sb.st_evn <> '00') "
					+ " inner join mutuo m on (m.anno_mutuo = sb.anno_mutuo and m.prog_mutuo = sb.prog_mutuo) "
					+ " inner join storiacondizioni sc on sc.anno_mutuo = sb.anno_mutuo "
					+ " and sc.prog_mutuo = sb.prog_mutuo and sc.dt_ora_inser = "
					+ " (SELECT MAX(dt_ora_inser) FROM StoriaCondizioni "
					+ " WHERE anno_mutuo = sc.anno_mutuo AND prog_mutuo = sc.prog_mutuo) "
					+ " where sb.dt_dec = (SELECT MAX(dt_dec) FROM StoriaBeneficiari "
					+ " WHERE anno_mutuo = m.anno_mutuo AND prog_mutuo = m.prog_mutuo and st_evn<>'00'))tbl "
					//inner join openquery ([sqlinpssvil04\sql04],'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE			
					
					//+ " inner join openquery ([sqlinpssvil04\\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE "		
					+ " inner join V_MT_SEDI sedi ON sedi.CODICESAP COLLATE "
					
					+ " DATABASE_DEFAULT = tbl.sede COLLATE DATABASE_DEFAULT and tbl.matricola in ('"+matricola+"')  "
					+ " AND tbl.ST_MUTUO<>'01' "
					+ " inner join TipoMutuo tpm on tpm.TP_MUTUO=tbl.TP_MUTUO "
					+ " inner join StatoMutuo stm on stm.ST_MUTUO=tbl.ST_MUTUO "
					+ " order by tbl.cognome, tbl.nome, sedeDesc ";
			
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
				
//				m.setSedeDesc(("sedeDesc"));
//				m.setCognome(rs.getString("cognome"));
//				m.setNome(rs.getString("nome"));
//				m.setMatricola(rs.getString("matricola"));
//				m.setSede(rs.getString("sede"));
//				m.setAnnoMutuo(rs.getString("anno_mutuo"));
//				m.setProgMutuo(rs.getString("prog_mutuo"));
//				m.setTasso(rs.getDouble("tas"));			
//				m.setPercentualeDiIntestazione(rs.getDouble("perc"));				
//				m.setDtDec(rs.getDate("dt_dec"));
//				m.setVoceContabile(rs.getString("voce"));
//				m.setStatoMutuo(rs.getString("st_mutuo"));
//				m.setTpEvn(rs.getString("tp_evn"));
//				m.setTipologiaMutuo(rs.getString("tp_mutuo"));
//				m.setMotCess(rs.getString("mot_cess"));				
//				m.setDescCess(rs.getString("desc_cess"));
				
				m.setNumeroDomanda(rs.getString("Numero Domanda"));
				m.setAnnoMutuo(rs.getString("anno_mutuo"));
				m.setProgMutuo(rs.getString("prog_mutuo"));
				m.setMatricola(rs.getString("matricola"));
				m.setCognome(rs.getString("cognome"));
				m.setNome(rs.getString("nome"));				
				m.setSedeDesc(rs.getString("sedeDesc"));
				m.setSede(rs.getString("sede"));
				m.setDescTipologiaMutuo(rs.getString("Tipologia Mutuo"));
				m.setVoceContabile(rs.getString("Voce Contabile"));				
				m.setTasso(rs.getDouble("Tasso"));			
				m.setPercentualeDiIntestazione(rs.getDouble("Percentuale di Intestazione"));						
				m.setDescStatoMutuo(rs.getString("Stato Mutuo"));
				m.setStatoIntestatario(rs.getString("Stato Intestatario"));
				
				res.add(m);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}

	
//	public List<MutuoBean> getmatricoleMutui(String codiceFis) throws DaoException {
//
//		List<MutuoBean> res = new ArrayList<MutuoBean>();
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
//			logger.debug("getmatricoleMutui query" + sql);
//			ResultSet rs = ps.executeQuery();
//
//			while(rs.next()){
//				MutuoBean p = new MutuoBean();
//				p.setAnnoCert(rs.getString("matricola"));
//				p.setAnnoM(rs.getString("nome"));
//				p.setProgM(rs.getString("cognome"));
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

	public List<ConfigurazioneApplicazione> getDescrizioneMutuiAiDipendenti(String chiave) throws DaoException {
		List<ConfigurazioneApplicazione> res = new ArrayList<ConfigurazioneApplicazione>();
		try {					
			String sql =  " SELECT chiave, valore "
					+ "FROM ConfigurazioneApplicazione ";
			
					if(chiave!=null) {
						sql = sql + "where chiave in ('"+chiave+"')";  
					}
					
			
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMatricoleMutui query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				ConfigurazioneApplicazione p = new ConfigurazioneApplicazione();
				p.setChiave(rs.getString("chiave"));
				p.setValore(rs.getString("valore"));
				res.add(p);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}

	public List<MutuiAssociatiCodFisBean> getDettaglioMutuo(String annoMutuo, String progMutuo) throws DaoException {

		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();

		try {

			String sql =  " SELECT "
					+ " Mutuo.ANNO_MUTUO+'/'+Mutuo.PROG_MUTUO As [Numero Domanda], Mutuo.DT_STI As [Data stipula], Mutuo.DT_NASC_MUTUO As [Data nascita mutuo], "
					+ " Mutuo.DT_ERO As [Data erogazione], StoriaCondizioni.DATA_SCADENZA [Data scadenza prevista], Mutuo.TP_MUTUO As [tipo Mutuo], StoriaCondizioni.IMP_RT As [Importo Rata], "
					+ " StoriaCondizioni.NUM_RT_TOT As [Numero rate], "
					+ " iif (StoriaCondizioni.FRQ_RT=12, 'MENSILE','NON MENSILE') as [frequenza rate], "
					+ " StoriaCondizioni.TP_TAS, tps.[DESC] as [Tipologia Tasso], "
					+ " Mutuo.ST_MUTUO,  tpm.[DESC] as [Tipologia Mutuo], Mutuo.imp_mutuo as [Importo Mutuo], StatoMutuo.[DESC] as [Desc Stato Mutuo], "
					+ " Mutuo.IMP_SPE as [Spese istruttoria], Mutuo.VOCE as [Voce Contabile], "
					//+ " SituazioneContabile.IMP_MOV as [Imposta sostitutiva intestatario], "
					+ " Mutuo.IMP_ITS_PREAMM as [Interessi preammortamento], "
					+ " Mutuo.DT_CHS as [Data chiusura], "
					+ " (select sum(StoriaCondizioni.imp_estin) "
					+ " FROM StoriaCondizioni "
					+ " where  ANNO_MUTUO = '"+annoMutuo+"' AND PROG_MUTUO = '"+progMutuo+"' and ST_EVN <> '00' ) as [Importo Est. Anticipate], "
					+ " Mutuo.IMP_CAPI_1_LOTTO as [Importo Primo Acconto], "
					+ " Mutuo.IMP_CAPI_2_LOTTO as [Importo Secondo Acconto], "
					+ " Mutuo.IMP_CAPI_3_LOTTO as [Saldo], "
					+ " Mutuo.DT_ERO_1_LOTTO as [Data Erog. Primo Acconto], "
					+ " Mutuo.DT_ERO_2_LOTTO as [Data Erog. Secondo Acconto], "
					+ " Mutuo.DT_ERO_3_LOTTO as [Data Erog. Saldo] "
		
					+ " FROM Mutuo "
					+ " INNER JOIN TipoMutuo tpm on tpm.TP_MUTUO=Mutuo.TP_MUTUO  "
					+ " INNER JOIN StoriaCondizioni ON Mutuo.ANNO_MUTUO = StoriaCondizioni.ANNO_MUTUO AND Mutuo.PROG_MUTUO = StoriaCondizioni.PROG_MUTUO "
					//+ " INNER JOIN SituazioneContabile ON Mutuo.ANNO_MUTUO = SituazioneContabile.ANNO_MUTUO AND Mutuo.PROG_MUTUO = SituazioneContabile.PROG_MUTUO AND SituazioneContabile.tp_mov = '10' and SituazioneContabile.st_mov <> '00' "	
					+ " INNER JOIN TipoTasso tps on tps.TP_TAS=StoriaCondizioni.TP_TAS "
					+ " INNER JOIN TipoMutuo ON Mutuo.TP_MUTUO = TipoMutuo.TP_MUTUO "
					+ " INNER JOIN StatoMutuo ON Mutuo.ST_MUTUO = StatoMutuo.ST_MUTUO "
					+ " WHERE (Mutuo.ANNO_MUTUO = '"+annoMutuo+"') AND (Mutuo.PROG_MUTUO = '"+progMutuo+"') "
					+ " and StoriaCondizioni.DT_DEC = "
					+ " (SELECT MAX(DT_DEC) "
					+ " FROM StoriaCondizioni "
					+ " WHERE ANNO_MUTUO = '"+annoMutuo+"' AND PROG_MUTUO = '"+progMutuo+"' AND ST_EVN <> '00' ) AND StoriaCondizioni.ST_EVN <> '00' ";


			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
				
				m.setNumeroDomanda(rs.getString("Numero Domanda"));						
				m.setNumeroRate(rs.getDouble("Numero rate"));	
				m.setFrequenzaRate(rs.getString("Frequenza rate"));	
				m.setTasso(rs.getDouble("TP_TAS"));	
				m.setDescTipoTasso(rs.getString("Tipologia Tasso"));	
				m.setStatoMutuo(rs.getString("ST_MUTUO"));
				m.setDescStatoMutuo(rs.getString("Desc Stato Mutuo"));
				m.setDescTipologiaMutuo(rs.getString("Tipologia Mutuo"));	
				m.setSpeseIstruttoria(rs.getDouble("Spese istruttoria"));	
				m.setVoceContabile(rs.getString("Voce Contabile"));						
								
				m.setImportoEstinzioni(rs.getDouble("Importo Est. Anticipate"));	
			
				m.setTipologiaMutuo(rs.getString("tipo Mutuo"));
				if(rs.getString("tipo Mutuo").equalsIgnoreCase("05")) {
					m.setInteressiPreammortamento(rs.getDouble("Interessi preammortamento"));						
					m.setImportoPrimoAcconto(rs.getDouble("Importo Primo Acconto"));	
					m.setImportoSecondoAcconto(rs.getDouble("Importo Secondo Acconto"));				
					m.setImportoSaldo(rs.getDouble("Saldo"));
					
					m.setDataErogPrimoAcconto(rs.getDate("Data Erog. Primo Acconto"));	
					m.setDataErogSecondoAcconto(rs.getDate("Data Erog. Secondo Acconto"));	
					m.setDataErogSaldo(rs.getDate("Data Erog. Saldo"));	
				}					
				
				m.setImportoMutuo(rs.getDouble("Importo Mutuo"));	
				m.setImportoRata(rs.getDouble("Importo Rata"));							
				m.setDataStipula(rs.getDate("Data stipula"));	
				m.setDataNascitaMutuo(rs.getDate("Data nascita mutuo"));	
				m.setDataErogazione(rs.getDate("Data erogazione"));	
				m.setDataScadenza(rs.getDate("Data scadenza prevista"));	
				m.setDataChiusura(rs.getDate("Data chiusura"));	

				res.add(m);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}
	
	public Double getImpostaSostitutiva(String annoMutuo, String progMutuo) throws DaoException {
		Double bd = Double.valueOf(0);
		try {
			String sql =  " select IMP_MOV as [Imposta sostitutiva intestatario] "
					+ " from SituazioneContabile "
					+ " WHERE ANNO_MUTUO = '"+annoMutuo+"' AND PROG_MUTUO = '"+progMutuo+"' AND ST_MOV <> '00' and SituazioneContabile.tp_mov = '10' ";
			
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
					
				bd = rs.getDouble("Imposta sostitutiva intestatario");			
				
			}
			rs.close();	
			ps.close();
			return bd;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	public List<MutuiAssociatiCodFisBean> getIntPream(String matricola, String annoMutuo, String progMutuo) throws DaoException {
		List<MutuiAssociatiCodFisBean> resList = new ArrayList<MutuiAssociatiCodFisBean>();
		Double d = Double.valueOf(0);
		try {				
			String sql =  "select PROG_RT, IMP_MOV, MTR "
					+ "FROM SituazioneContabile "
					+ "where  ANNO_MUTUO = '"+annoMutuo+"' AND PROG_MUTUO = '"+progMutuo
					+"' and TP_MOV='03'  and MTR='"+matricola+"' order by PROG_RT, IMP_MOV, MTR ";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();			
				m.setProgMutuo(rs.getString("PROG_RT"));	
				m.setImportoMov(rs.getDouble("IMP_MOV"));
				m.setMatricola(rs.getString("MTR"));
				resList.add(m);
			}
			rs.close();	
			ps.close();
			return resList;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	
	public Double getInteressiPreammortamentoTrattenuta(String annoMutuo, String progMutuo) throws DaoException {
		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();
		Double d = Double.valueOf(0);
		try {		
			
			String sql =  "Select sum(imp_mov) as interessiPreammortamentoTrattenuta "
					+ " from SituazioneContabile where anno_mutuo = '"+annoMutuo+"' and prog_mutuo = '"+progMutuo+"' "
					+ " and tp_mov in ('02', '03') and st_mov <> '00' and cau_mov in ('05', '07') ";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();			
				d = rs.getDouble("interessiPreammortamentoTrattenuta");													
				res.add(m);
			}
			rs.close();	
			ps.close();
			return d;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	public Double getInteressiPreammortamento(String annoMutuo, String progMutuo) throws DaoException {
		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();
		Double d = Double.valueOf(0);
		try {		
			
			String sql =  " select SUM(IMP_MOV) as [interessi preammortamento] "
					+ " from SituazioneContabile "
			+ " WHERE ANNO_MUTUO = '"+annoMutuo+"' AND PROG_MUTUO = '"+progMutuo+"' AND ST_MOV <> '00' and SituazioneContabile.tp_mov = '03' ";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();			
				d = rs.getDouble("interessi preammortamento");													
				res.add(m);
			}
			rs.close();	
			ps.close();
			return d;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}
	
	

	public Double getInteressiRimborso(String annoMutuo, String progMutuo) throws DaoException {
		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();
		Double d = Double.valueOf(0);
		try {
			
			String sql =  "Select abs(sum(imp_mov)) as interessiRimborso "
					+ " from SituazioneContabile where anno_mutuo = '"+annoMutuo+"' and prog_mutuo = '"+progMutuo+"' "
					+ " and tp_mov in ('02', '03') and st_mov <> '00' and cau_mov in ('06', '08') ";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();			
				d = rs.getDouble("interessiRimborso");													
				res.add(m);
			}
			rs.close();	
			ps.close();
			return d;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}
	
	public MutuiAssociatiCodFisBean getNumeroDataProtocollo(String annoMutuo, String progMutuo) throws DaoException {
		MutuiAssociatiCodFisBean res = new MutuiAssociatiCodFisBean();
		try {
						
//			String sql =  "SELECT NUMPROT, DTPROT "
//					//+ " FROM [MUTUIFIN].[dbo].[COM] "
//					+ " FROM COM "
//					+ " where ANNO_MUTUO='"+annoMutuo+"' and prog_mutuo='"+progMutuo+"' ";
			 
			
			String sql  = "";
			String annoMutuo91_92 =  annoMutuo.substring(0,2);//controllo che esiste mutuo separato attivo
			if(annoMutuo91_92.equalsIgnoreCase("91") || annoMutuo91_92.equalsIgnoreCase("92")) {
				
				String sql1 = "select anno_mutuo,PROG_MUTUO "+
				"from Mutuo where "+
				"anno_mutuo like '%"+annoMutuo.substring(2,4)+"' and left(anno_mutuo,2)<>'91' and left(anno_mutuo,2)<>'92' and prog_mutuo='" + progMutuo+ "'";
				
				PreparedStatement ps1 = mutuifinConn.prepareStatement(sql1);
				logger.debug("getMutuo query sql1 24_08_2022" + sql1);
				ResultSet rs1 = ps1.executeQuery();
				
				String annoMutuo1="";
				String progMutuo1="";
				if (rs1.next()) {
					annoMutuo1 = rs1.getString("anno_mutuo")!=null?rs1.getString("anno_mutuo").trim():null;
					progMutuo1 = rs1.getString("PROG_MUTUO")!=null?rs1.getString("PROG_MUTUO").trim():null;
				} 
				
				logger.info("getMutuo query modifica 24_08_2022 annoMutuo1" + annoMutuo1);
				
				sql =  "SELECT NUMPROT, DTPROT "						
						+ " FROM COM "
						+ " where ANNO_MUTUO='"+annoMutuo1+"' and prog_mutuo='"+progMutuo1+"' ";
			} else {
				
				logger.info("getMutuo query modifica 24_08_2022 annoMutuo" + annoMutuo); 
				
				sql =  "SELECT NUMPROT, DTPROT "						
						+ " FROM COM "
						+ " where ANNO_MUTUO='"+annoMutuo+"' and prog_mutuo='"+progMutuo+"' ";

			}
			


			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			
			
			logger.debug("getMutuo query modifica 24_08_2022" + sql);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){			
								
				res.setNumeroProtocollo(rs.getDouble("NUMPROT"));	
				res.setDataProtocollo(rs.getDate("DTPROT"));	
				
				logger.info("NumeroProtocollo modifica 24_08_2022 " + res.getNumeroProtocollo()); 
				logger.info("DataProtocollo() modifica 24_08_2022 " + res.getDataProtocollo()); 
				
				//res.add(m);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	public List<MutuiAssociatiCodFisBean> getAnagraficaIntestatario(String matricola, String annoMutuo, String progMutuo) throws DaoException {

		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();

		try {

			String sql =  " select "
					+ " tbl.anno_mutuo+'/'+tbl.prog_mutuo As [Numero Domanda], tbl.anno_mutuo,tbl.prog_mutuo,tbl.matricola,tbl.cognome, "
					+ " tbl.nome,dbo.change_sededescr(sedi.codicesap,sedi.[DESL SEDE]) AS sedeDesc, tbl.sede, "
					+ " tpm.[DESC] as [Tipologia Mutuo], tbl.voce as [Voce Contabile], tbl.tas as Tasso, tbl.PERC_DETR as [Percentuale di Intestazione],stm.[DESC] as [Stato Mutuo] , "
		
					+ " iif (substring(tbl.matricola,7,8) ='00', "
					+ " iif ((tbl.mot_cess is NULL or tbl.mot_cess=''),'In Servizio', iif(tbl.mot_cess='7', 'Deceduto', 'Cessato')), "
					+ " iif (substring(tbl.matricola,7,8) >='40', 'Erede','Portiere')) as [Stato Intestatario] "
					
					+ " from (select rtrim(ben.cognome) as cognome, "
					+ " rtrim(ben.nome) as nome, ben.mtr as matricola, ben.sede as sede, "
					+ " sb.anno_mutuo as anno_mutuo, sb.prog_mutuo as prog_mutuo, "
					+ " sc.tas, sb.PERC_DETR, sb.dt_dec, m.voce, m.st_mutuo, sc.tp_evn, m.tp_mutuo, ben.mot_cess ,ben.desc_cess "
					+ " from storiabeneficiari sb inner join beneficiari ben on (sb.mtr = ben.mtr and sb.st_evn <> '00') "
					+ " inner join mutuo m on (m.anno_mutuo = sb.anno_mutuo and m.prog_mutuo = sb.prog_mutuo) "
					+ " inner join storiacondizioni sc on sc.anno_mutuo = sb.anno_mutuo "
					+ " and sc.prog_mutuo = sb.prog_mutuo and sc.dt_ora_inser = "
					+ " (SELECT MAX(dt_ora_inser) FROM StoriaCondizioni "
					+ " WHERE anno_mutuo = sc.anno_mutuo AND prog_mutuo = sc.prog_mutuo) "
					+ " where sb.dt_dec = (SELECT MAX(dt_dec) FROM StoriaBeneficiari "
					+ " WHERE anno_mutuo = m.anno_mutuo AND prog_mutuo = m.prog_mutuo and st_evn<>'00'))tbl "
					//Test 
					//+ " inner join openquery ([sqlinpssvil04\\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE "					
				    //Collaudo
				    //+ " inner join openquery ([SQLCOL20], 'select * from veganet.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE "
				    //Esercizio
				    //+ " inner join openquery ([SQLINPS13\\SQLINPS13], 'select * from veganet.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE "

					+ " inner join V_MT_SEDI sedi ON sedi.CODICESAP COLLATE "
					
					+ " DATABASE_DEFAULT = tbl.sede COLLATE DATABASE_DEFAULT and tbl.matricola in ('"+matricola+"') "
					+ " and tbl.anno_mutuo = '"+annoMutuo+"'  and tbl.prog_mutuo = '"+progMutuo+"' "
					+ " inner join TipoMutuo tpm on tpm.TP_MUTUO=tbl.TP_MUTUO "
					+ " inner join StatoMutuo stm on stm.ST_MUTUO=tbl.ST_MUTUO "

					+ " order by tbl.cognome, tbl.nome, sedeDesc ";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
				
				m.setNumeroDomanda(rs.getString("Numero Domanda"));
				m.setAnnoMutuo(rs.getString("anno_mutuo"));
				m.setProgMutuo(rs.getString("prog_mutuo"));
				m.setMatricola(rs.getString("matricola"));
				m.setCognome(rs.getString("cognome"));
				m.setNome(rs.getString("nome"));				
				m.setSedeDesc(rs.getString("sedeDesc"));
				m.setSede(rs.getString("sede"));
				m.setDescTipologiaMutuo(rs.getString("Tipologia Mutuo"));
				m.setVoceContabile(rs.getString("Voce Contabile"));				
				m.setTasso(rs.getDouble("Tasso"));			
				m.setPercentualeDiIntestazione(rs.getDouble("Percentuale di Intestazione"));						
				m.setDescStatoMutuo(rs.getString("Stato Mutuo"));
				m.setStatoIntestatario(rs.getString("Stato Intestatario"));
//			m.setResidenza(rs.getString("residenza"));
				
				res.add(m);
			}
			rs.close();	
			ps.close();

			//inizio inserimento della residenza
			String sql1 =  					
					" select "
					//+" (RTRIM(nominativo.INDIR) +' n. '+ RTRIM(nominativo.NUMEROCIV) +' '+ RTRIM(nominativo.Comune_Res) +' ('+substring(nominativo.PROVRES,0,3)+') '+ nominativo.CODCAP + ' Qualora questi dati non fossero corretti, mandare una mail alla sede territoriale competente per il loro aggiornamento.' ) as [residenza] "
					+" (RTRIM(nominativo.INDIR) +' n. '+ RTRIM(nominativo.NUMEROCIV) +' '+ RTRIM(nominativo.Comune_Res) +' ('+substring(nominativo.PROVRES,0,3)+') '+ nominativo.CODCAP ) as [residenza] "
					+ " from (SELECT matranag,INDIR, NUMEROCIV, Comune_Res , PROVRES, CODCAP FROM V_MT_PERSONALE "
					+ " Union SELECT matranag, INDIR, NUMEROCIV, Comune_Res, PR_RES as PROVRES, CODCAP FROM V_MT_EREDI "
					+ " Union SELECT matranag, INDIR, NUMEROCIV, Comune_Residenza as Comune_Res, Prov_Res as PROVRES, CODCAP FROM V_MT_EREDIPERS) nominativo "
					+ " where nominativo.matranag in ('"+matricola+"') ";
								
			PreparedStatement ps1 = mutuifinConn.prepareStatement(sql1);

			logger.debug("getMutuo query" + sql1);
			ResultSet rs1 = ps1.executeQuery();

			while(rs1.next()){	
				res.get(0).setResidenza(rs1.getString("residenza"));	
			}
			rs1.close();	
			ps1.close();
			//fine inserimento della residenza	
			
			//descrizione residenza
			String chiave="fraseResidenza";
			String sql2 =  " SELECT chiave, valore "
					+ "FROM ConfigurazioneApplicazione where chiave in ('"+chiave+"') ";
//			
//					if(chiave!=null) {
//						sql2 = sql2 + "where chiave in ('"+chiave+"')";  
//					}
//					
//			
			PreparedStatement ps2 = mutuifinConn.prepareStatement(sql2);

			logger.debug("getMatricoleMutui query" + sql2);
			ResultSet rs2 = ps2.executeQuery();

			while(rs2.next()){
				ConfigurazioneApplicazione p = new ConfigurazioneApplicazione();
				res.get(0).setFraseResidenza(rs2.getString("valore"));
			}
			
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}
	
//	public List<MutuiAssociatiCodFisBean> getSituazioneContabile(String matricola) throws DaoException {
//
//		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();
//
//		try {
//			String sql =  " select dbo.change_sededescr(sedi.codicesap,sedi.[DESL SEDE]) AS sedeDesc, tbl.*  "
//					+ " from "
//					+ " (select rtrim(ben.cognome) as cognome, rtrim(ben.nome) as nome, ben.mtr as matricola, "
//					+ " ben.sede as sede, sb.anno_mutuo as anno_mutuo, sb.prog_mutuo as prog_mutuo, "
//					+ " sc.tas, sb.perc, sb.dt_dec, m.voce, m.st_mutuo, sc.tp_evn, m.tp_mutuo, "
//					+ " ben.mot_cess ,ben.desc_cess "
//					+ " from storiabeneficiari sb "
//					+ " inner join beneficiari ben on (sb.mtr = ben.mtr and sb.st_evn <> '00') "
//					+ " inner join mutuo m on (m.anno_mutuo = sb.anno_mutuo and m.prog_mutuo = sb.prog_mutuo) "
//					+ " inner join storiacondizioni sc on sc.anno_mutuo = sb.anno_mutuo and sc.prog_mutuo = sb.prog_mutuo and sc.dt_ora_inser = "
//					+ " (SELECT MAX(dt_ora_inser) "
//					+ " FROM StoriaCondizioni "
//					+ " WHERE anno_mutuo = sc.anno_mutuo AND prog_mutuo = sc.prog_mutuo) "
//					+ " where sb.dt_dec = (SELECT MAX(dt_dec) "
//					+ " FROM StoriaBeneficiari "
//					+ " WHERE  anno_mutuo = m.anno_mutuo AND prog_mutuo = m.prog_mutuo and st_evn<>'00'))tbl "
//					//+ " inner join openquery ([sqlinpssvil04\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi "
//					+ " inner join openquery ([sqlinpssvil04\\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi  "
//					+ " ON sedi.CODICESAP COLLATE DATABASE_DEFAULT = tbl.sede COLLATE DATABASE_DEFAULT and tbl.matricola in ('"+matricola+"')  "
//					+ " AND ST_MUTUO='01' "
//					+ " order by cognome, nome, sedeDesc ";
//
//
//			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
//
//			logger.debug("getMutuo query" + sql);
//			ResultSet rs = ps.executeQuery();
//
//			while(rs.next()){
//				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
//				
//				m.setSedeDesc(("sedeDesc"));
//				m.setCognome(rs.getString("cognome"));
//				m.setNome(rs.getString("nome"));
//				m.setMatricola(rs.getString("matricola"));
//				m.setSede(rs.getString("sede"));
//				m.setAnnoMutuo(rs.getString("anno_mutuo"));
//				m.setProgMutuo(rs.getString("prog_mutuo"));
//				m.setTasso(rs.getDouble("tas"));			
//				m.setPercentualeDiIntestazione(rs.getDouble("perc"));				
//				m.setDataDecorrenza(rs.getDate("dt_dec"));
//				m.setVoceContabile(rs.getString("voce"));
//				m.setStatoMutuo(rs.getString("st_mutuo"));
//				m.setTpEvn(rs.getString("tp_evn"));
//				m.setTipologiaMutuo(rs.getString("tp_mutuo"));
//				m.setMotCess(rs.getString("mot_cess"));				
//				m.setDescCess(rs.getString("desc_cess"));
//				
//				res.add(m);
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
//	}

	public List<MutuiAssociatiCodFisBean> getStoriaCondizioni(String annoMutuo, String progMutuo) throws DaoException {

		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();

		try {
//			String sql =  " select dbo.change_sededescr(sedi.codicesap,sedi.[DESL SEDE]) AS sedeDesc, tbl.*  "
//					+ " from "
//					+ " (select rtrim(ben.cognome) as cognome, rtrim(ben.nome) as nome, ben.mtr as matricola, "
//					+ " ben.sede as sede, sb.anno_mutuo as anno_mutuo, sb.prog_mutuo as prog_mutuo, "
//					+ " sc.tas, sb.perc, sb.dt_dec, m.voce, m.st_mutuo, sc.tp_evn, m.tp_mutuo, "
//					+ " ben.mot_cess ,ben.desc_cess "
//					+ " from storiabeneficiari sb "
//					+ " inner join beneficiari ben on (sb.mtr = ben.mtr and sb.st_evn <> '00') "
//					+ " inner join mutuo m on (m.anno_mutuo = sb.anno_mutuo and m.prog_mutuo = sb.prog_mutuo) "
//					+ " inner join storiacondizioni sc on sc.anno_mutuo = sb.anno_mutuo and sc.prog_mutuo = sb.prog_mutuo and sc.dt_ora_inser = "
//					+ " (SELECT MAX(dt_ora_inser) "
//					+ " FROM StoriaCondizioni "
//					+ " WHERE anno_mutuo = sc.anno_mutuo AND prog_mutuo = sc.prog_mutuo) "
//					+ " where sb.dt_dec = (SELECT MAX(dt_dec) "
//					+ " FROM StoriaBeneficiari "
//					+ " WHERE  anno_mutuo = m.anno_mutuo AND prog_mutuo = m.prog_mutuo and st_evn<>'00'))tbl "
//					//+ " inner join openquery ([sqlinpssvil04\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi "
//					+ " inner join openquery ([sqlinpssvil04\\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi  "
//					+ " ON sedi.CODICESAP COLLATE DATABASE_DEFAULT = tbl.sede COLLATE DATABASE_DEFAULT and tbl.matricola in ('"+matricola+"')  "
//					+ " AND ST_MUTUO='01' "
//					+ " order by cognome, nome, sedeDesc ";
			

			
			String sql =  " SELECT [anno_mutuo],[prog_mutuo] ,[DT_DEC] as Decorrenza,tec.[DESC] as [Tipo Evento],se.[DESC]  as [Stato evento] "	  
				  + " ,[IMP_DEB] as [Deb. Res.],tp.[DESC] as [Tipo Tasso],[TAS] as Tasso,'Mensile' as [Freq.] ,[IMP_RT] as [Imp. Rata] "
				  + " ,[NUM_RT_TOT] as [Rate tot],[IMP_ESTIN] as [Imp. Est],[DATA_SCADENZA] as Scadenza,[PROG_RT] as [Prossima rata] "
				  //+ " FROM [MUTUIFIN].[dbo].[StoriaCondizioni] sc "
				  + " FROM StoriaCondizioni sc "
				  + " INNER JOIN TipoEventoCondizioni tec on tec.TP_EVN_CON = sc.TP_EVN "
				  + " INNER JOIN StatoEvento se on se.ST_EVN = sc.ST_EVN "
				  + " INNER JOIN TipoTasso tp on tp.TP_TAS = sc.TP_TAS "
				  + " where ANNO_MUTUO = '"+annoMutuo+"' and PROG_MUTUO='"+progMutuo+"' "
				  + " order by Decorrenza desc ";

			System.out.println("28_Sett sql : " +sql);
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
				//System.out.println("29_Sett rs.getString(anno_mutuo) : " +rs.getString("anno_mutuo"));
				m.setAnnoMutuo(rs.getString("anno_mutuo"));
				m.setProgMutuo(rs.getString("prog_mutuo"));
				m.setTasso(rs.getDouble("Tasso"));						
				m.setDataDecorrenza(rs.getDate("Decorrenza"));
				m.setDescTipoEvento(rs.getString("Tipo Evento"));
				m.setDescStatoEvento(rs.getString("Stato evento"));
				m.setDebitoResiduo(rs.getDouble("Deb. Res."));
				//m.setDebitoResiduo(String.valueOf(rs.getDouble("Deb. Res.")));
				m.setDescTipoTasso(rs.getString("Tipo Tasso"));
				m.setFrequenzaRate(rs.getString("Freq."));
				m.setImportoRata(rs.getDouble("Imp. Rata"));
				m.setRateTotali(rs.getDouble("Rate tot"));
				m.setImportoEstinzioni(rs.getDouble("Imp. Est"));
				m.setDataScadenza(rs.getDate("Scadenza"));
				m.setProssimaRata(rs.getDouble("Prossima rata"));
				
				res.add(m);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	public List<MutuiAssociatiCodFisBean> getStoriaBeneficiari(String matricola,String annoMutuo, String progMutuo) throws DaoException {
		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();
		try {		
			String sql =  " select max(dt_dec) as 'Ultima data decorrenza', Tp_BEN, anno_mutuo,prog_mutuo,mtr "
					+ " from StoriaBeneficiari "
					+ " where anno_mutuo='"+annoMutuo+"' and prog_mutuo='"+progMutuo+"' and mtr='"+matricola+"' "
					+ " group by anno_mutuo, prog_mutuo,mtr, dt_dec, TP_BEN ";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
				
				m.setDataDecorrenza(rs.getDate("Ultima data decorrenza"));
				m.setTpBen(rs.getString("Tp_BEN"));
				//m.setNome(rs.getString("nome"));
				//m.setMatricola(rs.getString("mtr"));
				m.setAnnoMutuo(rs.getString("anno_mutuo"));
				m.setProgMutuo(rs.getString("prog_mutuo"));
				
				res.add(m);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	public List<MutuiAssociatiCodFisBean> getStoriaBeneficiariTitolari(String matricola,String annoMutuo, String progMutuo) throws DaoException {
		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();
		try {		
			
			String sql =  " select [ANNO_MUTUO] "
					+ " ,[PROG_MUTUO] "
				    + " ,[DT_DEC] as Decorrenza "
					+ "  ,teb.[DESC] as [Tipo Evento] "
					+ "   ,se.[DESC]  as [Stato evento]	"
					+ "  ,sb.MTR as matricola "
					+ "  ,iif(FLAG_INT = 'S', 'SI', 'NO') as [Int principale] "
					+ "  ,iif(tp_ben = 'T', 'TITOLARE', 'EREDE') as [Tipo Beneficiario] "
					+ "  ,tp.[DESC] as [tipo pagamento] "
					+ "  ,PERC_DETR as [% Detr. Fisc] "
					+ "  ,bf.SEDE "
					+ "	,dbo.change_sededescr(sedi.codicesap,sedi.[DESL SEDE]) AS sedeDesc "
					+ "	,nominativo.cognome "
					+ "	,nominativo.nome "
						
					+ " from StoriaBeneficiari sb "
					+ " INNER JOIN TipoEventoBeneficiari teb on teb.TP_EVN_BEN = sb.TP_EVN "
					+ " INNER JOIN StatoEvento se on se.ST_EVN = sb.ST_EVN "
					+ " INNER JOIN TipoPagamento tp on tp.TP_PGM = sb.TP_PGM "
					+ " INNER JOIN Beneficiari bf on bf.MTR = sb.MTR "

					//+ " inner join openquery ([sqlinpssvil04\\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE " 	 	
					+ " inner join V_MT_SEDI sedi ON sedi.CODICESAP COLLATE "
					
					
					+ "	DATABASE_DEFAULT = bf.sede COLLATE DATABASE_DEFAULT and bf.MTR = '"+matricola+"' "
	
					//+ " inner join openquery ([sqlinpssvil04\\sql04], ' "
					+ " inner join  "
					+ " (SELECT matranag, CODFIS, nome, cognome "
					//+ " FROM veganetmigr.dbo.V_MT_PERSONALE "  
					+ " FROM V_MT_PERSONALE "  
					+ " Union " 
					+ " SELECT matranag, CODFIS,nome, cognome "
					//+ " FROM veganetmigr.dbo.V_MT_EREDI "
					+ " FROM V_MT_EREDI "
					+ " Union "
					+ " SELECT matranag, CODFIS,nome, cognome "
					//+ " FROM veganetmigr.dbo.V_MT_EREDIPERS) ' ) nominativo ON nominativo.matranag COLLATE 	"
					//+ " FROM V_MT_EREDIPERS) ' ) nominativo ON nominativo.matranag COLLATE 	"
					+ " FROM V_MT_EREDIPERS) nominativo ON nominativo.matranag COLLATE 	"
					+ "	DATABASE_DEFAULT = bf.MTR COLLATE DATABASE_DEFAULT "
					+ " where anno_mutuo='"+annoMutuo+"' and prog_mutuo='"+progMutuo+"' and sb.MTR='"+matricola+"' and sb.ST_EVN <>'00'"
					+ " order by sb.DT_DEC desc ";


			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
				
					m.setCognome(rs.getString("cognome"));
					m.setNome(rs.getString("nome"));	
					m.setSedeDesc(("sedeDesc"));
					m.setSede(rs.getString("SEDE"));				
					m.setDetrazioneFiscale(rs.getDouble("% Detr. Fisc"));
					m.setDescTipoPagamento(rs.getString("tipo pagamento"));
					m.setDescTipoBeneficiario(rs.getString("Tipo Beneficiario"));
					m.setIntestatarioPrincipale(rs.getString("Int principale"));
					m.setDescTipoEvento(rs.getString("Tipo Evento"));
					m.setDescStatoEvento(rs.getString("Stato evento"));
					m.setDataDecorrenza(rs.getDate("Decorrenza"));
					m.setMatricola(rs.getString("matricola"));
					m.setAnnoMutuo(rs.getString("ANNO_MUTUO"));
					m.setProgMutuo(rs.getString("PROG_MUTUO"));
				
				res.add(m);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	public List<MutuiAssociatiCodFisBean> getStoriaBeneficiariEredi(String annoMutuo, String progMutuo) throws DaoException {
		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();
		try {					
			String sql =  " select [ANNO_MUTUO] "
					+ " ,[PROG_MUTUO] "
				    + "  ,[DT_DEC] as Decorrenza "
					+ "   ,teb.[DESC] as [Tipo Evento] "
					+ "   ,se.[DESC]  as [Stato evento]	 "
					+ "   ,sb.MTR as matricola "
					+ "   ,iif(FLAG_INT = 'S', 'SI', 'NO') as [Int principale] "
					+ "   ,iif(tp_ben = 'T', 'TITOLARE', 'EREDE') as [Tipo Beneficiario] "
					+ "   ,tp.[DESC] as [tipo pagamento] "
					+ "   ,PERC_DETR as [% Detr. Fisc] "
					+ "   ,bf.SEDE "
					+ "	,dbo.change_sededescr(sedi.codicesap,sedi.[DESL SEDE]) AS sedeDesc "
					+ "	,nominativo.cognome "
					+ "	,nominativo.nome "						
					+ " from StoriaBeneficiari sb "
					+ " INNER JOIN TipoEventoBeneficiari teb on teb.TP_EVN_BEN = sb.TP_EVN "
					+ " INNER JOIN StatoEvento se on se.ST_EVN = sb.ST_EVN "
					+ " INNER JOIN TipoPagamento tp on tp.TP_PGM = sb.TP_PGM "
					+ " INNER JOIN Beneficiari bf on bf.MTR = sb.MTR "
				    
					//+ " inner join openquery ([sqlinpssvil04\\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE "		
					+ " inner join V_MT_SEDI sedi ON sedi.CODICESAP COLLATE "
					
				    + "					DATABASE_DEFAULT = bf.sede COLLATE DATABASE_DEFAULT "
				    //+ " inner join openquery ([sqlinpssvil04\\sql04], ' "
				    + " inner join  "
					+ " (SELECT matranag, CODFIS, nome, cognome "
					//+ " FROM veganetmigr.dbo.V_MT_PERSONALE  "
					+ " FROM V_MT_PERSONALE  "
					+ " Union "
					+ " SELECT matranag, CODFIS,nome, cognome "
					//+ " FROM veganetmigr.dbo.V_MT_EREDI "
					+ " FROM V_MT_EREDI "
					+ " Union "
					+ " SELECT matranag, CODFIS,nome, cognome "
					//+ " FROM veganetmigr.dbo.V_MT_EREDIPERS) ' ) nominativo ON nominativo.matranag COLLATE 	"	
					//+ " FROM V_MT_EREDIPERS) ' ) nominativo ON nominativo.matranag COLLATE 	"
					+ " FROM V_MT_EREDIPERS)  nominativo ON nominativo.matranag COLLATE 	"
					+ "	DATABASE_DEFAULT = bf.MTR COLLATE DATABASE_DEFAULT  "
					+ " where anno_mutuo='"+annoMutuo+"' and prog_mutuo='"+progMutuo+"' and sb.ST_EVN <>'00' "
					+ " order by sb.DT_DEC desc ";



			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
				
					m.setCognome(rs.getString("cognome"));
					m.setNome(rs.getString("nome"));	
					m.setSedeDesc(("sedeDesc"));
					m.setSede(rs.getString("SEDE"));				
					m.setDetrazioneFiscale(rs.getDouble("% Detr. Fisc"));
					m.setDescTipoPagamento(rs.getString("tipo pagamento"));
					m.setDescTipoBeneficiario(rs.getString("Tipo Beneficiario"));
					m.setIntestatarioPrincipale(rs.getString("Int principale"));
					m.setDescTipoEvento(rs.getString("Tipo Evento"));
					m.setDescStatoEvento(rs.getString("Stato evento"));
					m.setDataDecorrenza(rs.getDate("Decorrenza"));
					m.setMatricola(rs.getString("matricola"));
					m.setAnnoMutuo(rs.getString("ANNO_MUTUO"));
					m.setProgMutuo(rs.getString("PROG_MUTUO"));
				
				res.add(m);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	public List<MutuiAssociatiCodFisBean> getSituazioneContabileTitolari(String matricola,String annoMutuo, String progMutuo) throws DaoException {
		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();
		try {		
			String sql =  " select [ANNO_MUTUO],[PROG_MUTUO],sc.[MTR],nominativo.cognome ,nominativo.nome, [PROG_RT] as [Num Rata]  "
					+ " ,(left(AM_RIF,4)+'/'+right(AM_RIF,2)) as [Annno/mese riferimento] "
					+ " ,(left(AM_OPE,4)+'/'+right(AM_OPE,2)) as [Annno/mese operazione] "									
					+ " ,sc.[ST_MOV],sm.[desc] as [Stato mov], sc.[TP_MOV],tm.[desc] as [Tipo mov] "
					+ " ,[VOCE], [IMP_MOV], iif(IMP_MOV>0,'Trattenuta','Rimborso') as [Segno mov] "
					+ " ,sc.[SEDE],dbo.change_sededescr(sedi.codicesap,sedi.[DESL SEDE]) AS sedeDesc,[DT_PGM],sc.[TP_PGM],tp.[Desc] as [Tipo pagamento] "
					+ " from SituazioneContabile sc "
					+ " INNER JOIN StatoMovimento sm on sm.ST_MOV = sc.ST_MOV "
					+ " inner join TipoMovimento tm on tm.TP_MOV=sc.TP_MOV "
					+ " INNER JOIN TipoPagamento tp on tp.TP_PGM = sc.TP_PGM "
					+ " INNER JOIN Beneficiari bf on bf.MTR = sc.MTR "
					//+ " inner join openquery ([sqlinpssvil04\\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE DATABASE_DEFAULT = sc.sede COLLATE DATABASE_DEFAULT "
					+ " inner join V_MT_SEDI sedi ON sedi.CODICESAP COLLATE DATABASE_DEFAULT = sc.sede COLLATE DATABASE_DEFAULT "
					+ " and bf.MTR = '"+matricola+"' "
					//+ " inner join openquery ([sqlinpssvil04\\sql04], ' "
					+ " inner join "
					+ " (SELECT matranag, CODFIS, nome, cognome "
					//+ " FROM veganetmigr.dbo.V_MT_PERSONALE "
					+ " FROM V_MT_PERSONALE "
					+ " Union "
					+ " SELECT matranag, CODFIS,nome, cognome "
					//+ " FROM veganetmigr.dbo.V_MT_EREDI Union "
					+ " FROM V_MT_EREDI Union "
					+ " SELECT matranag, CODFIS,nome, cognome "
					//+ " FROM veganetmigr.dbo.V_MT_EREDIPERS) ' ) nominativo ON nominativo.matranag COLLATE DATABASE_DEFAULT = bf.MTR COLLATE DATABASE_DEFAULT "
					//+ " FROM V_MT_EREDIPERS) ' ) nominativo ON nominativo.matranag COLLATE DATABASE_DEFAULT = bf.MTR COLLATE DATABASE_DEFAULT "
					+ " FROM V_MT_EREDIPERS) nominativo ON nominativo.matranag COLLATE DATABASE_DEFAULT = bf.MTR COLLATE DATABASE_DEFAULT "
					+ " where anno_mutuo='"+annoMutuo+"' and prog_mutuo='"+progMutuo+"' and sc.ST_MOV <>'00' "
					+ " and sc.MTR='"+matricola+"' "
					+ " order by PROG_RT desc ";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
				
				m.setProgressivoRata(rs.getDouble("Num Rata"));
				m.setVoceContabile(rs.getString("VOCE"));
				m.setImportoMov(rs.getDouble("IMP_MOV"));
				m.setAnnnoMeseRiferimento(rs.getString("Annno/mese riferimento"));
				m.setAnnoMeseOperazione(rs.getString("Annno/mese operazione"));
				m.setST_MOV(rs.getString("ST_MOV"));
				m.setTP_MOV(rs.getString("TP_MOV"));
				m.setDescStatoMov(rs.getString("Stato mov"));
				m.setDescTipoMov(rs.getString("Tipo mov"));
				m.setSegnoMov(rs.getString("Segno mov"));
				m.setDtPgm(rs.getString("DT_PGM"));
				m.setTpPgm(rs.getString("TP_PGM"));				
				m.setCognome(rs.getString("cognome"));
				m.setNome(rs.getString("nome"));	
				m.setSedeDesc(rs.getString("sedeDesc"));
				m.setSede(rs.getString("SEDE"));								
				m.setDescTipoPagamento(rs.getString("Tipo pagamento"));
				m.setMatricola(rs.getString("MTR"));
				m.setAnnoMutuo(rs.getString("ANNO_MUTUO"));
				m.setProgMutuo(rs.getString("PROG_MUTUO"));
				
				res.add(m);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	public List<MutuiAssociatiCodFisBean> getSituazioneContabileEredi(String annoMutuo, String progMutuo) throws DaoException {
		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();
		try {					
			String sql =  " select [ANNO_MUTUO],[PROG_MUTUO],sc.[MTR],nominativo.cognome ,nominativo.nome, [PROG_RT] as [Num Rata] "
					+ " ,(left(AM_RIF,4)+'/'+right(AM_RIF,2)) as [Annno/mese riferimento] "
					+ " ,(left(AM_OPE,4)+'/'+right(AM_OPE,2)) as [Annno/mese operazione] "
					+ " ,sc.[ST_MOV],sm.[desc] as [Stato mov], sc.[TP_MOV],tm.[desc] as [Tipo mov] "
					+ " ,[VOCE], [IMP_MOV], iif(IMP_MOV>0,'Trattenuta','Rimborso') as [Segno mov] "
					+ " ,sc.[SEDE],dbo.change_sededescr(sedi.codicesap,sedi.[DESL SEDE]) AS sedeDesc,[DT_PGM],sc.[TP_PGM],tp.[Desc] as [Tipo pagamento] "
					+ " from SituazioneContabile sc "
					+ " INNER JOIN StatoMovimento sm on sm.ST_MOV = sc.ST_MOV "
					+ " inner join TipoMovimento tm on tm.TP_MOV=sc.TP_MOV "
					+ " INNER JOIN TipoPagamento tp on tp.TP_PGM = sc.TP_PGM "
					+ " INNER JOIN Beneficiari bf on bf.MTR = sc.MTR "
					//+ " inner join openquery ([sqlinpssvil04\\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE DATABASE_DEFAULT = sc.sede COLLATE DATABASE_DEFAULT "
					+ " inner join V_MT_SEDI sedi ON sedi.CODICESAP COLLATE DATABASE_DEFAULT = sc.sede COLLATE DATABASE_DEFAULT "
					//+ " and bf.MTR = '"+matricola+"' "
					//+ " inner join openquery ([sqlinpssvil04\\sql04], ' "
					+ " inner join  "
					+ " (SELECT matranag, CODFIS, nome, cognome "
					//+ " FROM veganetmigr.dbo.V_MT_PERSONALE "
					+ " FROM V_MT_PERSONALE "
					+ " Union "
					+ " SELECT matranag, CODFIS,nome, cognome "
					//+ " FROM veganetmigr.dbo.V_MT_EREDI Union "
					+ " FROM V_MT_EREDI Union "
					+ " SELECT matranag, CODFIS,nome, cognome "
					//+ " FROM veganetmigr.dbo.V_MT_EREDIPERS) ' ) nominativo ON nominativo.matranag COLLATE DATABASE_DEFAULT = bf.MTR COLLATE DATABASE_DEFAULT "
					//+ " FROM V_MT_EREDIPERS) ' ) nominativo ON nominativo.matranag COLLATE DATABASE_DEFAULT = bf.MTR COLLATE DATABASE_DEFAULT "
					+ " FROM V_MT_EREDIPERS) nominativo ON nominativo.matranag COLLATE DATABASE_DEFAULT = bf.MTR COLLATE DATABASE_DEFAULT "
					+ " where anno_mutuo='"+annoMutuo+"' and prog_mutuo='"+progMutuo+"' and sc.ST_MOV <>'00' "
					//+ " and sc.MTR='"+matricola+"' "
					+ " order by PROG_RT desc ";


			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();				
				
				m.setProgressivoRata(rs.getDouble("Num Rata"));
				m.setVoceContabile(rs.getString("VOCE"));
				m.setImportoMov(rs.getDouble("IMP_MOV"));
				m.setAnnnoMeseRiferimento(rs.getString("Annno/mese riferimento"));
				m.setAnnoMeseOperazione(rs.getString("Annno/mese operazione"));
				m.setST_MOV(rs.getString("ST_MOV"));
				m.setTP_MOV(rs.getString("TP_MOV"));
				m.setDescStatoMov(rs.getString("Stato mov"));
				m.setDescTipoMov(rs.getString("Tipo mov"));
				m.setSegnoMov(rs.getString("Segno mov"));
				m.setDtPgm(rs.getString("DT_PGM"));
				m.setTpPgm(rs.getString("TP_PGM"));				
				m.setCognome(rs.getString("cognome"));
				m.setNome(rs.getString("nome"));	
				m.setSedeDesc(rs.getString("sedeDesc"));
				m.setSede(rs.getString("SEDE"));								
				m.setDescTipoPagamento(rs.getString("Tipo pagamento"));
				m.setMatricola(rs.getString("MTR"));
				m.setAnnoMutuo(rs.getString("ANNO_MUTUO"));
				m.setProgMutuo(rs.getString("PROG_MUTUO"));
				
				res.add(m);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}

	public List<MutuiAssociatiCodFisBean> getPianoAmmortamento(String matricola) throws DaoException {

		List<MutuiAssociatiCodFisBean> res = new ArrayList<MutuiAssociatiCodFisBean>();

		try {
			
			String sql =  " select "
					+ " tbl.anno_mutuo+'/'+tbl.prog_mutuo As [Numero Domanda], tbl.anno_mutuo,tbl.prog_mutuo,tbl.matricola,tbl.cognome, "
					+ " tbl.nome,dbo.change_sededescr(sedi.codicesap,sedi.[DESL SEDE]) AS sedeDesc, tbl.sede, tbl.st_mutuo, "
					+ " tpm.[DESC] as [Tipologia Mutuo], tbl.voce as [Voce Contabile], tbl.tas as Tasso, tbl.PERC as [Percentuale di Intestazione],stm.[DESC] as [Stato Mutuo] , "
		
					+ " iif (substring(tbl.matricola,7,8) ='00', "
					+ " iif ((tbl.mot_cess is NULL or tbl.mot_cess=''),'In Servizio', iif(tbl.mot_cess='7', 'Deceduto', 'Cessato')), "
					+ " iif (substring(tbl.matricola,7,8) >='40', 'Erede','Portiere')) as [Stato Intestatario] "
		
					+ " from (select rtrim(ben.cognome) as cognome, "
					+ " rtrim(ben.nome) as nome, ben.mtr as matricola, ben.sede as sede, "
					+ " sb.anno_mutuo as anno_mutuo, sb.prog_mutuo as prog_mutuo, "
					+ " sc.tas, sb.perc, sb.dt_dec, m.voce, m.st_mutuo, sc.tp_evn, m.tp_mutuo, ben.mot_cess ,ben.desc_cess "
					+ " from storiabeneficiari sb inner join beneficiari ben on (sb.mtr = ben.mtr and sb.st_evn <> '00') "
					+ " inner join mutuo m on (m.anno_mutuo = sb.anno_mutuo and m.prog_mutuo = sb.prog_mutuo) "
					+ " inner join storiacondizioni sc on sc.anno_mutuo = sb.anno_mutuo "
					+ " and sc.prog_mutuo = sb.prog_mutuo and sc.dt_ora_inser = "
					+ " (SELECT MAX(dt_ora_inser) FROM StoriaCondizioni "
					+ " WHERE anno_mutuo = sc.anno_mutuo AND prog_mutuo = sc.prog_mutuo) "
					+ " where sb.dt_dec = (SELECT MAX(dt_dec) FROM StoriaBeneficiari "
					+ " WHERE anno_mutuo = m.anno_mutuo AND prog_mutuo = m.prog_mutuo and st_evn<>'00'))tbl "
					//inner join openquery ([sqlinpssvil04\sql04],'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE			
					//+ " inner join openquery ([sqlinpssvil04\\sql04], 'select * from veganetmigr.dbo.V_MT_SEDI' ) sedi ON sedi.CODICESAP COLLATE "		
					+ " inner join V_MT_SEDI sedi ON sedi.CODICESAP COLLATE "
					+ " DATABASE_DEFAULT = tbl.sede COLLATE DATABASE_DEFAULT and tbl.matricola in ('"+matricola+"')  "
					+ " inner join TipoMutuo tpm on tpm.TP_MUTUO=tbl.TP_MUTUO "
					+ " inner join StatoMutuo stm on stm.ST_MUTUO=tbl.ST_MUTUO "
					+ " order by tbl.cognome, tbl.nome, sedeDesc ";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getMutuo query" + sql);
			ResultSet rs = ps.executeQuery();

			while(rs.next()){
				MutuiAssociatiCodFisBean m = new MutuiAssociatiCodFisBean();
				
				m.setAnnoMutuo(rs.getString("anno_mutuo"));
				m.setProgMutuo(rs.getString("prog_mutuo"));
				m.setMatricola(rs.getString("matricola"));
				m.setCognome(rs.getString("cognome"));
				m.setNome(rs.getString("nome"));				
				m.setSedeDesc(rs.getString("sedeDesc"));
				m.setSede(rs.getString("sede"));
				m.setDescTipologiaMutuo(rs.getString("Tipologia Mutuo"));
				m.setVoceContabile(rs.getString("Voce Contabile"));				
				m.setTasso(rs.getDouble("Tasso"));			
				m.setPercentualeDiIntestazione(rs.getDouble("Percentuale di Intestazione"));						
				m.setDescStatoMutuo(rs.getString("Stato Mutuo"));
				m.setStatoMutuo(rs.getString("st_mutuo"));
				m.setStatoIntestatario(rs.getString("Stato Intestatario"));
				
				res.add(m);
			}
			rs.close();	
			ps.close();
			return res;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}

	public byte[] getDocument(String cf, String id) throws DaoException {

		try {

			String sql = "select FILE_CONTENT from CERTIFICAZIONI_FISCALI where ID = '"+id+"' and CODFIS_BEN ='"+cf+"' ";
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			logger.debug("getDocumentById query" + sql);

			ResultSet rs = ps.executeQuery();
			byte[] b =  null;

			if(rs.next()){	
				b = rs.getBytes("FILE_CONTENT");
			}

			rs.close();
			ps.close();
			return b;
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	}


	public byte[] getBinaryToPdf(String annoMutuo, String progMutuo, String matricola, String pathNomeFile) throws Exception {


		try {
			File someFile = new File(pathNomeFile+".pdf");

			String SPsql = "SELECT FILE_CONTENT FROM LETTEREBLOB WHERE ANNO_MUTUO = '"+annoMutuo+"' AND PROG_MUTUO = '"+progMutuo+"' AND MTR = '"+matricola+"'";
			PreparedStatement ps1 = mutuifinConn.prepareStatement(SPsql);
			ResultSet rs1 = ps1.executeQuery();
			byte[] b=  null;
			if(rs1.next()){	 
				b=  new byte[rs1.getBytes("FILE_CONTENT").length];
				b=rs1.getBytes("FILE_CONTENT");
			}

			FileOutputStream fos = new FileOutputStream(someFile);
			fos.write(b);
			fos.flush();
			fos.close();

			return b;
		}			
		catch(Exception e) {
			throw e;
		}
		finally {
			//con.close();
		}

	}



	public int insertPdf(CertFisBean  certFisBean , byte[] source, String annoCert, boolean flRett, Timestamp dataEsecuzione, String statoElab) throws DaoException {
		String[] returnId = { "ID" };
		int res = 0;
		boolean flVisibile=false;
		if(certFisBean.getTipoCertif().equalsIgnoreCase(CodiciCerificazione.TIPO_CERT1) || 
				certFisBean.getTipoCertif().equalsIgnoreCase(CodiciCerificazione.TIPO_CERT3)){
			flVisibile=true;
		}

		if((certFisBean.getTipoCertif().equalsIgnoreCase(CodiciCerificazione.TIPO_CERT2) || 
				certFisBean.getTipoCertif().equalsIgnoreCase(CodiciCerificazione.TIPO_CERT4)) && statoElab.equalsIgnoreCase("E")){
			flVisibile=true;
		}

		try {

			ByteArrayInputStream bis = null;

			if(source!=null)
				bis = new ByteArrayInputStream(source);

			String query = "INSERT INTO CERTIFICAZIONI_FISCALI(DT_ORA_INSER,ANNO_CERT,ANNO_MUTUO,PROG_MUTUO,CODFIS_BEN,CODFIS_DEC,COD_LET,FL_RETT,FL_VISIBLE,MTR_BEN,MTR_DEC";
			if(source!=null)
				query+= ",FILE_CONTENT)  VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
			else
				query+= ")  VALUES(?,?,?,?,?,?,?,?,?,?,?)";

			PreparedStatement pstmt = mutuifinConn.prepareStatement(query, returnId);
			pstmt.setTimestamp(1, dataEsecuzione);
			pstmt.setString(2, annoCert);
			pstmt.setString(3, certFisBean.getRigo().get(certFisBean.getRigo().size()-1).getMutuoDaCertificare().getAnnoMutuo());
			pstmt.setString(4, certFisBean.getRigo().get(certFisBean.getRigo().size()-1).getMutuoDaCertificare().getProgMutuo());
			if(!FormalCheckUtils.isEmptyString(certFisBean.getBenefPrincipale().getCodiceFiscale()))
				pstmt.setString(5, certFisBean.getBenefPrincipale().getCodiceFiscale());
			else 
				pstmt.setString(5,"");
			if(certFisBean.getTipoCertif().equalsIgnoreCase(CodiciCerificazione.TIPO_CERT3) ||
					certFisBean.getTipoCertif().equalsIgnoreCase(CodiciCerificazione.TIPO_CERT4)	 ) {
				pstmt.setString(6, certFisBean.getRigo().get(certFisBean.getRigo().size()-1).getBenefCertif().getCodiceFiscale());
				pstmt.setString(11, certFisBean.getRigo().get(certFisBean.getRigo().size()-1).getBenefCertif().getMatricola());
			}
			else { 
				pstmt.setString(6, "");
				pstmt.setString(11, "");
			}
			pstmt.setString(7, certFisBean.getTipoCertif());
			pstmt.setInt(8,flRett?1:0);
			if(source!=null)
				pstmt.setBinaryStream(12, bis, source.length);

			pstmt.setInt(9,flVisibile?1:0);
			pstmt.setString(10, certFisBean.getBenefPrincipale().getMatricola());

			logger.info("insertPdf " + query);

			int executed = pstmt.executeUpdate();

			if(executed==0) 
				throw new DaoException("Creating user failed, no rows affected.");

			ResultSet rs = pstmt.getGeneratedKeys(); 
			if (rs.next()) {
				res = rs.getInt(1);
			}
			rs.close();
			pstmt.close();		
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

		return res;
	}

	//METODO CHE INSERISCE IL PDF DELLE LETTERE: L003, L004 E L005 NELLA TABELLA LETTERE_FILE
	public int insertPdfIntoLettereFile(CessazionePdfObj pdfObj , byte[] source, String tipoLettera) throws DaoException {
		String[] returnId = { "ID" };
		int res = 0;

		try {

			ByteArrayInputStream bis = null;

			if(source!=null)
				bis = new ByteArrayInputStream(source);

			String query = "INSERT INTO LETTERE_FILE(ANNO_MUTUO,PROG_MUTUO,MTR,COD_LET,DT_ORA_INSER,FILE_CONTENT) ";
			query       += "VALUES(?,?,?,?,?,?)";

			PreparedStatement pstmt = mutuifinConn.prepareStatement(query, returnId);
			pstmt.setString(1, pdfObj.getAnnoMutuo());
			pstmt.setString(2, pdfObj.getProgMutuo());
			pstmt.setString(3, pdfObj.getBenefPrincipale().getMatricola());
			pstmt.setString(4, tipoLettera);
			pstmt.setTimestamp(5, new Timestamp(new Date().getTime()));
			pstmt.setBinaryStream(6, bis, source.length);

			logger.info("insertPdfIntoLettereFile " + query);

			int executed = pstmt.executeUpdate();

			if(executed==0) 
				throw new DaoException("Creating user failed, no rows affected.");

			ResultSet rs = pstmt.getGeneratedKeys(); 
			if (rs.next()) {
				res = rs.getInt(1);
			}
			rs.close();
			pstmt.close();		
		}			
		catch(Exception e) {
			logger.info("ECCEZIONE METODO insertPdfIntoLettereFile: " + e.getMessage());
			throw new DaoException(e);
		}

		return res;
	}
	
	public byte[] getBinaryFromLettereFile(String annoMutuo, String progMutuo, String matricola, String dtOraInser) throws Exception {

		try {
			String SPsql = "SELECT FILE_CONTENT FROM LETTERE_FILE WHERE ANNO_MUTUO = '"+annoMutuo+"' "+
						   "AND PROG_MUTUO = '"+progMutuo+"' AND MTR = '"+matricola+"' "+
						   "AND SUBSTRING(convert(varchar, DT_ORA_INSER, 20), 0, LEN(convert(varchar, DT_ORA_INSER, 20))-2) = "+
						   "CONVERT(varchar,'"+dtOraInser+"', 20)";
//						   "AND DT_ORA_INSER = (SELECT MAX(DT_ORA_INSER) "+
//						   "FROM LETTERE_FILE "+
//						   "WHERE ANNO_MUTUO = '"+annoMutuo+"' "+
//						   "AND PROG_MUTUO = '"+progMutuo+"' AND MTR = '"+matricola+"') ";
			PreparedStatement ps1 = mutuifinConn.prepareStatement(SPsql);
			logger.info("QUERY METODO 'getBinaryFromLettereFile':  "+SPsql);
			ResultSet rs1 = ps1.executeQuery();
			byte[] b=  null;
			if(rs1.next()){	 
				b=  new byte[rs1.getBytes("FILE_CONTENT").length];
				b=rs1.getBytes("FILE_CONTENT");
			}
			return b;
		}			
		catch(Exception e) {
			logger.info("Exception nel metodo getBinaryFromLettereFile " +e.getMessage() );
			throw e;
		}

	}


	public void insertLogDatiMutuo(String caller, Timestamp dateIns, String operation, String codFiscale, String id, String codErrore, String descErrore, String ipAddress, String annoMutuo, String progMutuo) throws DaoException {
		
		String query = "INSERT INTO LOG_CERTIFICAZIONI_FISCALI(APP_NAME_CHIAMANTE,DT_ORA_LOG,OPERAZIONE,CODFIS_UTENTE,ID_DOCUMENTO,COD_ERRORE,DESC_ERRORE,IP_CHIAMANTE,ANNO_MUTUO,PROG_MUTUO)  VALUES(?,?,?,?,?,?,?,?,?,?)";

		try {
			PreparedStatement pstmt = mutuifinConn.prepareStatement(query);
			
			pstmt.setString(1, caller);
			pstmt.setTimestamp(2, dateIns);
			pstmt.setString(3, operation);
			pstmt.setString(4, codFiscale);
			pstmt.setString(5, id);
			pstmt.setString(6, codErrore);
			pstmt.setString(7, descErrore);
			pstmt.setString(8, ipAddress);
			pstmt.setString(9, annoMutuo);
			pstmt.setString(10, progMutuo);
			
			pstmt.executeUpdate(); 
			pstmt.close();		
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}

	public void insertLog(String caller, Timestamp dateIns, String operation, String codFiscale, String id, String codErrore, String descErrore, String ipAddress) throws DaoException {
		
		String query = "INSERT INTO LOG_CERTIFICAZIONI_FISCALI(APP_NAME_CHIAMANTE,DT_ORA_LOG,OPERAZIONE,CODFIS_UTENTE,ID_DOCUMENTO,COD_ERRORE,DESC_ERRORE,IP_CHIAMANTE)  VALUES(?,?,?,?,?,?,?,?)";

		try {
			PreparedStatement pstmt = mutuifinConn.prepareStatement(query);
			
			pstmt.setString(1, caller);
			pstmt.setTimestamp(2, dateIns);
			pstmt.setString(3, operation);
			pstmt.setString(4, codFiscale);
			pstmt.setString(5, id);
			pstmt.setString(6, codErrore);
			pstmt.setString(7, descErrore);
			pstmt.setString(8, ipAddress);
			
			pstmt.executeUpdate(); 
			pstmt.close();		
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}

	}

	
public int updateLog(Timestamp dateIns, String operation, String codFiscale, String id, String codErrore, String descErrore, String ipAddress) throws DaoException {
		
		String query = "update LOG_CERTIFICAZIONI_FISCALI set OPERAZIONE='"+operation+"', CODFIS_UTENTE='"+codFiscale+"',";
		if(!FormalCheckUtils.isEmptyString(id))
			query+="ID_DOCUMENTO="+id+",";
		query+=		 	   " COD_ERRORE='" + codErrore +"', DESC_ERRORE='" + descErrore +"' where DT_ORA_LOG=?";
		int rows = 0;
		try {
			PreparedStatement ps1 = mutuifinConn.prepareStatement(query);
			ps1.setTimestamp(1, dateIns);
			rows = ps1.executeUpdate();
			
			ps1.close();		
		}			
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
		
		return rows;
	}

public List<AnagraficaIntestatario> getMatricoleMutui(String codiceFis) throws DaoException {

	List<AnagraficaIntestatario> res = new ArrayList<AnagraficaIntestatario>();

	try {
		String sql =  " select u.matranag as matricola, u.CODFIS, u.nome, u.cognome "
				+ " from "
				+ " (SELECT matranag, CODFIS, nome, cognome "
				+ " FROM V_MT_PERSONALE  "
				+ " Union "
				+ " SELECT matranag, CODFIS,nome, cognome "
				+ " FROM V_MT_EREDI "
				+ " Union "
				+ " SELECT matranag, CODFIS,nome, cognome "
				+ " FROM V_MT_EREDIPERS "
				+ " ) u "
				+ " where u.CODFIS ='"+codiceFis+"'";
		

//		PreparedStatement ps = vegaConn.prepareStatement(sql);
		PreparedStatement ps = mutuifinConn.prepareStatement(sql);

		logger.debug("getMatricoleMutui query" + sql);
		ResultSet rs = ps.executeQuery();

		while(rs.next()){
			AnagraficaIntestatario p = new AnagraficaIntestatario(rs.getString("matricola"),codiceFis);
			p.setNome(rs.getString("nome"));
			p.setCognome(rs.getString("cognome"));
			res.add(p);
		}
		rs.close();	
		ps.close();
		return res;
	}			
	catch(SQLException e) {
		throw new DaoException(e);
	}
	finally {
		//con.close();
	}

}

	public ReportDettaglioMutuo getDettaglioMutuoEstinzioneAnticipata(String annoMutuo, String progrMutuo) throws DaoException {
		
		ReportDettaglioMutuo dettMutuo = new ReportDettaglioMutuo();
	
		try {
			String sql = "select sum(imp_estin) as IMP_ESTIN_TOT, sum(imp_estin_ecd) as IMP_ESTIN_TOT_ECD from StoriaCondizioni where anno_mutuo = ? and prog_mutuo = ? and st_evn = '01' ";        
			
			System.out.println("getDettaglioMutuo2: "+ sql);
								
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			
			ps.setString(1, annoMutuo);
			ps.setString(2, progrMutuo);
			
			logger.debug("getMutuo query" + sql);
			// esegue query
			ResultSet rs = ps.executeQuery();
	
			if(rs.next() && dettMutuo!=null) {
				double d = rs.getDouble("IMP_ESTIN_TOT");
				dettMutuo.setImportoEstinzioneAnticipata((rs.wasNull() ? null : Double.valueOf(d)));                      
				d = rs.getDouble("IMP_ESTIN_TOT_ECD");
				dettMutuo.setImportoEstinzioneAnticipataEcd((rs.wasNull() ? null : Double.valueOf(d)));         
			}
			
			dettMutuo.setImportoImpostaSostitutivaInt(null);
			dettMutuo.setDataPagImpostaSostInt(null);

			dettMutuo.setImportoImpostaSostitutivaCoInt(null);
			dettMutuo.setDataPagImpostaSostCoInt(null);
			
			rs.close();	
			ps.close();
			return dettMutuo;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	
	}

	public ReportDettaglioMutuo getDettaglioMutuoErogazione(String annoMutuo, String progrMutuo) throws DaoException {
		
		ReportDettaglioMutuo dettMutuo = new ReportDettaglioMutuo();
		String mtrInt = "";
		String mtrCoInt = "";		
		int ratePreamm = 0;
		
		try {
			
			//inizio A.S. reportDettaglioMutuoMap.put("reportDettaglioMutuo", reportDettaglioMutuo);
			String sql = " SELECT Mutuo.IMP_MUTUO, "+
					"Mutuo.TP_MUTUO, "+
					"Mutuo.VOCE, "+
					"Mutuo.ST_MUTUO, "+
					"Mutuo.DT_STI, "+
					"Mutuo.DT_ERO, "+
					"Mutuo.IMP_SPE, "+
					"Mutuo.IMP_ITS_PREAMM, "+
					"Mutuo.DT_NASC_MUTUO, "+
					"Mutuo.DT_CHS, "+

					"Mutuo.IMP_CAPI_1_LOTTO, "+
					"Mutuo.DT_ERO_1_LOTTO, "+		
					"Mutuo.IMP_CAPI_2_LOTTO, "+
					"Mutuo.DT_ERO_2_LOTTO, "+				
					"Mutuo.IMP_CAPI_3_LOTTO, "+		
					"Mutuo.DT_ERO_3_LOTTO, "+
				
					"Mutuo.DetrazioneFiscale, "+
					"Mutuo.Note, "+
					"Mutuo.email, "+
					"Mutuo.tel, "+
					"Mutuo.cell, "+
	
					//Mutui-48
					"TipoMutuo.FLAG_GEST_ACCONTI flagAcconti, " +

					//*************************************
					"Mutuo.IMP_MUTUO_ECD, "+		
					"Mutuo.IMP_ITS_PREAMM_ECD, "+		
					"StoriaCondizioni.TAS_ECD, "+		
					"StoriaCondizioni.IMP_RT_ECD, "+ 		
					"StoriaCondizioni.TP_TAS_ECD, "+		
					//*************************************		
					"StoriaCondizioni.TAS, "+
					"StoriaCondizioni.NUM_RT_RSD, "+ 
					"StoriaCondizioni.NUM_RT_TOT, "+
					"StoriaCondizioni.FRQ_RT, "+
					"StoriaCondizioni.IMP_RT, "+ 
					"StoriaCondizioni.DATA_SCADENZA, "+
					"StoriaCondizioni.TP_TAS "+
					"FROM         Mutuo INNER JOIN "+
					" StoriaCondizioni ON Mutuo.ANNO_MUTUO = StoriaCondizioni.ANNO_MUTUO AND "+
					" Mutuo.PROG_MUTUO = StoriaCondizioni.PROG_MUTUO " +
					"LEFT OUTER JOIN TipoMutuo " +
					"ON Mutuo.TP_MUTUO = TipoMutuo.TP_MUTUO "+

					"WHERE     (Mutuo.ANNO_MUTUO = ?) AND (Mutuo.PROG_MUTUO = ?) and StoriaCondizioni.DT_DEC = (SELECT MAX(DT_DEC) FROM StoriaCondizioni WHERE ANNO_MUTUO = ? AND PROG_MUTUO = ? AND ST_EVN <> '00') "+
					"AND StoriaCondizioni.ST_EVN <> '00'";
		
			PreparedStatement ps = mutuifinConn.prepareStatement(sql);
			ps.setString(1, annoMutuo);
			ps.setString(2, progrMutuo);
			ps.setString(3, annoMutuo);
			ps.setString(4, progrMutuo);
			logger.debug("getMutuo query" + sql);
			// esegue query
			ResultSet rs = ps.executeQuery();
	
			if(rs.next()) {
				// si, crea bean
				dettMutuo = new ReportDettaglioMutuo();
	
				// gestisce i null
				double d = rs.getDouble("IMP_MUTUO");
				dettMutuo.setImportoMutuo((rs.wasNull() ? null : Double.valueOf(d)));        	
				dettMutuo.setTipoMutuo(rs.getString("TP_MUTUO"));
				dettMutuo.setVoceContabile(rs.getString("VOCE"));
				dettMutuo.setDataStipula(rs.getDate("DT_STI"));
				dettMutuo.setDataErogazione(rs.getDate("DT_ERO"));
	
				dettMutuo.setDetrazioneFiscale(rs.getBoolean("DetrazioneFiscale"));
				dettMutuo.setNote(rs.getString("Note")!=null?rs.getString("Note"):"");			
				dettMutuo.setEmail(rs.getString("email")!=null?rs.getString("email"):"");
				dettMutuo.setTel(rs.getString("tel")!=null?rs.getString("tel"):"");
				dettMutuo.setCell(rs.getString("cell")!=null?rs.getString("cell"):"");
		    	
	
				d = rs.getDouble("IMP_SPE");
	
				System.out.println("Spese Istruttoria (Mutuo) " + annoMutuo + "/" + progrMutuo + " : " + d);
	
				dettMutuo.setSpeseIstruttoria((rs.wasNull() ? null : Double.valueOf(d)));
	
				d = rs.getDouble("IMP_ITS_PREAMM");
	
				System.out.println("Interessi Preammortamento (Mutuo) " + annoMutuo + "/" + progrMutuo + " : " + d);
	
				dettMutuo.setInteressiPreammortamento((rs.wasNull() ? null : Double.valueOf(d)));       
	
				dettMutuo.setDataNascitaMutuo(rs.getDate("DT_NASC_MUTUO"));
				dettMutuo.setDataChiusura(rs.getDate("DT_CHS"));
				dettMutuo.setTassoAttuale(Double.valueOf(rs.getDouble("TAS")));
				dettMutuo.setNumRateResidue(Integer.valueOf(rs.getInt("NUM_RT_RSD")));
				dettMutuo.setNumRateTotali(Integer.valueOf(rs.getInt("NUM_RT_TOT")));
				dettMutuo.setFrequenzaRate(Integer.valueOf(rs.getInt("FRQ_RT")));
				dettMutuo.setImportoRata(Double.valueOf(rs.getDouble("IMP_RT")));
	
				dettMutuo.setDataScadenzaPrevista(rs.getDate("DATA_SCADENZA"));
				String tpTass = rs.getString("TP_TAS");
				tpTass = (rs.wasNull()) ? "" : tpTass; 
				dettMutuo.setTipoTasso(tpTass);           
				// **************************************************        
				d = rs.getDouble("IMP_MUTUO_ECD");
				dettMutuo.setImportoMutuoEcd((rs.wasNull() ? null : Double.valueOf(d)));        	        	
				d = rs.getDouble("IMP_ITS_PREAMM_ECD");
				dettMutuo.setInteressiPreammortamentoEcd((rs.wasNull() ? null : Double.valueOf(d)));            
				dettMutuo.setTassoAttualeEcd(Double.valueOf(rs.getDouble("TAS_ECD")));        	
				dettMutuo.setImportoRataEcd(Double.valueOf(rs.getDouble("IMP_RT_ECD")));            
				String tpTassEcd = rs.getString("TP_TAS_ECD");            
				tpTassEcd = (rs.wasNull()) ? "" : tpTassEcd; 
				dettMutuo.setTipoTassoEcd(tpTassEcd);           
				// **************************************************       
	
				dettMutuo.setDataErogazionePriAcc(rs.getDate("DT_ERO"));
	
				d = rs.getDouble("IMP_CAPI_1_LOTTO");
	
				System.out.println("Importo Primo Acconto " + annoMutuo + "/" + progrMutuo + " : " + d);
	
				dettMutuo.setImportoMutuoPriAcc((rs.wasNull() ? null : Double.valueOf(d)));	
				dettMutuo.setDataErogazionePriAcc(rs.getDate("DT_ERO_1_LOTTO"));
	
				d = rs.getDouble("IMP_CAPI_2_LOTTO");
	
				System.out.println("Importo Secondo Acconto " + annoMutuo + "/" + progrMutuo + " : " + d);
	
				dettMutuo.setImportoMutuoSecAcc((rs.wasNull() ? null : Double.valueOf(d)));			
				dettMutuo.setDataErogazioneSecAcc(rs.getDate("DT_ERO_2_LOTTO"));
	
				d = rs.getDouble("IMP_CAPI_3_LOTTO");
	
				System.out.println("Importo Saldo " + annoMutuo + "/" + progrMutuo + " : " + d);
	
				dettMutuo.setImportoMutuoSaldo((rs.wasNull() ? null : Double.valueOf(d)));			
				dettMutuo.setDataErogazioneSaldo(rs.getDate("DT_ERO_3_LOTTO"));
	
				if (dettMutuo.getDataErogazionePriAcc() == null) { 
					dettMutuo.setDataErogazionePriAcc(rs.getDate("DT_ERO"));
				}
				
				//Mutui-48
				dettMutuo.setFlagGestAcconti(rs.getBoolean("flagAcconti"));
			}
			
			rs.close();	
			ps.close();
			
			
			
			//fine A.S. reportDettaglioMutuoMap.put("reportDettaglioMutuo", reportDettaglioMutuo);
			
			String sqlMDE = "Select * from Mutui_Dati_Erogazione where anno = ? and progressivo = ?";        
			
			System.out.println("getDettaglioMutuo2: "+ sqlMDE);
								
			PreparedStatement psMDE = mutuifinConn.prepareStatement(sqlMDE);
			
			//ps.clearParameters();
			psMDE.setString(1, annoMutuo);
			psMDE.setString(2, progrMutuo);		
			
			logger.debug("getMutuo query" + sqlMDE);
			// esegue query
			ResultSet rsMDE = psMDE.executeQuery();
	
			if(rsMDE.next()) {

				mtrInt = rsMDE.getString("MATR");
				if (rsMDE.getString("MATR_CO") != null && !rsMDE.getString("MATR_CO").equalsIgnoreCase("")) {
					mtrCoInt = rsMDE.getString("MATR_CO");				
				}

				ratePreamm = rsMDE.getInt("RATE_PREAMM");

				dettMutuo.setAnnoInizioErogazione(rsMDE.getString("ANNO_INI"));
				dettMutuo.setMeseInizioErogazione(rsMDE.getString("MESE_INI"));

			}
			
			rsMDE.close();	
			psMDE.close();
			
			if (ratePreamm == 0) ratePreamm = 1;
			
			String sql1 = "Select * from SituazioneContabile where anno_mutuo = ? and prog_mutuo = ? ";
			sql1+= "and tp_mov = '10' and st_mov <> '00' ";        
			sql1+= "order by mtr, dt_pgm";               

			PreparedStatement ps1 = mutuifinConn.prepareStatement(sql1);
			//ps.clearParameters();
			ps1.setString(1, annoMutuo);
			ps1.setString(2, progrMutuo);
			// esegue query
			ResultSet rs1 = ps1.executeQuery();
			
			while(rs1.next() && dettMutuo!=null) {
				double d = rs1.getDouble("IMP_MOV");

				if(mtrInt.equalsIgnoreCase(rs1.getString("MTR"))) {

					dettMutuo.setMtrInt(rs1.getString("MTR"));        	

					if (null != dettMutuo.getImportoImpostaSostitutivaInt()) {
						d = d + dettMutuo.getImportoImpostaSostitutivaInt().doubleValue();
					}

					dettMutuo.setImportoImpostaSostitutivaInt((rs1.wasNull() ? null : Double.valueOf(d)));
					dettMutuo.setDataPagImpostaSostInt(rs1.getDate("DT_PGM"));

				} else {

					dettMutuo.setMtrCoInt(rs1.getString("MTR"));        			        	

					if (null != dettMutuo.getImportoImpostaSostitutivaCoInt()) {
						d = d + dettMutuo.getImportoImpostaSostitutivaCoInt().doubleValue();
					}
					dettMutuo.setImportoImpostaSostitutivaCoInt((rs1.wasNull() ? null : Double.valueOf(d)));
					dettMutuo.setDataPagImpostaSostCoInt(rs1.getDate("DT_PGM"));
				}
			}
			
			String amRifPriAcc = "";       	
			String amRifPriAcc2 = "";       	
			java.sql.Date dtRifPriAcc = null;
			if (dettMutuo.getDataErogazionePriAcc() != null) {     	
				Calendar dtErogPriAcc = Calendar.getInstance();
				dtErogPriAcc.setTime(dettMutuo.getDataErogazionePriAcc());
				dtErogPriAcc.set(Calendar.DAY_OF_MONTH,1);			
				dtErogPriAcc.set(Calendar.MONTH,dtErogPriAcc.get(Calendar.MONTH) + 1);
				dtRifPriAcc = Utility.toSQLDate(dtErogPriAcc.getTime());
				amRifPriAcc = String.valueOf((anno(dtRifPriAcc) * 100) + mese(dtRifPriAcc));
				dtErogPriAcc.set(Calendar.MONTH,dtErogPriAcc.get(Calendar.MONTH) + ratePreamm - 1);
				dtRifPriAcc = Utility.toSQLDate(dtErogPriAcc.getTime());
				amRifPriAcc2 = String.valueOf((anno(dtRifPriAcc) * 100) + mese(dtRifPriAcc));
			}

			String amRifSecAcc = "";       	
			String amRifSecAcc2 = "";       	        
			java.sql.Date dtRifSecAcc = null;
			if (dettMutuo.getDataErogazioneSecAcc() != null) {     	
				Calendar dtErogSecAcc = Calendar.getInstance();
				dtErogSecAcc.setTime(dettMutuo.getDataErogazioneSecAcc());
				dtErogSecAcc.set(Calendar.DAY_OF_MONTH,1);			
				dtErogSecAcc.set(Calendar.MONTH,dtErogSecAcc.get(Calendar.MONTH) + 1);
				dtRifSecAcc = Utility.toSQLDate(dtErogSecAcc.getTime());
				amRifSecAcc = String.valueOf((anno(dtRifSecAcc) * 100) + mese(dtRifSecAcc));
				dtErogSecAcc.set(Calendar.MONTH,dtErogSecAcc.get(Calendar.MONTH) + ratePreamm - 1);
				dtRifSecAcc = Utility.toSQLDate(dtErogSecAcc.getTime());
				amRifSecAcc2 = String.valueOf((anno(dtRifSecAcc) * 100) + mese(dtRifSecAcc));
			}

			String amRifSaldo = "";       	
			String amRifSaldo2 = "";       	        
			java.sql.Date dtRifSaldo = null;
			if (dettMutuo.getDataErogazioneSaldo() != null) {     	
				Calendar dtErogSaldo = Calendar.getInstance();
				dtErogSaldo.setTime(dettMutuo.getDataErogazioneSaldo());
				dtErogSaldo.set(Calendar.DAY_OF_MONTH,1);			
				dtErogSaldo.set(Calendar.MONTH,dtErogSaldo.get(Calendar.MONTH) + 1);
				dtRifSaldo = Utility.toSQLDate(dtErogSaldo.getTime());
				amRifSaldo = String.valueOf((anno(dtRifSaldo) * 100) + mese(dtRifSaldo));
				dtErogSaldo.set(Calendar.MONTH,dtErogSaldo.get(Calendar.MONTH) + ratePreamm - 1);
				dtRifSaldo = Utility.toSQLDate(dtErogSaldo.getTime());
				amRifSaldo2 = String.valueOf((anno(dtRifSaldo) * 100) + mese(dtRifSaldo));
			}
			
			
			
			String sql5518 = null;
			PreparedStatement ps5518 = null;
			ResultSet rs5518 = null;

			sql5518 = "Select * from SituazioneContabile where anno_mutuo = ? and prog_mutuo = ? ";    
			sql5518+= "and tp_mov = '03' and st_mov <> '00'  ";
			sql5518+= "order by mtr";               

			ps5518 = mutuifinConn.prepareStatement(sql5518);
			// ps5518.clearParameters();
			ps5518.setString(1, annoMutuo);
			ps5518.setString(2, progrMutuo);
			// esegue query
			rs5518 = ps5518.executeQuery();
			//trovato??

			dettMutuo.setInteressiPreammortamento(null);               
			dettMutuo.setInteressiPreammortamentoEcd(null);                       

			double dPriAcc = 0;
			double dSecAcc = 0;
			double dSaldo = 0;			

			dettMutuo.setInteressiPreammortamentoPriAcc(null);
			dettMutuo.setInteressiPreammortamentoSecAcc(null);
			dettMutuo.setInteressiPreammortamentoSaldo(null);

			int amRifPriAccInt = 0;
			int amRifPriAcc2Int = 0;
			if (amRifPriAcc != null && !amRifPriAcc.equalsIgnoreCase("")) {
				amRifPriAccInt = Integer.parseInt(amRifPriAcc);
				amRifPriAcc2Int = Integer.parseInt(amRifPriAcc2);	        
			}
			int amRifSecAccInt = 0;		
			int amRifSecAcc2Int = 0;		        
			if (amRifSecAcc != null && !amRifSecAcc.equalsIgnoreCase("")) {		
				amRifSecAccInt = Integer.parseInt(amRifSecAcc);       	
				amRifSecAcc2Int = Integer.parseInt(amRifSecAcc2);       		        
			}
			int amRifSaldoInt = 0;				
			int amRifSaldo2Int = 0;				        
			if (amRifSaldo != null && !amRifSaldo.equalsIgnoreCase("")) {		        
				amRifSaldoInt = Integer.parseInt(amRifSaldo);       	                
				amRifSaldo2Int = Integer.parseInt(amRifSaldo2);       	                	        
			}
			
			while(rs5518.next() && dettMutuo!=null) {

				int amRifInt = Integer.parseInt(rs5518.getString("AM_RIF"));

				//			System.out.println("Interessi Preammortamento amRif " + annoMutuo + "/" + progrMutuo + " : " + amRifInt);

				if (amRifInt >= amRifPriAccInt && amRifInt <= amRifPriAcc2Int) {

					dPriAcc = rs5518.getDouble("IMP_MOV");

					System.out.println("Interessi Preammortamento Primo Acconto " + annoMutuo + "/" + progrMutuo + " : " + amRifInt + " " + dPriAcc);	

					if (null != dettMutuo.getInteressiPreammortamentoPriAcc()) {
						dPriAcc = dPriAcc + dettMutuo.getInteressiPreammortamentoPriAcc().doubleValue();
					}

					dettMutuo.setInteressiPreammortamentoPriAcc((rs5518.wasNull() ? null : Double.valueOf(dPriAcc)));       

				}

				if (amRifInt >= amRifSecAccInt && amRifInt <= amRifSecAcc2Int) {

					dSecAcc = rs5518.getDouble("IMP_MOV");

					System.out.println("Interessi Preammortamento Secondo Acconto " + annoMutuo + "/" + progrMutuo + " : " + amRifInt + " " + dSecAcc);	

					if (null != dettMutuo.getInteressiPreammortamentoSecAcc()) {
						dSecAcc = dSecAcc + dettMutuo.getInteressiPreammortamentoSecAcc().doubleValue();
					}

					dettMutuo.setInteressiPreammortamentoSecAcc((rs5518.wasNull() ? null : Double.valueOf(dSecAcc)));       

				}

				if (amRifInt >= amRifSaldoInt && amRifInt <= amRifSaldo2Int) {

					dSaldo = rs5518.getDouble("IMP_MOV");

					System.out.println("Interessi Preammortamento Saldo " + annoMutuo + "/" + progrMutuo + " : " + amRifInt + " " + dSaldo);	

					if (null != dettMutuo.getInteressiPreammortamentoSaldo()) {
						dSaldo = dSaldo + dettMutuo.getInteressiPreammortamentoSaldo().doubleValue();
					}

					dettMutuo.setInteressiPreammortamentoSaldo((rs5518.wasNull() ? null : Double.valueOf(dSaldo)));       

				}

				double d = rs5518.getDouble("IMP_MOV");

				System.out.println("Interessi Preammortamento " + annoMutuo + "/" + progrMutuo + " : " + d);

				if (null != dettMutuo.getInteressiPreammortamento()) {
					d = d + dettMutuo.getInteressiPreammortamento().doubleValue();
				}

				dettMutuo.setInteressiPreammortamento((rs5518.wasNull() ? null : Double.valueOf(d)));       

				double dEcd = rs5518.getDouble("IMP_MOV_ECD");        	

				System.out.println("Interessi Preammortamento Ecd " + annoMutuo + "/" + progrMutuo + " : " + dEcd);

				if (null != dettMutuo.getInteressiPreammortamentoEcd()) {
					dEcd = dEcd + dettMutuo.getInteressiPreammortamentoEcd().doubleValue();
				}

				dettMutuo.setInteressiPreammortamentoEcd((rs5518.wasNull() ? null : Double.valueOf(dEcd)));       

			}

			// chiude tutto
			ps5518.close();
			rs5518.close();
			
			String sql5519 = null;
			PreparedStatement ps5519 = null;
			ResultSet rs5519 = null;

			sql5519 = "Select * from SituazioneContabile where anno_mutuo = ? and prog_mutuo = ? ";
			sql5519+= "and tp_mov = '04' and st_mov <> '00'  ";        
			sql5519+= "order by mtr";               

			ps5519 = mutuifinConn.prepareStatement(sql5519);
			// ps5519.clearParameters();
			ps5519.setString(1, annoMutuo);
			ps5519.setString(2, progrMutuo);
			// esegue query
			rs5519 = ps5519.executeQuery();
			//trovato??

			dettMutuo.setSpeseIstruttoria(null);               

			while(rs5519.next() && dettMutuo!=null) {

				double d = rs5519.getDouble("IMP_MOV");

				System.out.println("Spese Istruttoria " + annoMutuo + "/" + progrMutuo + " : " + d);

				if (null != dettMutuo.getSpeseIstruttoria()) {
					d = d + dettMutuo.getSpeseIstruttoria().doubleValue();
				}

				dettMutuo.setSpeseIstruttoria((rs5519.wasNull() ? null : Double.valueOf(d)));       

			}

			// chiude tutto
			ps5519.close();
			rs5519.close();
			
			String sqlCP = null;
			PreparedStatement psCP = null;
			ResultSet rsCP = null;

			sqlCP = "Select * from SituazioneContabile where anno_mutuo = ? and prog_mutuo = ? ";
			sqlCP+= "and tp_mov in ('02', '03') and st_mov <> '00' and cau_mov in ('05', '06', '07', '08') ";        
			sqlCP+= "order by mtr";               

			psCP = mutuifinConn.prepareStatement(sqlCP);
			// psCP.clearParameters();
			psCP.setString(1, annoMutuo);
			psCP.setString(2, progrMutuo);
			// esegue query
			rsCP = psCP.executeQuery();
			//trovato??

			dettMutuo.setInteressiPreammortamentoRimborso(Double.valueOf(0));               
			dettMutuo.setInteressiPreammortamentoTrattenuta(Double.valueOf(0));               
			dettMutuo.setInteressiRimborso(Double.valueOf(0));               
			dettMutuo.setInteressiTrattenuta(Double.valueOf(0));                                       

			while(rsCP.next() && dettMutuo!=null) {

				double d = rsCP.getDouble("IMP_MOV");

				System.out.println("Rimborso/Trattenuta - Interessi Preammortamento/Interessi " + annoMutuo + "/" + progrMutuo +
						" " + rsCP.getString("voce") + " " + rsCP.getString("cau_mov") + " " + d);

				if (rsCP.getString("tp_mov").equalsIgnoreCase("03")) {            	

					if (rsCP.getString("cau_mov").equalsIgnoreCase("06") || rsCP.getString("cau_mov").equalsIgnoreCase("08")) {

						d = d * (-1);

						System.out.println("Rimborso - Interessi Preammortamento " + annoMutuo + "/" + progrMutuo + " : " + d);

						if (null != dettMutuo.getInteressiPreammortamentoRimborso()) {
							d = d + dettMutuo.getInteressiPreammortamentoRimborso().doubleValue();
						}

						dettMutuo.setInteressiPreammortamentoRimborso((rsCP.wasNull() ? null : Double.valueOf(d)));       

					} else {

						System.out.println("Trattenuta - Interessi Preammortamento " + annoMutuo + "/" + progrMutuo + " : " + d);

						if (null != dettMutuo.getInteressiPreammortamentoTrattenuta()) {
							d = d + dettMutuo.getInteressiPreammortamentoTrattenuta().doubleValue();
						}

						dettMutuo.setInteressiPreammortamentoTrattenuta((rsCP.wasNull() ? null : Double.valueOf(d)));       

					}

				} else {

					if (rsCP.getString("cau_mov").equalsIgnoreCase("06") || rsCP.getString("cau_mov").equalsIgnoreCase("08")) {

						d = d * (-1);

						System.out.println("Rimborso - Interessi " + annoMutuo + "/" + progrMutuo + " : " + d);

						if (null != dettMutuo.getInteressiRimborso()) {
							d = d + dettMutuo.getInteressiRimborso().doubleValue();
						}

						dettMutuo.setInteressiRimborso((rsCP.wasNull() ? null : Double.valueOf(d)));       

					} else {

						System.out.println("Trattenuta - Interessi " + annoMutuo + "/" + progrMutuo + " : " + d);

						if (null != dettMutuo.getInteressiTrattenuta()) {
							d = d + dettMutuo.getInteressiTrattenuta().doubleValue();
						}

						dettMutuo.setInteressiTrattenuta((rsCP.wasNull() ? null : Double.valueOf(d)));       

					}

				}

			}

			dettMutuo.setAnnoMutuo(annoMutuo);
			dettMutuo.setProgressivoMutuo(progrMutuo);
			
			// chiude tutto
			psCP.close();
			rsCP.close();

			
			return dettMutuo;
		}			
		catch(SQLException e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}
	
	}
	
	
	
	public static int anno(Date Data)	{
		Calendar calendario = Calendar.getInstance();
		calendario.setTime(Data);
		int anno = calendario.get(1);
		return anno;
	}

	public static int mese(Date Data) {
		int mese = 0;
		Calendar calendario = Calendar.getInstance();
		calendario.setTime(Data);
		mese = calendario.get(2) + 1;
		return mese;
	}	

	public static int giorno(Date data) {
		Calendar calendario = Calendar.getInstance();
		calendario.setTime(data);
		int giorno = calendario.get(5);
		return giorno;
	}	

}
