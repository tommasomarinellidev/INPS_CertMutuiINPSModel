package it.inps.eng.mutui.wscertificazionemutui.model.dao.mutuifin.lettere;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import it.inps.eng.mutui.wscertificazionemutui.model.dao.GenericDao;
import it.inps.eng.mutui.wscertificazionemutui.model.dao.exceptions.DaoException;
import it.inps.eng.wscertificazionemutui.common.beans.certificazionefiscale.CertFisBean;



public class LettereDao  extends GenericDao{



	public LettereDao(Connection mutuifinConn) {

		super(mutuifinConn);
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



	public void writeRecordLettera(CertFisBean certFisBean, String pathNomeFile, String annoCert, String progCert, 
			String matrOperatore, String codSede, int idCertFis, Timestamp dataEsecuzione) throws DaoException {


		try {
			String sql = "insert into lettere(anno_mutuo,prog_mutuo,mtr,dt_ora_inser,cod_let,st_let,sede,path_nome_file,dt_ul_agg,mtr_ul_agg, anno_cert, prog_cert, dt_stampa, ID_CERT_FIS) values ";
			sql+= " (                       ?    ,    ?     , ? ,      ?     ,   ?   ,  ?   ,  ? ,       ?      ,    ?    ,    ?     ,      ?   ,     ?    ,    ?, ?)";

			PreparedStatement ps = mutuifinConn.prepareStatement(sql);

			ps.setString(1, certFisBean.getRigo().get(certFisBean.getRigo().size()-1).getMutuoDaCertificare().getAnnoMutuo());
			ps.setString(2, certFisBean.getRigo().get(certFisBean.getRigo().size()-1).getMutuoDaCertificare().getProgMutuo());
			ps.setString(3, certFisBean.getBenefPrincipale().getMatricola());
			ps.setTimestamp(4, dataEsecuzione);
			ps.setString(5, certFisBean.getTipoCertif());
			ps.setString(6, "00");
			ps.setString(7, codSede);
			ps.setString(8, pathNomeFile);
			ps.setTimestamp(9, new Timestamp(System.currentTimeMillis()));
			ps.setString(10, matrOperatore);
			ps.setString(11, annoCert);
			ps.setString(12, progCert);			
			ps.setDate(13, null);
			ps.setInt(14, idCertFis);

			ps.executeUpdate();

			ps.close();
			//		mutuifinConn.setAutoCommit(true);
		}
		catch(Exception e) {
			throw new DaoException(e);
		}
		finally {
			//con.close();
		}	
	}

}
