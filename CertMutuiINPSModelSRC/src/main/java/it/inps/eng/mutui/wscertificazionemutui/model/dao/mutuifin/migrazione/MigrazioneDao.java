package it.inps.eng.mutui.wscertificazionemutui.model.dao.mutuifin.migrazione;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;

import it.inps.eng.mutui.wscertificazionemutui.model.dao.GenericDao;
import it.inps.eng.mutui.wscertificazionemutui.model.dao.exceptions.DaoException;
import it.inps.eng.wscertificazionemutui.common.beans.Lettera;
import it.inps.eng.wscertificazionemutui.common.beans.migrazione.ReportObject;

public class MigrazioneDao extends GenericDao{

	public MigrazioneDao(Connection mutuifinConn) {

		super(mutuifinConn);
	}
	
	//METODO CHE INSERISCE IL PDF DELLE LETTERE: L002
		public int insertPdfIntoLettereFile(ReportObject pdfObj , byte[] source, String tipoLettera, String anno, String progMutuo, String matricola) throws DaoException {
			String[] returnId = { "ID" };
			int res = 0;

			try {

				ByteArrayInputStream bis = null;

				if(source!=null)
					bis = new ByteArrayInputStream(source);

				String query = "INSERT INTO LETTERE_FILE(ANNO_MUTUO,PROG_MUTUO,MTR,COD_LET,DT_ORA_INSER,FILE_CONTENT) ";
				query       += "VALUES(?,?,?,?,?,?)";

				PreparedStatement pstmt = mutuifinConn.prepareStatement(query, returnId);
				
				//EXPRIVIA MOD
				
				pstmt.setString(1, anno);
				pstmt.setString(2, progMutuo);
				pstmt.setString(3, matricola);
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
}
