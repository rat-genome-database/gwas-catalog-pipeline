package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.GWASCatalogDAO;
import edu.mcw.rgd.dao.impl.RGDManagementDAO;
import edu.mcw.rgd.dao.impl.XdbIdDAO;
import edu.mcw.rgd.dao.impl.variants.VariantDAO;
import edu.mcw.rgd.dao.spring.variants.VariantMapQuery;
import edu.mcw.rgd.dao.spring.variants.VariantSampleQuery;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.process.FastaParser;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DAO {
    private GWASCatalogDAO dao = new GWASCatalogDAO();
    VariantDAO vdao = new VariantDAO();
    private RGDManagementDAO managementDAO = new RGDManagementDAO();
    private XdbIdDAO xdao = new XdbIdDAO();
    private int xdbKey = XdbId.XDB_KEY_GWAS;

    public int getXdbKey() {
        return xdbKey;
    }

    public List<GWASCatalog> getFullCatalog() throws Exception{
        return dao.getFullCatalog();
    }

    public List<XdbId> getGwasXdbs(int rgdId) throws Exception {
        return xdao.getXdbIdsByRgdId(getXdbKey(),rgdId);
    }

    public int insertGWASBatch(Collection<GWASCatalog> incoming) throws Exception{
        return dao.insertGWASBatch(incoming);
    }

    public void deleteGWASBatch(Collection<GWASCatalog> deleting) throws Exception{
        dao.deleteGWASBatch(deleting);
    }

    public int updateGWASBatch(Collection<GWASCatalog> update) throws Exception{
        return dao.updateGWASBatch(update);
    }

    public List<GWASCatalog> getGWASbyRsId(String rsId) throws Exception{
        return dao.getGWASListByRsId(rsId);
    }

    public List<VariantMapData> getVariantsByRsId(String rsId) throws Exception{
        String sql = "SELECT * FROM variant v inner join variant_map_data vmd on v.rgd_id=vmd.rgd_id where v.rs_id=? and vmd.map_key=38";
        VariantMapQuery q = new VariantMapQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        return q.execute(rsId);
    }

    public List<VariantMapData> getActiveVariantsByRsId(String rsId) throws Exception{
        String sql = "SELECT * FROM variant v, variant_map_data vmd, RGD_IDS r where v.rgd_id=vmd.rgd_id and v.rs_id=? and vmd.map_key=38 and r.rgd_id=v.rgd_id and r.OBJECT_STATUS='ACTIVE'";
        VariantMapQuery q = new VariantMapQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        return q.execute(rsId);
    }

    String getRefAllele(int mapKey, GWASCatalog gc) throws Exception {

        FastaParser parser = new FastaParser();
        parser.setMapKey(mapKey);
        if( parser.getLastError()!=null ) {

        }

        parser.setChr(Utils.defaultString(gc.getChr()));

        int startPos = Integer.parseInt(Utils.defaultString(gc.getPos()));
        int stopPos = Integer.parseInt(Utils.defaultString(gc.getPos()));

        String fasta = parser.getSequence(startPos, stopPos);
        if( parser.getLastError()!=null ) {

        }
        if( fasta == null ) {
            return null;
        }

        return fasta;
    }

    public List<VariantMapData> getVariants(GWASCatalog g) throws Exception{
        String sql = "SELECT * FROM variant v inner join variant_map_data vmd on v.rgd_id=vmd.rgd_id where vmd.map_key=? and vmd.chromosome=? and vmd.start_pos=?";
        VariantMapQuery q = new VariantMapQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(38, g.getChr(), g.getPos());
    }

    public List<VariantMapData> getVariantExt(GWASCatalog g) throws Exception{
        String sql = "SELECT * FROM variant_ext v inner join variant_map_data vmd on v.rgd_id=vmd.rgd_id where vmd.map_key=? and vmd.chromosome=? and vmd.start_pos=?";
        VariantMapQuery q = new VariantMapQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(38, g.getChr(), g.getPos());
    }

    public List<VariantMapData> getAllVariants(GWASCatalog g) throws Exception{
        String sql = """
                select * from (
                SELECT v.rgd_id,vm.END_POS,vm.START_POS,v.var_nuc, v.ref_nuc
                FROM variant v, variant_map_data vm where v.rgd_id=vm.rgd_id and vm.map_key = ? and vm.chromosome = ? and vm.start_pos=?
                UNION ALL
                SELECT v.rgd_id,vmd.END_POS,vmd.START_POS,v.var_nuc, v.ref_nuc
                FROM variant_ext v, variant_map_data vmd where v.rgd_id=vmd.rgd_id and vmd.map_key = ? and vmd.chromosome = ? and  vmd.start_pos=?
                )""";
        VariantMapQuery q = new VariantMapQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(38, g.getChr(), g.getPos(), 38, g.getChr(), g.getPos());
    }

    public List<VariantMapData> getAllActiveVariantsByRsId(String rsId) throws Exception{
        return vdao.getAllActiveVariantsByRsId(rsId);
    }

    public void insertVariants(List<VariantMapData> mapsData, Logger log)  throws Exception{
        int total = vdao.insertVariantRgdIds(mapsData);
        log.info("\t\t\t\t\tAffected rows: " + total);
        vdao.insertVariants(mapsData);
    }

    public void insertVariantExt(List<VariantMapData> mapsData, Logger log)  throws Exception{
        int total = vdao.insertVariantRgdIds(mapsData);
        log.info("\t\t\t\t\tAffected rows: " + total);
        vdao.insertVariantExt(mapsData);
    }

    public void insertVariantMapData(List<VariantMapData> mapsData)  throws Exception{
        vdao.insertVariantMapData(mapsData);
    }

    public int insertVariantSample(List<VariantSampleDetail> sampleData) throws Exception {
        return vdao.insertVariantSample(sampleData);
    }

    public void updateVariant(List<VariantMapData> mapsData) throws Exception {
        BatchSqlUpdate sql2 = new BatchSqlUpdate(this.getVariantDataSource(),
                "update variant set RS_ID=? where RGD_ID=?",
                new int[]{Types.VARCHAR,Types.INTEGER}, 10000);
        sql2.compile();
        for( VariantMapData v: mapsData) {
            long id = v.getId();
            sql2.update(v.getRsId(),id);
        }
        sql2.flush();
    }

    public List<VariantSampleDetail> getVariantSampleDetail(int rgdId, int sampleId) throws Exception{
        String sql = "SELECT * FROM variant_sample_detail  WHERE rgd_id=? AND sample_id=?";
        VariantSampleQuery q = new VariantSampleQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(rgdId, sampleId);
    }

    public void updateVariantMapData(List<VariantMapData> mapsData) throws Exception {
        BatchSqlUpdate sql2 = new BatchSqlUpdate(this.getVariantDataSource(),
                "update variant_map_data set GENIC_STATUS=? where RGD_ID=?",
                new int[]{Types.VARCHAR,Types.INTEGER}, 10000);
        sql2.compile();
        for( VariantMapData v: mapsData) {
            long id = v.getId();
            sql2.update(v.getGenicStatus(),id);
        }
        sql2.flush();
    }

    public DataSource getVariantDataSource() throws Exception{
        return DataSourceFactory.getInstance().getCarpeNovoDataSource();
    }

    public RgdId createRgdId(int objectKey, String objectStatus, String notes, int mapKey) throws Exception{
        int speciesKey= SpeciesType.getSpeciesTypeKeyForMap(mapKey);
        return managementDAO.createRgdId(objectKey, objectStatus, notes, speciesKey);
    }

    public int insertGwasXdbs(List<XdbId> xdbs) throws Exception{
        return xdao.insertXdbs(xdbs);
    }
    public int withdrawVariants(Collection<GWASCatalog> tobeWithdrawn) throws Exception{
        RGDManagementDAO mdao = new RGDManagementDAO();
        List<Integer> ids = new ArrayList<>();
        for (GWASCatalog g : tobeWithdrawn){

            if (Utils.isStringEmpty(g.getChr()) || Utils.isStringEmpty(g.getPos()) || Utils.isStringEmpty(g.getSnps()) )
                continue;
//            if (g.getStrongSnpRiskallele()!=null && !g.getStrongSnpRiskallele().contains("?"))
//                continue;

            List<VariantMapData> vars = getAllVariants(g);
            String ref = getRefAllele(38, g);
            for (VariantMapData vmd : vars){

                if (Utils.stringsAreEqual(vmd.getVariantNucleotide(), g.getStrongSnpRiskallele()) &&
                        Utils.stringsAreEqual(vmd.getReferenceNucleotide(), ref)) {
                    RgdId id = new RgdId((int) vmd.getId());
                    mdao.withdraw(id);
                    break;
                }
            }
        }
        return 1;
    }

    public int removeSampleDetailConnectionToVariant(Collection<GWASCatalog> tobeWithdrawn) throws Exception{
        RGDManagementDAO mdao = new RGDManagementDAO();
        List<Integer> ids = new ArrayList<>();
        for (GWASCatalog g : tobeWithdrawn){

            if (Utils.isStringEmpty(g.getChr()) || Utils.isStringEmpty(g.getPos()) || Utils.isStringEmpty(g.getSnps()) )
                continue;
//            if (g.getStrongSnpRiskallele()!=null && !g.getStrongSnpRiskallele().contains("?"))
//                continue;

            List<VariantMapData> vars = getAllVariants(g);
            String ref = getRefAllele(38, g);
            for (VariantMapData vmd : vars){

                if (Utils.stringsAreEqual(vmd.getVariantNucleotide(), g.getStrongSnpRiskallele()) &&
                        Utils.stringsAreEqual(vmd.getReferenceNucleotide(), ref)) {
                    // delete sample detail
                    ids.add((int)vmd.getId());
                    break;
                }
            }
        }

        return deleteSampleDetails(ids);
    }

    public int withdrawQTLs(Collection<GWASCatalog> withdraw) throws Exception{
        RGDManagementDAO mdao = new RGDManagementDAO();
        for (GWASCatalog g : withdraw){
            if (g.getQtlRgdId()== null || g.getQtlRgdId()==0){
                continue;
            }
            RgdId id = new RgdId(g.getQtlRgdId());
            mdao.withdraw(id);
        }
        return 1;
    }
    public void withdrawVariants(Collection<Long> tobeWithdrawn, Logger logger) throws Exception{
        RGDManagementDAO mdao = new RGDManagementDAO();
        for (Long rgdId : tobeWithdrawn){
            RgdId id = new RgdId(rgdId.intValue());
            logger.debug("RGD ID being withdrawn: "+id.getRgdId());
            mdao.withdraw(id);
        }
    }

    public List<Long> getGWASRgdIds() throws Exception{
        String sql = "select rgd_id from rgd_ids where notes like '%GWAS%' and object_status='ACTIVE'";
        List<Long> rgdIds = new ArrayList<>();
        Connection con = DataSourceFactory.getInstance().getDataSource().getConnection();
        PreparedStatement ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            rgdIds.add( rs.getLong(1) );
        }
        con.close();
        return rgdIds;
    }

    public List<String> getGWASrsIds() throws Exception{
        String sql = "select distinct(snps) from gwas_catalog";
        List<String> rsIds = new ArrayList<>();
        Connection con = DataSourceFactory.getInstance().getDataSource().getConnection();
        PreparedStatement ps = con.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while( rs.next() ) {
            rsIds.add( rs.getString(1) );
        }
        con.close();
        return rsIds;
    }

    public List<VariantMapData> getVariantsbyRgdId(int rgdId) throws Exception{
        return vdao.getVariantsByRgdId(rgdId);
    }

    public int deleteSampleDetails(List<Integer> ids) throws Exception{
        return vdao.deleteSampleDetailByRgdIdAndSampleId(ids, 3);
    }
}
