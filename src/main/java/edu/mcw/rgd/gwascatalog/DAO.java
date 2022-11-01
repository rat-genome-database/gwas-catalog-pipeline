package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.GWASCatalogDAO;
import edu.mcw.rgd.dao.impl.RGDManagementDAO;
import edu.mcw.rgd.dao.impl.VariantDAO;
import edu.mcw.rgd.dao.impl.XdbIdDAO;
import edu.mcw.rgd.dao.spring.variants.VariantMapQuery;
import edu.mcw.rgd.dao.spring.variants.VariantSampleQuery;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.process.FastaParser;
import edu.mcw.rgd.process.Utils;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

import javax.sql.DataSource;
import java.sql.Types;
import java.util.Collection;
import java.util.List;

public class DAO {
    private GWASCatalogDAO dao = new GWASCatalogDAO();
    private VariantDAO vdao = new VariantDAO();
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

    public void setDataSource() throws Exception{
        vdao.setDataSource(DataSourceFactory.getInstance().getCarpeNovoDataSource());
    }

    public List<Variant> getVariants(int sampleId, String chr, long start, long stop)throws Exception{
        return vdao.getVariants(sampleId, chr, start, stop);
    }

    public List<VariantMapData> getVariantsByRsId(String rsId) throws Exception{
        String sql = "SELECT * FROM variant v inner join variant_map_data vmd on v.rgd_id=vmd.rgd_id where v.rs_id=? and vmd.map_key=38";
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

    public void insertVariants(List<VariantMapData> mapsData)  throws Exception{
        BatchSqlUpdate sql1 = new BatchSqlUpdate(this.getVariantDataSource(),
                "INSERT INTO variant (\n" +
                        " RGD_ID,REF_NUC, VARIANT_TYPE, VAR_NUC, RS_ID, CLINVAR_ID, SPECIES_TYPE_KEY)\n" +
                        "VALUES (\n" +
                        "  ?,?,?,?,?,?,?)",
                new int[]{Types.INTEGER,Types.VARCHAR,Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.INTEGER}, 10000);
        sql1.compile();
        for( VariantMapData v: mapsData) {
            long id = v.getId();
            sql1.update(id, v.getReferenceNucleotide(), v.getVariantType(), v.getVariantNucleotide(), v.getRsId(), v.getClinvarId(), v.getSpeciesTypeKey());

        }
        sql1.flush();
    }
    public void insertVariantMapData(List<VariantMapData> mapsData)  throws Exception{
        BatchSqlUpdate sql2 = new BatchSqlUpdate(this.getVariantDataSource(),
                "INSERT INTO variant_map_data (\n" +
                        " RGD_ID,CHROMOSOME,START_POS,END_POS,PADDING_BASE,GENIC_STATUS,MAP_KEY)\n" +
                        "VALUES (\n" +
                        " ?,?,?,?,?,?,?)",
                new int[]{Types.INTEGER,Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR,Types.VARCHAR, Types.INTEGER}, 10000);
        sql2.compile();
        for( VariantMapData v: mapsData) {
            long id = v.getId();
            sql2.update(id, v.getChromosome(), v.getStartPos(), v.getEndPos(), v.getPaddingBase(), v.getGenicStatus(), v.getMapKey());
        }
        sql2.flush();
    }

    public int insertVariantSample(List<VariantSampleDetail> sampleData) throws Exception {
        BatchSqlUpdate bsu= new BatchSqlUpdate(this.getVariantDataSource(),
                "INSERT INTO variant_sample_detail (\n" +
                        " RGD_ID,SOURCE,SAMPLE_ID,TOTAL_DEPTH,VAR_FREQ,ZYGOSITY_STATUS,ZYGOSITY_PERCENT_READ," +
                        "ZYGOSITY_POSS_ERROR,ZYGOSITY_REF_ALLELE,ZYGOSITY_NUM_ALLELE,ZYGOSITY_IN_PSEUDO,QUALITY_SCORE)\n" +
                        "VALUES (?,?,?,?,?,?,?," +
                        "?,?,?,?,?)",
                new int[]{Types.INTEGER,Types.VARCHAR,Types.INTEGER, Types.INTEGER, Types.INTEGER,Types.VARCHAR, Types.INTEGER,
                        Types.VARCHAR,Types.VARCHAR, Types.INTEGER,Types.VARCHAR, Types.INTEGER}, 10000);
        bsu.compile();
        for(VariantSampleDetail v: sampleData ) {
            bsu.update(v.getId(), v.getSource(), v.getSampleId(),v.getDepth(),v.getVariantFrequency(),v.getZygosityStatus(),v.getZygosityPercentRead(),
                    v.getZygosityPossibleError(),v.getZygosityRefAllele(),v.getZygosityNumberAllele(),v.getZygosityInPseudo(),v.getQualityScore());
        }
        bsu.flush();
        // compute nr of rows affected
        int totalRowsAffected = 0;
        for( int rowsAffected: bsu.getRowsAffected() ) {
            totalRowsAffected += rowsAffected;
        }
        return totalRowsAffected;
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
        for (GWASCatalog g : tobeWithdrawn){
            List<VariantMapData> vars = getVariants(g);
            String ref = getRefAllele(38, g);
            for (VariantMapData vmd : vars){
                if (Utils.stringsAreEqual(vmd.getVariantNucleotide(), g.getStrongSnpRiskallele() ) &&
                        Utils.stringsAreEqual(vmd.getReferenceNucleotide(), ref)) {
                    RgdId id = new RgdId((int) vmd.getId());
//                    mdao.withdraw(id);
                    break;
                }
            }
        }
        return 1;
    }
}
