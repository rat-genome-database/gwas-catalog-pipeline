package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.datamodel.GWASCatalog;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class GWASCatImport {
    private String version;
    private String gwasFile;
    private DAO dao = new DAO();

    protected Logger logger = LogManager.getLogger("status");
    protected Logger inserted = LogManager.getLogger("inserted");
    protected Logger deleted = LogManager.getLogger("deleted");

    void run() throws Exception
    {
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logger.info(getVersion());
        long pipeStart = System.currentTimeMillis();
        logger.info("   Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");
        String myFile = downloadFile(gwasFile);
        Parser parser = new Parser();

        try {
            ArrayList<GWASCatalog> incoming = parser.parse(myFile);
            logger.info("- - Total objects coming in: "+incoming.size());
            // send incoming to method and check with DB
            insertDeleteData(incoming);
        }
        catch (Exception e){
            logger.info(e);
        }
        logger.info("   Total GWAS Catalog pipeline runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));
    }

    void insertDeleteData(ArrayList<GWASCatalog> incoming) throws Exception{
        List<GWASCatalog> inRgd = dao.getFullCatalog();
//        insertNewVariants(inRgd); // initial load
        Collection<GWASCatalog> inserting = CollectionUtils.subtract(incoming,inRgd);
        if (!inserting.isEmpty()){
            logger.info("- - Total objects inserted: " + inserting.size());
            logInsDel(inserted, inserting);
            insertNewVariants(inserting); // used after initial load for new variants
            dao.insertGWASBatch(inserting);
        }

        Collection<GWASCatalog> deleteMe = CollectionUtils.subtract(inRgd, incoming);
        if(!deleteMe.isEmpty()){
            logger.info("- - Total objects deleted: " + deleteMe.size());
            logInsDel(deleted, deleteMe);
            dao.deleteGWASBatch(deleteMe);
        }

        Collection<GWASCatalog> match = CollectionUtils.intersection(incoming,inRgd);
        if (!match.isEmpty()){
            logger.info("- - Total matching objects: " + match.size());
        }
    }

    String downloadFile(String file) throws Exception{
        FileDownloader downloader = new FileDownloader();
        downloader.setExternalFile(file);
        downloader.setLocalFile("data/associations_ontology_annot.tsv");
        downloader.setUseCompression(true);
        downloader.setPrependDateStamp(true);
        return downloader.downloadNew();
    }

    void logInsDel(Logger log, Collection<GWASCatalog> list) throws Exception{
        for (GWASCatalog gc : list){
            log.debug(gc.print());
        }
        return;
    }

    void insertNewVariants(Collection<GWASCatalog> newVariants) throws Exception{
        List<VariantMapData> update = new ArrayList<>();
        List<VariantMapData> insert = new ArrayList<>();
        List<VariantSampleDetail> newDetails = new ArrayList<>();

        for (GWASCatalog g : newVariants){
            boolean found = false;
            if (g.getChr()==null || g.getPos()==null || g.getSnps()==null)
                continue;
            if (g.getStrongSnpRiskallele()!=null && !g.getStrongSnpRiskallele().contains("?")){
                // check if in db, and compare.
                String ref = dao.getRefAllele(38, g);
                String riskAllele = g.getStrongSnpRiskallele().replaceAll("\\s+","" );
                if (Utils.stringsAreEqual(ref,riskAllele) || riskAllele.length()>1) // reference and var nucleotide are equal or large risk allele
                    continue;
                List<VariantMapData> data = dao.getVariants(g);
                if (!data.isEmpty()) {
                    for (VariantMapData vmd : data) {
                        // if exists, update rs is and add sample detail if that does not exist
                        if (Utils.stringsAreEqual(vmd.getVariantNucleotide(), riskAllele ) &&
                                Utils.stringsAreEqual(vmd.getReferenceNucleotide(), ref)) {
                            vmd.setRsId(g.getSnps());
                            update.add(vmd);
                            List<VariantSampleDetail> sampleDetailInRgd = dao.getVariantSampleDetail((int)vmd.getId(), 3);
                            if (sampleDetailInRgd.isEmpty()) {
                                newDetails.add(createGwasVariantSampleDetail(g,vmd));
                            }
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        VariantMapData vmd = createMapData(g,ref);
                        VariantSampleDetail vsd = createGwasVariantSampleDetail(g,vmd);
                        insert.add(vmd);
                        newDetails.add(vsd);
                    }
                } // end check if there are variants
                else {
                    //else create new variant map data and and sample detail
                    VariantMapData vmd = createMapData(g,ref);
                    VariantSampleDetail vsd = createGwasVariantSampleDetail(g,vmd);
                    insert.add(vmd);
                    newDetails.add(vsd);
                }

            } // created/update variants

        } // end GWAS for

        if (!update.isEmpty()){
            logger.info("Variants being updated: "+update.size());
            dao.updateVariantMapData(update);
        }
        if (!insert.isEmpty()){
            logger.info("Variants being added: "+insert.size());
            dao.insertVariants(insert);
            dao.insertVariantMapData(insert);
        }
        if (!newDetails.isEmpty()){
            logger.info("New Sample Details: "+newDetails.size());
            dao.insertVariantSample(newDetails);
        }
    }

    VariantMapData createMapData(GWASCatalog g, String ref) throws Exception{
        VariantMapData vmd = new VariantMapData();
        int speciesKey= SpeciesType.getSpeciesTypeKeyForMap(38);
        RgdId r = dao.createRgdId(RgdId.OBJECT_KEY_VARIANTS, "ACTIVE", "created by GWAS Catalog pipeline", 38);
        vmd.setId(r.getRgdId());
        vmd.setRsId(g.getSnps());
        vmd.setSpeciesTypeKey(speciesKey);
        String varType = "snv";
        String riskAllele = g.getStrongSnpRiskallele().replaceAll("\\s+","" );
        vmd.setVariantType(varType);
        vmd.setChromosome(g.getChr());
        vmd.setStartPos( Integer.parseInt(g.getPos()) );
        vmd.setReferenceNucleotide(ref);
        vmd.setVariantNucleotide(riskAllele);
        vmd.setEndPos(Integer.parseInt(g.getPos())+1);
        vmd.setMapKey(38);
        return vmd;
    }

    public VariantSampleDetail createGwasVariantSampleDetail(GWASCatalog g, VariantMapData vmd) throws Exception{
        VariantSampleDetail vsd = new VariantSampleDetail();
        vsd.setId(vmd.getId());
        vsd.setSampleId( 3 );
        vsd.setDepth(9);
        vsd.setVariantFrequency(1);
        return vsd;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setGwasFile(String gwasFile) {
        this.gwasFile = gwasFile;
    }

    public String getGwasFile() {
        return gwasFile;
    }
}
