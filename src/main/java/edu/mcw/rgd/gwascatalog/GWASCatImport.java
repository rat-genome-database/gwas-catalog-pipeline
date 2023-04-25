package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.datamodel.GWASCatalog;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.XdbId;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

public class GWASCatImport {
    private String version;
    private String gwasFile;
    private DAO dao = new DAO();

    protected Logger logger = LogManager.getLogger("status");
    protected Logger inserted = LogManager.getLogger("inserted");
    protected Logger deleted = LogManager.getLogger("deleted");
    protected Logger xdbLog = LogManager.getLogger("xdbSummary");
    protected Logger varLog = LogManager.getLogger("variants");

    void run() throws Exception
    {
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String myFile = isNewFile();
        Parser parser = new Parser();

        if (myFile != null) {
        logger.info(getVersion());
        xdbLog.debug(getVersion());
        varLog.debug(getVersion());
        long pipeStart = System.currentTimeMillis();
        logger.info("   Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");

            try {
                ArrayList<GWASCatalog> incoming = parser.parse(myFile);
                logger.info("- - Total objects coming in: " + incoming.size());
                // send incoming to method and check with DB
                insertDeleteData(incoming);
            } catch (Exception e) {
                logger.info(e);
            }
            logger.info("   Total GWAS Catalog pipeline runtime -- elapsed time: "+
                    Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));
        }

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
            dao.withdrawVariants(deleteMe);
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
        List<VariantMapData> updateVar = new ArrayList<>();
        List<VariantMapData> updateVmd = new ArrayList<>();
        List<VariantMapData> insert = new ArrayList<>();
        List<VariantSampleDetail> newDetails = new ArrayList<>();
        List<XdbId> newXdbs = new ArrayList<>();
        HashMap<XdbId,Boolean> studies = new HashMap<>();

        for (GWASCatalog g : newVariants){
            boolean found = false;
            if (Utils.isStringEmpty(g.getChr()) || Utils.isStringEmpty(g.getPos()) || Utils.isStringEmpty(g.getSnps()) )
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
                        boolean diffGenic = false;
                        // if exists, update rs is and add sample detail if that does not exist
                        if (Utils.stringsAreEqual(vmd.getVariantNucleotide(), riskAllele ) &&
                                Utils.stringsAreEqual(vmd.getReferenceNucleotide(), ref)) {

                            String genicStat = isGenic(38,vmd.getChromosome(),(int)vmd.getStartPos() ) ? "GENIC":"INTERGENIC";
                            if (!Utils.stringsAreEqual(genicStat,vmd.getGenicStatus())) {
                                varLog.debug("Old genic status: "+vmd.getGenicStatus()+"|New Genic Status: " + genicStat);
                                vmd.setGenicStatus(genicStat);
                                updateVmd.add(vmd);
                            }
                            String rsId = g.getSnps().trim();
                            if (!Utils.stringsAreEqual(rsId,vmd.getRsId())) {
                                if (vmd.getRsId()==null) {
                                    vmd.setRsId(g.getSnps());
                                    updateVar.add(vmd);
                                }
                                else {
                                    varLog.debug("Old rsId: " + vmd.getRsId() + "|New rsId: " + rsId);
                                }
                            }

                            List<XdbId> xdbs = dao.getGwasXdbs((int) vmd.getId());
                            if (xdbs.isEmpty()){
                                XdbId x = createXdb(g, vmd);
                                if ( studies.get(x) == null || !studies.get(x)) {
                                    xdbLog.debug("New Xdb" + x.dump("|"));
                                    studies.put(x, true);
                                    newXdbs.add(x);
                                }
                            }
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        VariantMapData vmd = createMapData(g,ref);
                        insert.add(vmd);
                        XdbId x = createXdb(g, vmd);
                        xdbLog.debug("New Xdb" + x.dump("|"));
                        newXdbs.add(x);
                    }
                } // end check if there are variants
                else {
                    //else create new variant map data and and sample detail
                    VariantMapData vmd = createMapData(g,ref);
                    insert.add(vmd);
                    XdbId x = createXdb(g, vmd);
                    xdbLog.debug("New Xdb" + x.dump("|"));
                    newXdbs.add(x);
                }

            } // created/update variants

        } // end GWAS for

        if (!updateVar.isEmpty()){
            logger.info("       Variants being updated: "+updateVar.size());
            dao.updateVariant(updateVar);
        }
        if (!updateVmd.isEmpty()){
            logger.info("       Variant Genic Status being updated: "+updateVmd.size());
            dao.updateVariantMapData(updateVmd);
        }
        if (!insert.isEmpty()){
            logger.info("       Variants being added: "+insert.size());
            List<VariantMapData> newInsert = removeDuplicates(insert, newDetails);
            dao.insertVariants(newInsert);
            dao.insertVariantMapData(newInsert);
        }
        if (!newDetails.isEmpty()){
            logger.info("       New Sample Details: "+newDetails.size());
            dao.insertVariantSample(newDetails);
        }
        if (!newXdbs.isEmpty()){
            logger.info("       New XdbIds being added: "+newXdbs.size());
            dao.insertGwasXdbs(newXdbs);
        }
    }

    VariantMapData createMapData(GWASCatalog g, String ref) throws Exception{
        VariantMapData vmd = new VariantMapData();
        int speciesKey= SpeciesType.getSpeciesTypeKeyForMap(38);
//        RgdId r = dao.createRgdId(RgdId.OBJECT_KEY_VARIANTS, "ACTIVE", "created by GWAS Catalog pipeline", 38);
//        vmd.setId(r.getRgdId());
        vmd.setRsId(g.getSnps());
        vmd.setSpeciesTypeKey(speciesKey);
        String varType = "snv";
        String riskAllele = g.getStrongSnpRiskallele().replaceAll("\\s+","" );
        vmd.setVariantType(varType);
        vmd.setChromosome(g.getChr());
        vmd.setGenicStatus( isGenic(38,g.getChr(),Integer.parseInt(g.getPos())) ? "GENIC":"INTERGENIC" );
        vmd.setStartPos( Integer.parseInt(g.getPos()) );
        vmd.setReferenceNucleotide(ref);
        vmd.setVariantNucleotide(riskAllele);
        vmd.setEndPos(Integer.parseInt(g.getPos())+1);
        vmd.setMapKey(38);
        return vmd;
    }

    public VariantSampleDetail createGwasVariantSampleDetail(VariantMapData vmd) throws Exception{
        VariantSampleDetail vsd = new VariantSampleDetail();
        vsd.setId(vmd.getId());
        vsd.setSampleId( 3 );
        vsd.setDepth(9);
        vsd.setVariantFrequency(1);
        return vsd;
    }

    XdbId createXdb(GWASCatalog g, VariantMapData vmd) throws Exception{
        XdbId x = new XdbId();
        x.setAccId(g.getStudyAcc());
        x.setLinkText(g.getStudyAcc());
        x.setRgdId((int)vmd.getId());
        Date date = new Date();
        x.setCreationDate(date);
        x.setModificationDate(date);
        x.setSrcPipeline("GWAS Catalog");
        x.setXdbKey(dao.getXdbKey());
        return x;
    }

    boolean isGenic(int mapKey, String chr, int pos) throws Exception {

        GeneCache geneCache = geneCacheMap.get(chr);
        if( geneCache==null ) {
            geneCache = new GeneCache();
            geneCacheMap.put(chr, geneCache);
            geneCache.loadCache(mapKey, chr, DataSourceFactory.getInstance().getDataSource());
        }
        List<Integer> geneRgdIds = geneCache.getGeneRgdIds(pos);
        return !geneRgdIds.isEmpty();
    }

    String isNewFile() throws Exception {
        File lastFile = getLastModified("data/");
        String myFile = downloadFile(gwasFile);
        Path path = Paths.get(myFile);
        Path path2 = Paths.get(lastFile.getPath());
        try {

            // size of a file (in bytes)
            long bytes = Files.size(path);
            long bytes2 = Files.size(path2);
//            System.out.println(String.format("%,d bytes", bytes));
//            System.out.println(String.format("%,d bytes", bytes2));
            if (Utils.longsAreEqual(bytes,bytes2))
                return null;
            else
                return myFile;

        } catch (Exception e) {
            logger.debug(e);
            return myFile;
        }

    }

    public static File getLastModified(String directoryFilePath)
    {
        File directory = new File(directoryFilePath);
        File[] files = directory.listFiles(File::isFile);
        long lastModifiedTime = Long.MIN_VALUE;
        File chosenFile = null;

        if (files != null)
        {
            for (File file : files)
            {
                if (file.lastModified() > lastModifiedTime)
                {
                    chosenFile = file;
                    lastModifiedTime = file.lastModified();
                }
            }
        }

        return chosenFile;
    }

    ArrayList<VariantMapData> removeDuplicates(List<VariantMapData> list, List<VariantSampleDetail> newDetails) throws Exception {
        ArrayList<VariantMapData> newList = new ArrayList<>();
        Set<VariantMapData> set = new HashSet<>(list);
        newList.addAll(set);
        for (VariantMapData vmd : newList) {
//            if (!newList.contains(vmd)) {
            RgdId r = dao.createRgdId(RgdId.OBJECT_KEY_VARIANTS, "ACTIVE", "created by GWAS Catalog pipeline", 38);
            vmd.setId(r.getRgdId());
            List<VariantSampleDetail> sampleDetailInRgd = dao.getVariantSampleDetail((int)vmd.getId(), 3);
            if (sampleDetailInRgd.isEmpty()) {
                newDetails.add(createGwasVariantSampleDetail(vmd));
            }
//            }
        }

        // return the new list
        return newList;
    }
    Map<String, GeneCache> geneCacheMap = new HashMap<>();

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
