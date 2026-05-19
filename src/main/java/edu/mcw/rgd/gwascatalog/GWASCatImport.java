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
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class GWASCatImport {
    private String version;
    private String gwasFile;
    private String deleteThreshold;
    private DAO dao = new DAO();

    protected Logger logger = LogManager.getLogger("status");
    protected Logger efoStatus = LogManager.getLogger("efoStatus");
    protected Logger inserted = LogManager.getLogger("inserted");
    protected Logger deleted = LogManager.getLogger("deleted");
    protected Logger xdbLog = LogManager.getLogger("xdbSummary");
    protected Logger varLog = LogManager.getLogger("variants");

    void run() throws Exception
    {
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String myFile = isNewFile(); //downloadZipFile(gwasFile);
        Parser parser = new Parser();

        if (myFile != null) {
        logger.info(getVersion());
        xdbLog.debug(getVersion());
        varLog.debug(getVersion());
        long pipeStart = System.currentTimeMillis();
        logger.info("   Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");

            try {
                ArrayList<GWASCatalog> incoming = parser.processZip(myFile, logger);
                logger.info("- - Total objects coming in: " + incoming.size());
                // send incoming to method and check with DB
                insertDeleteData(incoming);
            } catch (Exception e) {
                Utils.printStackTrace(e, LogManager.getLogger("status"));
            }
            logger.info("   Total GWAS Catalog pipeline runtime -- elapsed time: "+
                    Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));
        }

    }

    void insertDeleteData(ArrayList<GWASCatalog> incoming) throws Exception {
        List<GWASCatalog> inRgd = dao.getGWASByMapKey(38);
//        insertNewVariants(inRgd); // initial load
        logEfoIdChanges(incoming, inRgd);
        Collection<GWASCatalog> inserting = CollectionUtils.subtract(incoming, inRgd);
        Collection<GWASCatalog> deleteMe = CollectionUtils.subtract(inRgd, incoming);
        if (deletionThresholdCheck(deleteThreshold, deleteMe.size(), inRgd.size())){
            boolean insertExt = true;
            if (!inserting.isEmpty()) {
                logger.info("- - Total objects inserted: " + inserting.size());
                logInsDel(inserted, inserting);
                insertNewVariants(inserting); // used after initial load for new variants
                dao.insertGWASBatch(inserting);
                insertExt = false;
            }


            if (!deleteMe.isEmpty()) {
                logger.info("- - Total objects deleted: " + deleteMe.size());
                logInsDel(deleted, deleteMe);
                dao.removeSampleDetailConnectionToVariant(deleteMe);
                dao.withdrawQTLs(deleteMe);
                dao.deleteGWASBatch(deleteMe);
            }

            Collection<GWASCatalog> match = CollectionUtils.intersection(incoming, inRgd);
            if (!match.isEmpty()) {
                logger.info("- - Total matching objects: " + match.size());
            }

            if (insertExt) {
                insertNewVariantExt(inRgd);
            }
        }
    }

    // Logs each unique old -> new EFO id transition where an incoming record matches an
    // existing record on every field used by GWASCatalog.equals() except efoId. Surfaces
    // which EFO ids changed so related tables can be updated; does not mutate either
    // collection.
    void logEfoIdChanges(Collection<GWASCatalog> incoming, Collection<GWASCatalog> inRgd) {
        Map<EfoMatchKey, GWASCatalog> inRgdByKey = new HashMap<>(inRgd.size() * 2);
        for (GWASCatalog gb : inRgd) {
            inRgdByKey.putIfAbsent(EfoMatchKey.of(gb), gb);
        }
        Set<String> loggedTransitions = new HashSet<>();
        for (GWASCatalog ga : incoming) {
            GWASCatalog gb = inRgdByKey.get(EfoMatchKey.of(ga));
            if (gb == null) continue;
            if (!Utils.stringsAreEqual(ga.getEfoId(), gb.getEfoId())) {
                String key = gb.getEfoId() + " -> " + ga.getEfoId();
                if (loggedTransitions.add(key)) {
                    efoStatus.info("EFO id change -- old: " + gb.getEfoId() + " | new: " + ga.getEfoId());
                }
            }
        }
    }

    // Hash key matching the equality semantics previously in equalsIgnoringEfoId:
    // null strings are normalized to "" (matches Utils.stringsAreEqual), and pValMlog
    // is formatted to 10 fraction digits (matches Utils.doublesAreEqual(..., 10)).
    private record EfoMatchKey(
            String pmid, String diseaseTrait, String initialSample, String replicateSample,
            String region, String chr, String pos, String reportedGenes, String mappedGene,
            String strongSnpRiskallele, String snps, String curSnpId, String context,
            String riskAlleleFreq, String pValStr, String pValMlog10, String snpPassQc,
            String mapTrait, String studyAcc, String orBeta, int mapKey) {

        static EfoMatchKey of(GWASCatalog g) {
            return new EfoMatchKey(
                    n(g.getPmid()), n(g.getDiseaseTrait()), n(g.getInitialSample()), n(g.getReplicateSample()),
                    n(g.getRegion()), n(g.getChr()), n(g.getPos()), n(g.getReportedGenes()), n(g.getMappedGene()),
                    n(g.getStrongSnpRiskallele()), n(g.getSnps()), n(g.getCurSnpId()), n(g.getContext()),
                    n(g.getRiskAlleleFreq()), n(g.getpValStr()), formatMlog(g.getpValMlog()), n(g.getSnpPassQc()),
                    n(g.getMapTrait()), n(g.getStudyAcc()), n(g.getOrBeta()), g.getMapKey());
        }

        private static String n(String s) { return s == null ? "" : s; }

        private static String formatMlog(Double d) {
            if (d == null) d = 0.0;
            DecimalFormat df = new DecimalFormat();
            df.setMinimumFractionDigits(10);
            df.setMaximumFractionDigits(10);
            return df.format(d);
        }
    }

    void insertNewVariantExt(List<GWASCatalog> incoming) throws Exception {
        // go through what we have and insert ? and same as ref alleles
        ArrayList<GWASCatalog> insertExt = new ArrayList<>();
        for (GWASCatalog g : incoming){
            if (Utils.isStringEmpty(g.getChr()) || Utils.isStringEmpty(g.getPos()) || Utils.isStringEmpty(g.getSnps()) )
                continue;
            if (g.getStrongSnpRiskallele()==null)
                continue;
            try {
                if (g.getStrongSnpRiskallele().contains("?"))
                    insertExt.add(g);
                else {
                    String ref = dao.getRefAllele(38, g);
                    String riskAllele = g.getStrongSnpRiskallele().replaceAll("\\s+", "");
                    if (Utils.stringsAreEqual(ref, riskAllele))
                        insertExt.add(g);
                }
            }
            catch (Exception e){
                System.out.println("error");
            }
        }
        if (!insertExt.isEmpty()){
            logger.info("\t\tChecking all variants in VARIANT_EXT: "+insertExt.size());
            insertVariantsExt(insertExt);
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

    String downloadZipFile(String file) throws Exception {

        FileDownloader downloader = new FileDownloader();
        downloader.setExternalFile(file);
        downloader.setLocalFile("data/associations_ontology_annot.zip");
        downloader.setPrependDateStamp(true); // prefix downloaded files with the current date

        // starting Jan 2017, HttpClient from apache commons had problems downloading files via HTTPS;
        // therefore we switched to use native java URL object to download the file from PharmGKB
        // it worked!
        downloader.setDoNotUseHttpClient(true);

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
        List<GWASCatalog> varExt = new ArrayList<>();
        List<VariantSampleDetail> newDetails = new ArrayList<>();
        List<XdbId> newXdbs = new ArrayList<>();
        HashMap<XdbId,Boolean> studies = new HashMap<>();

        for (GWASCatalog g : newVariants){
            boolean found = false;
            if (Utils.isStringEmpty(g.getChr()) || Utils.isStringEmpty(g.getPos()) || Utils.isStringEmpty(g.getSnps()) )
                continue;
            if (g.getStrongSnpRiskallele()!=null && g.getStrongSnpRiskallele().contains("?")){
                varExt.add(g);
                continue;
            }
            if (g.getStrongSnpRiskallele()!=null){
                // check if in db, and compare.
                String ref = dao.getRefAllele(38, g);
                String riskAllele = g.getStrongSnpRiskallele().replaceAll("\\s+","" );
                if (Utils.stringsAreEqual(ref,riskAllele) || riskAllele.length()>1){
                    // reference and var nucleotide are equal or large risk allele
                    varExt.add(g);
                    continue;
                }

                List<VariantMapData> data = dao.getVariants(g);
                if (!data.isEmpty()) {
                    for (VariantMapData vmd : data) {
                        boolean diffGenic = false;
                        // if exists, update rs is and add sample detail if that does not exist
                        if (Utils.stringsAreEqual(vmd.getVariantNucleotide(), riskAllele ) &&
                                Utils.stringsAreEqual(vmd.getReferenceNucleotide(), ref)) {

                            String genicStat = dao.isGenic(38,vmd.getChromosome(),(int)vmd.getStartPos() ) ? "GENIC":"INTERGENIC";
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
                    }
                } // end check if there are variants
                else {
                    //else create new variant map data and and sample detail
                    VariantMapData vmd = createMapData(g,ref);
                    insert.add(vmd);
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
            List<VariantMapData> newInsert = removeDuplicates(insert, newDetails, newXdbs);
            dao.insertVariants(newInsert, logger);
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
        if (!varExt.isEmpty()){
            logger.info("\t\tVariants being checked in Variant Ext: " + varExt.size());
            insertVariantsExt(varExt);
        }
    }

    void insertVariantsExt(Collection<GWASCatalog> newVariants) throws Exception{
        List<VariantMapData> updateVar = new ArrayList<>();
        List<VariantMapData> updateVmd = new ArrayList<>();
        List<VariantMapData> insert = new ArrayList<>();
        List<VariantSampleDetail> newDetails = new ArrayList<>();
        HashMap<Long,VariantSampleDetail> detailHashMap = new HashMap<>();
        List<XdbId> newXdbs = new ArrayList<>();
        HashMap<XdbId,Boolean> studies = new HashMap<>();

        for (GWASCatalog g : newVariants){
            boolean found = false;
            if (Utils.isStringEmpty(g.getChr()) || Utils.isStringEmpty(g.getPos()) || Utils.isStringEmpty(g.getSnps()) )
                continue;
            // check if in db, and compare.
            String ref = dao.getRefAllele(38, g);
            String riskAllele = g.getStrongSnpRiskallele().replaceAll("\\s+","" );

            List<VariantMapData> data = dao.getVariantExt(g);
            if (!data.isEmpty()) {
                for (VariantMapData vmd : data) {
                    boolean diffGenic = false;
                    // if exists, update rs is and add sample detail if that does not exist
                    if (Utils.stringsAreEqual(vmd.getVariantNucleotide(), riskAllele ) &&
                            Utils.stringsAreEqual(vmd.getReferenceNucleotide(), ref)) {

                            String genicStat = dao.isGenic(38,vmd.getChromosome(),(int)vmd.getStartPos() ) ? "GENIC":"INTERGENIC";
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
                        if ( detailHashMap.get(vmd.getId()) == null ){
                            VariantSampleDetail vsd = createGwasVariantSampleDetail(vmd);
                            detailHashMap.put(vmd.getId(),vsd);
                        }
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    VariantMapData vmd = createMapData(g,ref);
                    insert.add(vmd);
                }
            } // end check if there are variants
            else {
                //else create new variant map data and and sample detail
                VariantMapData vmd = createMapData(g,ref);
                insert.add(vmd);
            }
                // created/update variants

        } // end GWAS for

        if (!updateVar.isEmpty()){
            logger.info("       Variant_Ext being updated in: "+updateVar.size());
            dao.updateVariant(updateVar);
        }
        if (!updateVmd.isEmpty()){
            logger.info("       Variant_Ext Genic Status being updated: "+updateVmd.size());
            dao.updateVariantMapData(updateVmd);
        }
        if (!insert.isEmpty()){
            logger.info("       Variant_Ext being added: "+insert.size());
            List<VariantMapData> newInsert = removeDuplicates(insert, newDetails, newXdbs);
            dao.insertVariantExt(newInsert, logger);
            dao.insertVariantMapData(newInsert);
        }
        if (!newDetails.isEmpty()){
            logger.info("       New Sample Details: "+newDetails.size());
            dao.insertVariantSample(newDetails);
        }
        if (!detailHashMap.isEmpty()){
            List<VariantSampleDetail> samples = new ArrayList<>();
            for (Long id : detailHashMap.keySet()){
                samples.add(detailHashMap.get(id));
            }
            logger.info("\t\tNew Sample Details for Variant_EXT: "+samples.size());
            dao.insertVariantSample(samples);
        }
        if (!newXdbs.isEmpty()){
            logger.info("       New XdbIds being added: "+newXdbs.size());
            dao.insertGwasXdbs(newXdbs);
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
        vmd.setGenicStatus( dao.isGenic(38,g.getChr(),Integer.parseInt(g.getPos())) ? "GENIC":"INTERGENIC" );
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

    String isNewFile() throws Exception {
        File lastFile = getLastModified("data/");
        String myFile = downloadZipFile(gwasFile);
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

    ArrayList<VariantMapData> removeDuplicates(List<VariantMapData> list, List<VariantSampleDetail> newDetails, List<XdbId> newXdbs) throws Exception {
        ArrayList<VariantMapData> newList = new ArrayList<>();
        Set<VariantMapData> set = new HashSet<>(list);
        newList.addAll(set);
        for (VariantMapData vmd : newList) {
            RgdId r = dao.createRgdId(RgdId.OBJECT_KEY_VARIANTS, "ACTIVE", "created by GWAS Catalog pipeline", 38);
            vmd.setId(r.getRgdId());
            List<VariantSampleDetail> sampleDetailInRgd = dao.getVariantSampleDetail((int)vmd.getId(), 3);
            if (sampleDetailInRgd.isEmpty()) {
                newDetails.add(createGwasVariantSampleDetail(vmd));
            }
            newXdbs.addAll(createXdbIds(vmd));
        }

        // return the new list
        return newList;
    }

    List<XdbId> createXdbIds(VariantMapData vmd) throws Exception{
        List<GWASCatalog> gwas = dao.getGWASbyRsId(vmd.getRsId());
        HashMap<XdbId,Boolean> studies = new HashMap<>();
        List<XdbId> xdbs = dao.getGwasXdbs((int)vmd.getId());
        List<XdbId> newXdbs = new ArrayList<>();
        for (GWASCatalog g : gwas) {
            if (xdbs.isEmpty()) {
                XdbId x = createXdb(g, vmd);
                if (studies.get(x) == null || !studies.get(x)) {
                    xdbLog.debug("New Xdb" + x.dump("|"));
                    studies.put(x, true);
                    newXdbs.add(x);
                }
            }
            else {
                boolean exist = false;
                for (XdbId xdb : xdbs){
                    if (xdb.getAccId().equals(g.getStudyAcc())){
                        exist = true;
                        break;
                    }
                }
                if (!exist){
                    XdbId x = createXdb(g, vmd);
                    xdbLog.debug("New Xdb" + x.dump("|"));
                    newXdbs.add(x);
                }
            }
        }
        return newXdbs;
    }

    boolean deletionThresholdCheck(String thresholdStr, int deletionCnt, int inRgdCnt) throws Exception{
        int threshold = Integer.parseInt(thresholdStr.substring(0, thresholdStr.length()-1));
        int deleteLimit = (threshold * inRgdCnt) / 100;
        if (deletionCnt > deleteLimit) {
            logger.warn("*** INSERT/DELETE of GWAS Data aborted! *** "+thresholdStr+" delete threshold exceeded!");
            logger.warn("*** Amount that is in RGD: " + inRgdCnt);
            logger.warn("*** Amount that would have been deleted: " + deletionCnt);
            return false;
        }
        return true;
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

    public void setDeleteThreshold(String deleteThreshold) {
        this.deleteThreshold = deleteThreshold;
    }

    public String getDeleteThreshold(){
        return deleteThreshold;
    }
}
