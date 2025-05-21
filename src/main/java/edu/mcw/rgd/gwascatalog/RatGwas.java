package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.datamodel.GWASCatalog;
import edu.mcw.rgd.datamodel.GWASVersion;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSSId;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.process.Utils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class RatGwas {

    private String version;
    private Map<Integer, String> ratFiles;
    private int sampleId;
    private String cmoVtTermFile;
    protected Logger logger = LogManager.getLogger("ratStatus");
    protected Logger varLog = LogManager.getLogger("variants");
    private SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private DAO dao = new DAO();


    public void run(){
        logger.info(getVersion());
        logger.info("   "+dao.getConnection());

        long pipeStart = System.currentTimeMillis();
        logger.info("Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");

        try{
            for (Integer mapKey : ratFiles.keySet()){
                logger.info("Parsing file: "+ ratFiles.get(mapKey));
                insertIntoCatalog(mapKey);
                logger.info("-----------------------------------------");
            }
        }catch (Exception e){
            Utils.printStackTrace(e,logger);
        }

        logger.info(" Total Elapsed time -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis())+"\n");
    }

    void insertIntoCatalog(int mapKey) throws Exception{
        String file = ratFiles.get(mapKey);
        HashMap<String, String> termNames = new HashMap<>();
        HashMap<String, String> termMap = assignCmoVtMaps(termNames);
        HashMap<GWASCatalog, List<String>> gwasVersionMap = new HashMap<>();
        List<VariantMapData> newVars = new ArrayList<>();
        List<VariantSampleDetail> newSamples = new ArrayList<>();
//        List<GWASCatalog> gwasList = new ArrayList<>();
        try (BufferedReader br = openFile(file)) {
            String lineData;
            while ((lineData = br.readLine()) != null) {
                GWASCatalog g = new GWASCatalog();
                if (lineData.startsWith("Chr")){
                    continue;
                }
                String[] splitLine = lineData.split("\t");
                String chr = splitLine[0];
//                21 = chrX
//                22 = chrY
//                24 = chrM/MT,
                switch (chr) {
                    case "21" -> chr = "X";
                    case "22" -> chr = "Y";
                    case "24" -> chr = "MT";
                    default -> chr = splitLine[0];
                }
                g.setChr(chr);
                int pos = Integer.parseInt(splitLine[2]);
                g.setPos(splitLine[2]);
                String allele = splitLine[3];
                g.setStrongSnpRiskallele(allele);
                String ref = splitLine[4];
                g.setRiskAlleleFreq(splitLine[5]);
                String beta = splitLine[6];
                g.setOrBeta(beta);
                String pVal = splitLine[8];
                g.setpVal(pVal);
                String traits = termMap.get(splitLine[12]);
                if (Utils.isStringEmpty(traits))
                    continue;
                g.setEfoId(traits);
                g.setMapTrait(termNames.get(splitLine[12]));
                g.setContext(splitLine[12]);
                // splitLine[18] is version
                VariantMapData var = dao.getVariantByChrPosRefAlleleMapKey(chr, pos, ref, allele, mapKey);
                if (var == null) {
                    var = createMapData(g, ref, mapKey);
                    newSamples.add(createGwasVariantSampleDetail(var));
                    newVars.add(var);
//                    continue;
                }
                else {
                    // check sample details if one exists
                    List<VariantSampleDetail> details = dao.getVariantSampleDetail((int)var.getId(),sampleId);
                    if (details.isEmpty())
                    {
                        newSamples.add(createGwasVariantSampleDetail(var));
                    }
                }
                g.setVariantRgdId((int)var.getId());

                if (!Utils.isStringEmpty(var.getRsId()) && var.getRsId().startsWith("rs"))
                    g.setSnps(var.getRsId());

                g.setMapKey(mapKey);

                GWASCatalog exist = dao.getGWASbyChrPosPValMapKey(g.getChr(),g.getPos(),g.getpVal(),g.getMapKey());
                if (exist!=null){
                    String ver = splitLine[18];
                    List<String> versions = new ArrayList<>();
                    versions.add(ver);
                    gwasVersionMap.put(g, versions);
                }
                else if (gwasVersionMap.get(g)==null) {
                    String ver = splitLine[18];
                    List<String> versions = new ArrayList<>();
                    versions.add(ver);
                    gwasVersionMap.put(g, versions);
                }
                else {
                    List<String> versions = gwasVersionMap.get(g);
                    String ver = splitLine[18];
                    if (!versions.contains(ver)){
                        versions.add(ver);
                        gwasVersionMap.put(g, versions);
                    }
                }
            }

            if (!newVars.isEmpty()){
                logger.info("\tNew Variants being entered: "+newVars.size());
                dao.insertVariants(newVars,varLog);
                dao.insertVariantMapData(newVars);
            }
            if (!newSamples.isEmpty()){
                logger.info("\tNew Variant Samples being entered: "+newSamples.size());
                dao.insertVariantSample(newSamples);
            }
            List<GWASCatalog> inDb = dao.getGWASByMapKey(mapKey);
            Collection<GWASCatalog> insert = CollectionUtils.subtract(gwasVersionMap.keySet(), inDb);
            Collection<GWASCatalog> delete = CollectionUtils.subtract(inDb,gwasVersionMap.keySet());
            Collection<GWASCatalog> existing = CollectionUtils.intersection(gwasVersionMap.keySet(),inDb);
            if (!insert.isEmpty()){
                logger.info("\tNew Rat GWAS being entered: "+insert.size());
                dao.insertGWASBatch(insert);
            }
            if (!delete.isEmpty()){
                logger.info("\tRat GWAS being removed: "+delete.size());
                dao.deleteGWASBatch(delete);
                dao.withdrawQTLs(delete);
            }
            if (!existing.isEmpty()){
                logger.info("\tRat GWAS unchanged: "+existing.size());
            }

        }
        catch (Exception e){
            Utils.printStackTrace(e,logger);
        }
        List<GWASVersion> allVersions = new ArrayList<>();
        for (GWASCatalog g : gwasVersionMap.keySet()){
            List<String> versions = gwasVersionMap.get(g);
            for (String ver : versions){
                GWASVersion gv = new GWASVersion();
                gv.setGwasId(g.getGwasId());
                gv.setVersion(ver);
                if (!allVersions.contains(gv))
                    allVersions.add(gv);
            }
        }
        List<GWASVersion> inDb = dao.getGwasVersion();
        Collection<GWASVersion> ins = CollectionUtils.subtract(allVersions, inDb);
        Collection<GWASVersion> del = CollectionUtils.subtract(inDb, allVersions);
        Collection<GWASVersion> exi = CollectionUtils.intersection(allVersions, inDb);
        if (!ins.isEmpty()){
            logger.info("GWAS Versions being entered: " + ins.size());
            dao.insertGWASVersionBatch(ins);
        }
        if (!del.isEmpty()){
            logger.info("GWAS Versions being deleted: " + del.size());
            dao.deleteGWASVersionBatch(del);
        }
        if (!exi.isEmpty()){
            logger.info("Versions unchanged: " + exi.size());
        }
    }

    void readAndWriteToFile(int mapKey) throws Exception{
        String file = ratFiles.get(mapKey);
        String[] fileSplit = file.split("/");
        int notfound = 0;
        int diffAsm = 0;
        int found = 0;
        try {
            BufferedReader br = openFile(file);
            BufferedWriter bw = new BufferedWriter(new FileWriter("updated_"+fileSplit[fileSplit.length-1]));
            String lineData;
            while ((lineData = br.readLine()) != null) {
                if (lineData.startsWith("Chr")){
                    bw.write(lineData+"\trs ID\tRGD ID\n");
                    continue;
                }
                String[] splitLine = lineData.split("\t");
                String chr = splitLine[0];
//                21 = chrX
//                22 = chrY
//                24 = chrM/MT,
                switch (chr) {
                    case "21" -> chr = "X";
                    case "22" -> chr = "Y";
                    case "24" -> chr = "MT";
                    default -> chr = splitLine[0];
                }
                int pos = Integer.parseInt(splitLine[2]);
                String allele = splitLine[3];
                String ref = splitLine[4];

                List<VariantMapData> variants = dao.getVariantsBySpecies(chr, pos, 3);
                if (variants == null || variants.isEmpty()){
                    logger.info("\tNo Variant found!\t"+chr+":"+pos);
                    logger.debug("\t\t"+lineData);
                    bw.write(lineData+"\t\t\n");
                    notfound++;
                    continue;
                }
                VariantMapData v = null;
                for (VariantMapData vmd : variants){
                    if (Utils.stringsAreEqual(vmd.getReferenceNucleotide(),ref) &&
                            Utils.stringsAreEqual(vmd.getVariantNucleotide(),allele) && mapKey == vmd.getMapKey()){
                        v = vmd;
                        break;
                    }
                }

                if (v == null){
                    logger.info("\tVariant not found in given assembly!\t"+chr+":"+pos);
                    logger.debug("\t\t"+lineData);
                    bw.write(lineData+"\t\t\n");
                    diffAsm++;
                    continue;
                }

                if (!Utils.isStringEmpty(v.getRsId()) && !Utils.stringsAreEqual(v.getRsId(), ".")){
                    bw.write(lineData+"\t"+v.getRsId()+"\t"+v.getId()+"\n");
                    found++;
                }
                else {
                    List<VariantSSId> ssIds = dao.getVariantSSIds((int) v.getId());
                    StringBuilder builder = new StringBuilder();
                    for (VariantSSId id : ssIds){
                        builder.append(id.getSSId());
                        builder.append(" ");
                    }
                    String ssid = builder.toString();
                    if (Utils.isStringEmpty(ssid)) {
                        bw.write(lineData + "\t\t" + v.getId() + "\n");
                        found++;
                    }
                    else {
                        bw.write(lineData + "\t" + ssid.trim() + "\t" + v.getId() + "\n");
                        found++;
                    }
                }
            }

            br.close();
            bw.close();

            logger.info("\tTotal found: "+ found);
            logger.info("\tTotal not Found: "+notfound);
            logger.info("\tTotal in different Assembly: "+diffAsm);
        }
        catch (Exception e){
            logger.info(e);
        }
    }

    HashMap<String, String> assignCmoVtMaps(HashMap<String, String> termName) throws Exception {
        HashMap<String, String> terms = new HashMap<>();
        try (BufferedReader br = openFile(cmoVtTermFile)){
            String lineData;
            while ((lineData = br.readLine()) != null){
                String[] split = lineData.split("\t");
                /*  * 0 project name
                    * 1 trait
                    * 2 cmo term
                    * 3 cmo ACC id
                    * 4 vt term 1
                    * 5 vt 1 ACC id
                    * 6 vt term 2
                    * 7 vt 2 ACC id     */
                String trait = split[1];
                String cmoName = split[2];
                String cmo = split[3];
                String vtName1= split[4];
                String vt1 = split[5];
                String vtName2 = split[6];
                String vt2 = split[7];
                StringBuilder sb = new StringBuilder();
                StringBuilder sbName = new StringBuilder();
                sb.append(cmo).append(",").append(vt1);
                sbName.append(vtName1);
                if (!Utils.isStringEmpty(vt2)){
                    sb.append(",").append(vt2);
                    sbName.append(" and ").append(vtName2);
                }
                termName.put(trait,sbName.toString());
                terms.put(trait, sb.toString());
            }
        }
        return terms;
    }

    VariantMapData createMapData(GWASCatalog g, String ref, int mapKey) throws Exception{
        VariantMapData vmd = new VariantMapData();
        int speciesKey= SpeciesType.getSpeciesTypeKeyForMap(mapKey);
        RgdId r = dao.createRgdId(RgdId.OBJECT_KEY_VARIANTS, "ACTIVE", "created by GWAS Catalog pipeline", mapKey);
        vmd.setId(r.getRgdId());
        vmd.setRsId(g.getSnps());
        vmd.setSpeciesTypeKey(speciesKey);
        String varType = "snv";
        String riskAllele = g.getStrongSnpRiskallele().replaceAll("\\s+","" );
        vmd.setVariantType(varType);
        vmd.setChromosome(g.getChr());
        vmd.setGenicStatus( dao.isGenic(mapKey,g.getChr(),Integer.parseInt(g.getPos())) ? "GENIC":"INTERGENIC" );
        vmd.setStartPos( Integer.parseInt(g.getPos()) );
        vmd.setReferenceNucleotide(ref);
        vmd.setVariantNucleotide(riskAllele);
        vmd.setEndPos(Integer.parseInt(g.getPos())+1);
        vmd.setMapKey(mapKey);
        return vmd;
    }

    public VariantSampleDetail createGwasVariantSampleDetail(VariantMapData vmd) throws Exception{
        VariantSampleDetail vsd = new VariantSampleDetail();
        vsd.setId(vmd.getId());
        vsd.setSampleId( sampleId );
        vsd.setDepth(9);
        vsd.setVariantFrequency(1);
        return vsd;
    }

    private BufferedReader openFile(String fileName) throws IOException {

        String encoding = "UTF-8"; // default encoding

        InputStream is;
        if( fileName.endsWith(".gz") ) {
            is = new GZIPInputStream(new FileInputStream(fileName));
        } else {
            is = new FileInputStream(fileName);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(is, encoding));
        return reader;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setRatFiles(Map<Integer, String> ratFiles) {
        this.ratFiles = ratFiles;
    }

    public Map<Integer, String> getRatFiles() {
        return ratFiles;
    }

    public void setSampleId(int sampleId) {
        this.sampleId = sampleId;
    }

    public int getSampleId() {
        return sampleId;
    }

    public void setCmoVtTermFile(String cmoVtTermFile) {
        this.cmoVtTermFile = cmoVtTermFile;
    }

    public String getCmoVtTermFile() {
        return cmoVtTermFile;
    }
}
