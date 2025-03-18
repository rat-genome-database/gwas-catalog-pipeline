package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSSId;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class RatGwas {

    private String version;
    private Map<Integer, String> ratFiles;
    protected Logger logger = LogManager.getLogger("ratStatus");
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
                readAndWriteToFile(mapKey);
                logger.info("-----------------------------------------");
            }
        }catch (Exception e){
            Utils.printStackTrace(e,logger);
        }

        logger.info(" Total Elapsed time -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis())+"\n");
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
}
