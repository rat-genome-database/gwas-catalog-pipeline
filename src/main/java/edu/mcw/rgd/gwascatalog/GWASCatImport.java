package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.dao.impl.GWASCatalogDAO;
import edu.mcw.rgd.datamodel.GWASCatalog;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.MathContext;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class GWASCatImport {
    private String version;
    private String gwasFile;
    private GWASCatalogDAO dao = new GWASCatalogDAO();

    protected Logger logger = Logger.getLogger("status");
    protected Logger inserted = Logger.getLogger("logInsertedGWAS");
    protected Logger deleted = Logger.getLogger("logDeletedGWAS");

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

        Collection<GWASCatalog> inserting = CollectionUtils.subtract(incoming,inRgd);
        if (!inserting.isEmpty()){
            logger.info("- - Total objects inserted: " + inserting.size());
            logInsDel(inserted, inserting);
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
