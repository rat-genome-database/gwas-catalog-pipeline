package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.process.FileDownloader;
import org.apache.log4j.Logger;

import java.util.ArrayList;

public class GWASCatImport {
    private String version;
    private String gwasFile;

    protected Logger logger = Logger.getLogger("status");

    void run() throws Exception
    {
        logger.info(getVersion());
        String myFile = downloadFile(gwasFile);
        // create new method
        Parser parser = new Parser();
        ArrayList<GWASCatalog> incoming = parser.parse(myFile);

        // send incoming to method and check with DB

    }


    String downloadFile(String file) throws Exception{
        FileDownloader downloader = new FileDownloader();
        downloader.setExternalFile(file);
        downloader.setLocalFile("data/associations_ontology_annot.tsv");
        downloader.setUseCompression(true);
        downloader.setPrependDateStamp(true);
        return downloader.downloadNew();
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
