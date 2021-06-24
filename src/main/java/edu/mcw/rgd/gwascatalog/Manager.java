package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Arrays;

public class Manager {
    private String version;
    private String gwasFile;

    protected Logger logger = Logger.getLogger("status");

    public static void main(String[] args) throws Exception
    {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        edu.mcw.rgd.gwascatalog.Manager managerBean = (edu.mcw.rgd.gwascatalog.Manager) (bf.getBean("manager"));
        try
        {
            managerBean.run();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    void run() throws Exception
    {
        System.out.println("before download");
        String myFile = downloadFile(gwasFile);
        System.out.println("after download");
        // create new method
        Parser parser = new Parser();
        ArrayList<GWASCatalog> incoming = parser.parse(myFile);

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
