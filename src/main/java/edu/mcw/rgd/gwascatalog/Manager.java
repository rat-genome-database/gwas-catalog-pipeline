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

    public static void main(String[] args) throws Exception
    {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));

        try
        {
            for (int i = 0; i < args.length; i++){
                switch (args[i]){
                    case "--importAssoc":
                        GWASCatImport gcimport = (GWASCatImport) (bf.getBean("gwasCatImport"));
                        gcimport.run();
                        break;
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }



}
