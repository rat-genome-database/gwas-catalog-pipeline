package edu.mcw.rgd.gwascatalog;

import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

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
                        GWASCatImport gcImport = (GWASCatImport) (bf.getBean("gwasCatImport"));
                        gcImport.run();
                        break;
                    case "--assignRgdId":
                        GwasRgdIdAssign gcAssign = (GwasRgdIdAssign) (bf.getBean("gwasRgdIdAssign"));
                        gcAssign.run();
                        break;
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }



}
