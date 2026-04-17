package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.process.MemoryMonitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

public class Manager {

    private static final Logger log = LogManager.getLogger("status");

    public static void main(String[] args) throws Exception
    {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));

        MemoryMonitor memoryMonitor = new MemoryMonitor();
        memoryMonitor.start();

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
                    case "--removeDupes":
                        RemoveDuplicateInVar gcRemove = (RemoveDuplicateInVar) (bf.getBean("gwasDuplicateRemoval"));
                        gcRemove.run();
                        break;
                    case "--withdrawOldVars":
                        WithdrawVariants withdrawVariants = (WithdrawVariants) (bf.getBean("withdrawVars"));
                        withdrawVariants.run();
                        break;
                    case "--ratLoad":
                        RatGwas rg = (RatGwas) (bf.getBean("ratGwas"));
                        rg.run();
                        break;
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }

        memoryMonitor.stop();
        log.info(memoryMonitor.getSummary());
    }



}
