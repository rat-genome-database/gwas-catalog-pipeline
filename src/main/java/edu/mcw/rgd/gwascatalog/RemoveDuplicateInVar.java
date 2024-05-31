package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.process.Utils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

public class RemoveDuplicateInVar {
    private String version;
    private DAO dao = new DAO();

    protected Logger logger = LogManager.getLogger("dupeRemove");
    protected Logger dupeVars = LogManager.getLogger("dupeVars");
    void run() throws Exception {
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logger.info(getVersion());
        long pipeStart = System.currentTimeMillis();
        logger.info("   Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");

        List<String> rsIds = dao.getGWASrsIds();
        List<Long> rgdIds = new ArrayList<>();

        try {
            List<VariantMapData> gwasVMD = new ArrayList<>();
//            List<VariantMapData> gwasVMD2 = new ArrayList<>();
            // go through rgdIDs

            for (String rsId : rsIds) {
                List<VariantMapData> vmds = dao.getActiveVariantsByRsId(rsId);
//                List<VariantMapData> vmds2 = dao.getVariantsByRsId(rsId);
                gwasVMD.addAll(vmds);
//                gwasVMD2.addAll(vmds2);
            }

            // maybe override equal for VMD and use the below method to see if work


            ArrayList<VariantMapData> newList = new ArrayList<>();
            Set<VariantMapData> set = new HashSet<>(gwasVMD);
            newList.addAll(set);
            Collection<VariantMapData> subtraction = CollectionUtils.subtract(gwasVMD, newList);
            // then withdraw rgdids that are no longer present
            // remove from rgdIds list based on what is in newList
            //  withdraw remaining ids
            for (VariantMapData vmd : subtraction) {
                rgdIds.add(vmd.getId());
            }
            logger.info("       Total RGD_IDS being withdrawn: "+rgdIds.size());
            dao.withdrawVariants(rgdIds, dupeVars);
        }
        catch (Exception e){
            logger.info(e);
        }
        logger.info("   pipeline runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));

    }
    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
