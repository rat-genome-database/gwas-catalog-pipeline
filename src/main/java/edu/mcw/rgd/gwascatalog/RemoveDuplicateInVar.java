package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.datamodel.variants.VariantMapData;
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
        dao.setDataSource();
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logger.info(getVersion());
        long pipeStart = System.currentTimeMillis();
        logger.info("   Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");
        List<Long> rgdIds = dao.getGWASRgdIds();

        List<VariantMapData> gwasVMD = new ArrayList<>();
        // go through rgdIDs

        for (Long rgdId : rgdIds){
            List<VariantMapData> vmds = dao.getVariantsbyRgdId(rgdId.intValue());
            gwasVMD.addAll(vmds);
        }

        // maybe override equal for VMD and use the below method to see if work

        ArrayList<VariantMapData> newList = new ArrayList<>();
        Set<VariantMapData> set = new HashSet<>(gwasVMD);
        newList.addAll(set);

        // then withdraw rgdids that are no longer present
        // remove from rgdIds list based on what is in newList
        //  withdraw remaining ids
        for (VariantMapData vmd : newList){
            rgdIds.remove(vmd.getId());
        }

        System.out.println(rgdIds.size());
        dao.withdrawVariants(rgdIds, dupeVars);

    }
    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
