package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.datamodel.GWASCatalog;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class WithdrawVariants {

    String version;
    Logger log = LogManager.getLogger("withdrawStatus");
    private DAO dao = new DAO();
    public void run() throws Exception{
        // go through all gwas and check with VAriant tables
        // if not present, add to hash map (rgdId, varMapData)
        // if found in a different row, remove from hash map
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info(getVersion());
        long pipeStart = System.currentTimeMillis();
        log.info("   Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");
        HashMap<Long, VariantMapData> remove = new HashMap<>();
        HashMap<Long, VariantMapData> keep = new HashMap<>();
        List<GWASCatalog> catalog = dao.getFullCatalog();

        for (GWASCatalog gwas : catalog) { // totally didn't just make this confusing
            if (gwas.getStrongSnpRiskallele()==null)
                continue;
            List<VariantMapData> vars = dao.getVariantsByRsId(gwas.getSnps());
            if (vars.isEmpty())
                continue;
            String allele = gwas.getStrongSnpRiskallele();
            int pos;
            try{
                pos = Integer.parseInt(gwas.getPos());
            }
            catch (Exception e){
                pos = 0;
            }
            for (VariantMapData vm : vars){
                // loop through variants, if it does exist add to keep and remove from 'remove'
                if (Utils.stringsAreEqual(allele,vm.getVariantNucleotide()) && vm.getStartPos()==pos){// compare by position and var nuc
                    remove.remove(vm.getId());
                    keep.put(vm.getId(),vm);
                }
                else {
                    // else if does not exist add to remove
                    if (keep.get(vm.getId()) == null)
                        remove.put(vm.getId(),vm);
                }
            }

        } // end for gwas
        log.info("Variants to be withdrawn:"+remove.size()+"|Variants to be kept:"+keep.size());
        if (!remove.isEmpty()){
            dao.withdrawVariants(remove.keySet(),log);
        }
        log.info("   Total GWAS Catalog pipeline runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
