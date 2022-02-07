package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.GWASCatalogDAO;
import edu.mcw.rgd.dao.impl.VariantDAO;
import edu.mcw.rgd.datamodel.GWASCatalog;
import edu.mcw.rgd.datamodel.Variant;
import edu.mcw.rgd.process.FastaParser;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GwasRgdIdAssign {
    String version;
    GWASCatalogDAO dao = new GWASCatalogDAO();
    VariantDAO vdao = new VariantDAO();
    protected Logger logger = LogManager.getLogger("assignSum");

    void run() throws Exception {
        vdao.setDataSource(DataSourceFactory.getInstance().getCarpeNovoDataSource());
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logger.info(getVersion());
        long pipeStart = System.currentTimeMillis();
        logger.info("   Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");
        // get variants rgd id from variant table in CN
        // check position, var_nuc=risk_allele
        // if ref = var then rgd_id=null
        // potentially grab whole catalog then add ones w/rgdids to new list to update and ignore above
        List<GWASCatalog> catalog = dao.getFullCatalog();
        ArrayList<GWASCatalog> updateRgdId = new ArrayList<>();

        for (GWASCatalog gc : catalog){

            if (gc.getChr() != null && gc.getPos() != null && gc.getStrongSnpRiskallele() != null) {
                long start = Integer.parseInt(gc.getPos());
                String ref = getRefAllele(38, gc);
                if (ref.equals(gc.getStrongSnpRiskallele()) || gc.getStrongSnpRiskallele().contains("?")) // skip lines that have the same ref and var nuc
                    continue;

                List<Variant> variants = vdao.getVariants(3, gc.getChr(), start, start); // get variants that are the same as GWAS data to get RGD ID
                for (Variant v : variants) {
                    if (gc.getVariantRgdId() == v.getRgdId()) {
                        logger.info("GWAS ID:" + gc.getGwasId() + " RGD_ID has not changed: " + gc.getVariantRgdId());
                        break;
                    }
                    if (v.getVariantNucleotide().equals(gc.getStrongSnpRiskallele())) { // check if var_nuc is the same
                        gc.setVariantRgdId(v.getRgdId());
                        updateRgdId.add(gc); // same variant_nucleotide added to insert list
                        logger.info("       GWAS ID: " + gc.getGwasId() + " getting assigned RGD ID: " + gc.getVariantRgdId());
                        break;
                    }
                } // end variants for
            } // end if
        } // end gwas for
        updateGwas(updateRgdId);
        logger.info("   Total GWAS Catalog pipeline runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));
    }

    void updateGwas(List<GWASCatalog> update) throws Exception{
        logger.info("       Total amount getting an updated variant RGD Id:"+update.size());
        dao.updateGWASBatch(update);
    }

    String getRefAllele(int mapKey, GWASCatalog gc) throws Exception {

        FastaParser parser = new FastaParser();
        parser.setMapKey(mapKey);
        if( parser.getLastError()!=null ) {

        }

        parser.setChr(Utils.defaultString(gc.getChr()));

        int startPos = Integer.parseInt(Utils.defaultString(gc.getPos()));
        int stopPos = Integer.parseInt(Utils.defaultString(gc.getPos()));

        String fasta = parser.getSequence(startPos, stopPos);
        if( parser.getLastError()!=null ) {

        }
        if( fasta == null ) {
            return null;
        }

        return fasta;
    }


    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
