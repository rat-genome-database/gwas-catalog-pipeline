package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.datamodel.GWASCatalog;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;

/**
 * Loops through the entire GWAS catalog for human (map key 38) and writes a log file
 * listing the distinct ontology terms whose id matches the configured prefix
 * (e.g. EFO, MONDO, Orphanet, HP, GO).  The prefix is supplied as a bean property
 * in AppConfigure.xml.
 */
public class GwasOntologyList {
    private String version;
    private String prefix;
    DAO dao = new DAO();
    protected Logger logger = LogManager.getLogger("ontologyList");

    void run() throws Exception {
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long pipeStart = System.currentTimeMillis();
        logger.info(getVersion());
        logger.info("   Pipeline started at " + sdt.format(new Date(pipeStart)));
        logger.info("   Listing distinct ontology terms with prefix: " + getPrefix());

        List<GWASCatalog> catalog = dao.getGWASByMapKey(38);
        logger.info("   GWAS catalog rows retrieved: " + catalog.size());

        // TreeSet gives sorted, distinct terms
        TreeSet<String> terms = new TreeSet<>();
        for (GWASCatalog gc : catalog) {
            String efoId = gc.getEfoId();
            if (Utils.isStringEmpty(efoId))
                continue;

            // a single row can list multiple comma-separated ontology terms
            for (String term : efoId.split(",")) {
                term = term.trim();
                if (matchesPrefix(term))
                    terms.add(term);
            }
        }

        for (String term : terms) {
            logger.info(term);
        }

        logger.info("   Total distinct ontology terms with prefix '" + getPrefix() + "': " + terms.size());
        logger.info("   Elapsed time: " + Utils.formatElapsedTime(pipeStart, System.currentTimeMillis()));
    }

    boolean matchesPrefix(String term) {
        if (Utils.isStringEmpty(term) || Utils.isStringEmpty(prefix))
            return false;
        return term.toUpperCase().startsWith(prefix.toUpperCase());
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }
}
