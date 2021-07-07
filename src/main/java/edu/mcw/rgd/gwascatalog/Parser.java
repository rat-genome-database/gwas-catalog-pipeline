package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.datamodel.GWASCatalog;
import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Parser {
    int cnt = 0;
    public ArrayList<GWASCatalog> parse(String myFile) throws Exception
    {
        ArrayList<GWASCatalog> list = new ArrayList<>();
        BufferedReader br = Utils.openReader(myFile);
        String lineData;
        String[] col = null;
        ArrayList<String> columns = new ArrayList<>();
        int i = 0,riskAllele=0;
        try {
            while ((lineData = br.readLine()) != null) {
                if (i == 0) {
                    col = lineData.split("\t");
                    columns = new ArrayList<String>(Arrays.asList(col));
                    riskAllele = columns.indexOf("STRONGEST SNP-RISK ALLELE");
//                System.out.println(riskAllele);
                } else // if (i < 30)
                {
                    GWASCatalog gc = parseLine(lineData, riskAllele, columns);
                    ArrayList<GWASCatalog> splitData = copiedData(gc);
                    if (splitData != null)
                        list.addAll(splitData);
                }

                i++;
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            br.close();
        }


//        System.out.println(list.size());
//        System.out.println(cnt);
        return list;
    }

    ArrayList<GWASCatalog> copiedData(GWASCatalog gc) throws Exception {
        ArrayList<GWASCatalog> shallow = new ArrayList<>();
        if (gc.getPos().contains("x")) {
//            System.out.println(gc.print());
            return null;
        }
        else if (gc.getSnps().isEmpty())
            shallow.add(gc);
        else {
            String[] listChr = gc.getChr().split(";");
            String[] listPos = gc.getPos().split(";");
            String[] riskAllele = gc.getStrongSnpRiskallele().split("/");
            String[] snps = gc.getSnps().split(";");
            for (int i = 0; i < listChr.length; i++) {
                GWASCatalog carbon = new GWASCatalog(gc, listChr[i],listPos[i],riskAllele[i],snps[i]);
//                System.out.println(gc.print());
                shallow.add(carbon);
            }
        }
        return shallow;
    }

    GWASCatalog parseLine(String lineData, int riskAllele, ArrayList<String> columns) throws Exception
    {
//        System.out.println(lineData);
        GWASCatalog gc = new GWASCatalog();
        String[] row = lineData.split("\t");
        for (int i = 0; i < row.length; i++) {
            switch (columns.get(i)){
                case "DATE ADDED TO CATALOG":
                    break;
                case "PUBMEDID":
                    //System.out.print(row[i]+"|");
                    int pmid = Integer.parseInt(row[i]);
                    gc.setPmid("PMID:"+pmid);
                    break;
                case "FIRST AUTHOR":
                    break;
                case "DATE":
                    break;
                case "JOURNAL":
                    break;
                case "LINK":
                    break;
                case "STUDY":
                    break;
                case "DISEASE/TRAIT":
                    //System.out.print(row[i]+"|");
                    gc.setDiseaseTrait(row[i]);
                    break;
                case "INITIAL SAMPLE SIZE":
                    //System.out.print(row[i]+"|");
                    gc.setInitialSample(row[i]);
                    break;
                case "REPLICATION SAMPLE SIZE":
                    //System.out.print(row[i]+"|");
                    gc.setReplicateSample(row[i]);
                    break;
                case "REGION":
                    //System.out.print(row[i]+"|");
                    gc.setRegion(row[i]);
                    break;
                case "CHR_ID":
                    //System.out.print(row[i]+"|");
                    gc.setChr(row[i]);
                    break;
                case "CHR_POS":
                    //System.out.print(row[i]+"|");
                    gc.setPos(row[i]);
                    break;
                case "REPORTED GENE(S)":
                    //System.out.print(row[i]+"|");
                    gc.setReportedGenes(row[i]);
                    break;
                case "MAPPED_GENE":
                    //System.out.print(row[i]+"|");
                    gc.setMappedGene(row[i]);
                    break;
                case "UPSTREAM_GENE_ID":
                    break;
                case "DOWNSTREAM_GENE_ID":
                    break;
                case "SNP_GENE_IDS":
                    //System.out.print(row[i]+"|");
                    gc.setSnpGeneId(row[i]);
                    break;
                case "UPSTREAM_GENE_DISTANCE":
                    break;
                case "DOWNSTREAM_GENE_DISTANCE":
                    break;
                case "STRONGEST SNP-RISK ALLELE":
                    //System.out.print(row[i]+"|");
                    try {
                        String[] alleles = row[riskAllele].split(";");
                        String variant = "";
                        for (int j = 0; j < alleles.length; j++){
                            String[] allele = alleles[j].split("-");
                            if (j != alleles.length-1)
                                variant += allele[allele.length - 1]+"/";
                            else
                                variant += allele[allele.length - 1];
                        }
                        gc.setStrongSnpRiskallele(variant);
                    }
                    catch (Exception e) {
                        String[] allele = row[riskAllele].split("-");
                        gc.setStrongSnpRiskallele(allele[allele.length - 1]);
                    }
                    break;
                case "SNPS":
                    //System.out.print(row[i]+"|");
                    gc.setSnps(row[i]);
                    break;
                case "MERGED":
                    break;
                case "SNP_ID_CURRENT":
                    //System.out.print(row[i]+"|");
                    gc.setCurSnpId(row[i]);
                    break;
                case "CONTEXT":
                    //System.out.print(row[i]+"|");
                    gc.setContext(row[i]);
                    break;
                case "INTERGENIC":
                    break;
                case "RISK ALLELE FREQUENCY":
                    //System.out.print(row[i]+"|");
                    gc.setRiskAlleleFreq(row[i]);
                    break;
                case "P-VALUE":
                    //System.out.print(row[i]+"|");
                    gc.setpVal(row[i]);
                    break;
                case "PVALUE_MLOG":
                    //System.out.print(row[i]+"|");
                    //BigDecimal d = new BigDecimal(row[i], MathContext.DECIMAL64).stripTrailingZeros();
                    gc.setpValMlog(Double.parseDouble(row[i]));
                    break;
                case "P-VALUE (TEXT)":
                    break;
                case "OR or BETA":
                    break;
                case "95% CI (TEXT)":
                    break;
                case "PLATFORM [SNPS PASSING QC]":
                    //System.out.print(row[i]+"|");
                    gc.setSnpPassQc(row[i]);
                    break;
                case "CNV":
                    break;
                case "MAPPED_TRAIT":
                    //System.out.print(row[i]+"|");
                    gc.setMapTrait(row[i]);
                    break;
                case "MAPPED_TRAIT_URI":
                    //System.out.print(row[i]+"|");
                    // only get EFO Id
                    String[] urls = row[i].split(",");
                    String efoId = "";
                    for (int j = 0; j < urls.length; j++){
                        String[] efo = urls[j].split("/");
                        if (j == urls.length-1)
                            efoId += efo[efo.length-1];
                        else
                            efoId += (efo[efo.length-1]+", ");
                    }
                    gc.setEfoId(efoId);
                    break;
                case "STUDY ACCESSION":
                    //System.out.print(row[i]+"|");
                    gc.setStudyAcc(row[i]);
                    break;
                case "GENOTYPING TECHNOLOGY":
                    break;
                default:
                    System.out.print(row[i]);
            }

        }
//        System.out.println();
        return gc;
    }
}
