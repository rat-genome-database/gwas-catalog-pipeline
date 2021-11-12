package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.datamodel.GWASCatalog;
import edu.mcw.rgd.process.Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class Parser {
    int cnt = 0;
    public ArrayList<GWASCatalog> parse(String myFile) throws Exception
    {
        ArrayList<GWASCatalog> list = new ArrayList<>();
        BufferedReader br = openFile(myFile);
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
            String[] listChr = {null};  // = gc.getChr().split(";");
            String[] listPos = {null};  //gc.getPos().split(";");
            String[] riskAllele = {};   //gc.getStrongSnpRiskallele().split("/");
            String[] snps = {null}; //gc.getSnps().split(";");
            if (!gc.getChr().isEmpty())
                listChr = gc.getChr().split(";");
            if (!gc.getPos().isEmpty())
                listPos = gc.getPos().split(";");
            if (!gc.getStrongSnpRiskallele().isEmpty())
                riskAllele = gc.getStrongSnpRiskallele().split("/");
            if (!gc.getSnps().isEmpty())
                snps = gc.getSnps().split(";");

            for (int i = 0; i < listChr.length; i++) {
                GWASCatalog carbon;
                if (riskAllele.length==0 || checkStringNotUnicode(riskAllele[i])) {
                   carbon = new GWASCatalog(gc, listChr[i], listPos[i], null, snps[i]);
                }
                else {
                    carbon = new GWASCatalog(gc, listChr[i], listPos[i], riskAllele[i], snps[i]);
                }
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
        String rowCol;
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
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setDiseaseTrait(rowCol);
                    break;
                case "INITIAL SAMPLE SIZE":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setInitialSample(rowCol);
                    break;
                case "REPLICATION SAMPLE SIZE":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setReplicateSample(rowCol);
                    break;
                case "REGION":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setRegion(rowCol);
                    break;
                case "CHR_ID":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    gc.setChr(rowCol);
                    break;
                case "CHR_POS":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    gc.setPos(rowCol);
                    break;
                case "REPORTED GENE(S)":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setReportedGenes(rowCol);
                    break;
                case "MAPPED_GENE":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setMappedGene(rowCol);
                    break;
                case "UPSTREAM_GENE_ID":
                    break;
                case "DOWNSTREAM_GENE_ID":
                    break;
                case "SNP_GENE_IDS":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setSnpGeneId(rowCol);
                    break;
                case "UPSTREAM_GENE_DISTANCE":
                    break;
                case "DOWNSTREAM_GENE_DISTANCE":
                    break;
                case "STRONGEST SNP-RISK ALLELE":
                    //System.out.print(row[i]+"|");
                    try {
                        // check if the start is chr, then parse chromosome and position from it
                        String[] alleles = row[riskAllele].split(";");
                        if (alleles.length > 1) {
                            String variant = "";
                            for (int j = 0; j < alleles.length; j++) {
                                String[] allele = alleles[j].split("-");
                                if (j != alleles.length - 1)
                                    variant += allele[allele.length - 1] + "/";
                                else
                                    variant += allele[allele.length - 1];
                            }

                            gc.setStrongSnpRiskallele(variant);
                        }
                        else {
                            String[] allele = row[riskAllele].split("-");
                            if (allele.length == 1)
                                gc.setStrongSnpRiskallele("");
                            else
                                gc.setStrongSnpRiskallele(allele[allele.length - 1]);
                        }
                    }
                    catch (Exception e) {
                        String[] allele = row[riskAllele].split("-");
                        if (allele.length == 1)
                            gc.setStrongSnpRiskallele("");
                        else
                            gc.setStrongSnpRiskallele(allele[allele.length - 1]);
                    }
                    break;
                case "SNPS":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    gc.setSnps(rowCol);
                    break;
                case "MERGED":
                    break;
                case "SNP_ID_CURRENT":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setCurSnpId(rowCol);
                    break;
                case "CONTEXT":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setContext(rowCol);
                    break;
                case "INTERGENIC":
                    break;
                case "RISK ALLELE FREQUENCY":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setRiskAlleleFreq(rowCol);
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
                    gc.setOrBeta(row[i]);
                    break;
                case "95% CI (TEXT)":
                    break;
                case "PLATFORM [SNPS PASSING QC]":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setSnpPassQc(rowCol);
                    break;
                case "CNV":
                    break;
                case "MAPPED_TRAIT":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setMapTrait(rowCol);
                    break;
                case "MAPPED_TRAIT_URI":
                    //System.out.print(row[i]+"|");
                    String[] urls = row[i].split(",");
                    String efoId = "";
                    for (int j = 0; j < urls.length; j++){
                        String[] efo = urls[j].split("/");
                        if (j == urls.length-1)
                            efoId += efo[efo.length-1];
                        else
                            efoId += (efo[efo.length-1]+", ");
                    }
                    if (efoId.isEmpty())
                        efoId = null;
                    gc.setEfoId(efoId);
                    break;
                case "STUDY ACCESSION":
                    //System.out.print(row[i]+"|");
                    rowCol = row[i];
                    if (rowCol.isEmpty())
                        rowCol = null;
                    gc.setStudyAcc(rowCol);
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
    private BufferedReader openFile(String fileName) throws IOException {

        String encoding = "UTF-8"; // default encoding

        InputStream is;
        if( fileName.endsWith(".gz") ) {
            is = new GZIPInputStream(new FileInputStream(fileName));
        } else {
            is = new FileInputStream(fileName);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(is, encoding));
        return reader;
    }

    private boolean checkStringNotUnicode(String allele) throws Exception{

        for (char c : allele.toCharArray()){
            if (Character.UnicodeBlock.of(c) != Character.UnicodeBlock.BASIC_LATIN) {
                return true;
            }
        }
        return false;
    }
}
