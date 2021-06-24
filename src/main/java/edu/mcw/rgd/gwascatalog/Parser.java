package edu.mcw.rgd.gwascatalog;

import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

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
        while((lineData = br.readLine()) != null)
        {
            if (i==0)
            {
                col = lineData.split("\t");
                columns = new ArrayList<String>(Arrays.asList(col));
                riskAllele = columns.indexOf("STRONGEST SNP-RISK ALLELE");
                System.out.println(riskAllele);
            }
            else
            {
                list.add(parseLine(lineData,riskAllele, columns));
            }

            i++;
        }
        System.out.println(cnt);
        return list;
    }
    GWASCatalog parseLine(String lineData, int riskAllele, ArrayList<String> columns) throws Exception
    {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        System.out.println(lineData);
        GWASCatalog gc = new GWASCatalog();
        String[] row = lineData.split("\t");
        for (int i = 0; i < row.length; i++) {
            switch (columns.get(i)){
                case "DATE ADDED TO CATALOG":
                    //System.out.print(row[i]+"|");
                    java.util.Date date = sdf.parse(row[i]);
                    Date sqlDate = new Date(date.getTime());
                    gc.setAdded(sqlDate);
                    break;
                case "PUBMEDID":
                    //System.out.print(row[i]+"|");
                    int pmid = Integer.parseInt(row[i]);
                    gc.setPmid("PMID:"+pmid);
                    break;
                case "FIRST AUTHOR":
                    //System.out.print(row[i]+"|");
                    gc.setAuthor(row[i]);
                    break;
                case "DATE":
                    //System.out.print(row[i]+"|");
                    java.util.Date date1 = sdf.parse(row[i]);
                    Date sqlDate1 = new Date(date1.getTime());
                    gc.setDate(sqlDate1);
                    break;
                case "JOURNAL":
                    //System.out.print(row[i]+"|");
                    gc.setJournal(row[i]);
                    break;
                case "LINK":
                    //System.out.print(row[i]+"|");
                    gc.setLink(row[i]);
                    break;
                case "STUDY":
                    //System.out.print(row[i]+"|");
                    gc.setStudy(row[i]);
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
                    //System.out.print(row[i]+"|");
                    gc.setUpstreamGeneId(row[i]);
                    break;
                case "DOWNSTREAM_GENE_ID":
                    //System.out.print(row[i]+"|");
                    gc.setDownstreamGeneId(row[i]);
                    break;
                case "SNP_GENE_IDS":
                    //System.out.print(row[i]+"|");
                    gc.setSnpGeneId(row[i]);
                    break;
                case "UPSTREAM_GENE_DISTANCE":
                    //System.out.print(row[i]+"|");
                    gc.setUpstreamDistance(row[i]);
                    break;
                case "DOWNSTREAM_GENE_DISTANCE":
                    //System.out.print(row[i]+"|");
                    gc.setDownstreamDistance(row[i]);
                    break;
                case "STRONGEST SNP-RISK ALLELE":
                    //System.out.print(row[i]+"|");
                    // try split with ";", then insert like X/Y
                    try {
                        String[] alleles = row[riskAllele].split(";");
                        String variant = "";
                        for (int j = 0; j < alleles.length; j++){
                            String[] allele = alleles[j].split("-");
                            if (allele[allele.length - 1].equals("?"))
                                cnt++;
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
                    //System.out.print(row[i]+"|");
                    gc.setMerge(Integer.parseInt(row[i]));
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
                    //System.out.print(row[i]+"|");
                    gc.setIntergenic(row[i]);
                    break;
                case "RISK ALLELE FREQUENCY":
                    //System.out.print(row[i]+"|");
                    gc.setRiskAlleleFreq(row[i]);
                    break;
                case "P-VALUE":
                    //System.out.print(row[i]+"|");
                    gc.setpVal(Double.parseDouble(row[i]));
                    break;
                case "PVALUE_MLOG":
                    //System.out.print(row[i]+"|");
                    gc.setpValMlog(Double.parseDouble(row[i]));
                    break;
                case "P-VALUE (TEXT)":
                    //System.out.print(row[i]+"|");
                    gc.setpValTxt(row[i]);
                    break;
                case "OR or BETA":
                    //System.out.print(row[i]+"|");
                    gc.setOrBeta(row[i]);
                    break;
                case "95% CI (TEXT)":
                    //System.out.print(row[i]+"|");
                    gc.setCi95(row[i]);
                    break;
                case "PLATFORM [SNPS PASSING QC]":
                    //System.out.print(row[i]+"|");
                    gc.setPlatform(row[i]);
                    break;
                case "CNV":
                    //System.out.print(row[i]+"|");
                    gc.setCnv(row[i]);
                    break;
                case "MAPPED_TRAIT":
                    //System.out.print(row[i]+"|");
                    gc.setMapTrait(row[i]);
                    break;
                case "MAPPED_TRAIT_URI":
                    //System.out.print(row[i]+"|");
                    gc.setMapTraitUri(row[i]);
                    break;
                case "STUDY ACCESSION":
                    //System.out.print(row[i]+"|");
                    gc.setStudyAcc(row[i]);
                    break;
                case "GENOTYPING TECHNOLOGY":
                    //System.out.print(row[i]+"|");
                    gc.setGenotypeTech(row[i]);
                    break;
                default:
                    System.out.print(row[i]);
            }

        }
//        System.out.println();
        return gc;
    }
}
