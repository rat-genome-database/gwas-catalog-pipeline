package edu.mcw.rgd.gwascatalog;

import java.sql.Date;

public class GWASCatalog {
    private Date added;
    private String pmid;
    private String author;
    private Date date;
    private String journal;
    private String link;
    private String study;
    private String diseaseTrait;
    private String initialSample;
    private String replicateSample;
    private String region;
    private String chr;
    private String pos;
    private String reportedGenes;
    private String mappedGene;
    private String upstreamGeneId;
    private String downstreamGeneId;
    private String snpGeneId;
    private String upstreamDistance;
    private String downstreamDistance;
    private String strongSnpRiskallele;
    private String snps;
    private int merge;
    private String curSnpId;
    private String context;
    private String intergenic;
    private String riskAlleleFreq;
    private double pVal;
    private double pValMlog;
    private String pValTxt;
    private String orBeta;
    private String ci95;
    private String platform;
    private String cnv;
    private String mapTrait;
    private String mapTraitUri;
    private String studyAcc;
    private String genotypeTech;

    public GWASCatalog(){};

    public Date getAdded() {
        return added;
    }

    public void setAdded(Date added) {
        this.added = added;
    }

    public String getPmid() {
        return pmid;
    }

    public void setPmid(String pmid) {
        this.pmid = pmid;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getJournal() {
        return journal;
    }

    public void setJournal(String journal) {
        this.journal = journal;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getStudy() {
        return study;
    }

    public void setStudy(String study) {
        this.study = study;
    }

    public String getDiseaseTrait() {
        return diseaseTrait;
    }

    public void setDiseaseTrait(String diseaseTrait) {
        this.diseaseTrait = diseaseTrait;
    }

    public String getInitialSample() {
        return initialSample;
    }

    public void setInitialSample(String initialSample) {
        this.initialSample = initialSample;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getChr() {
        return chr;
    }

    public void setChr(String chr) {
        this.chr = chr;
    }

    public String getPos() {
        return pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public String getReportedGenes() {
        return reportedGenes;
    }

    public void setReportedGenes(String reportedGenes) {
        this.reportedGenes = reportedGenes;
    }

    public String getMappedGene() {
        return mappedGene;
    }

    public void setMappedGene(String mappedGene) {
        this.mappedGene = mappedGene;
    }

    public String getUpstreamGeneId() {
        return upstreamGeneId;
    }

    public void setUpstreamGeneId(String upstreamGeneId) {
        this.upstreamGeneId = upstreamGeneId;
    }

    public String getDownstreamGeneId() {
        return downstreamGeneId;
    }

    public void setDownstreamGeneId(String downstreamGeneId) {
        this.downstreamGeneId = downstreamGeneId;
    }

    public String getSnpGeneId() {
        return snpGeneId;
    }

    public void setSnpGeneId(String snpGeneId) {
        this.snpGeneId = snpGeneId;
    }

    public String getUpstreamDistance() {
        return upstreamDistance;
    }

    public void setUpstreamDistance(String upstreamDistance) {
        this.upstreamDistance = upstreamDistance;
    }

    public String getDownstreamDistance() {
        return downstreamDistance;
    }

    public void setDownstreamDistance(String downstreamDistance) {
        this.downstreamDistance = downstreamDistance;
    }

    public String getStrongSnpRiskallele() {
        return strongSnpRiskallele;
    }

    public void setStrongSnpRiskallele(String strongSnpRiskallele) {
        this.strongSnpRiskallele = strongSnpRiskallele;
    }

    public String getSnps() {
        return snps;
    }

    public void setSnps(String snps) {
        this.snps = snps;
    }

    public int getMerge() {
        return merge;
    }

    public void setMerge(int merge) {
        this.merge = merge;
    }

    public String getCurSnpId() {
        return curSnpId;
    }

    public void setCurSnpId(String curSnpId) {
        this.curSnpId = curSnpId;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getIntergenic() {
        return intergenic;
    }

    public void setIntergenic(String intergenic) {
        this.intergenic = intergenic;
    }

    public String getRiskAlleleFreq() {
        return riskAlleleFreq;
    }

    public void setRiskAlleleFreq(String riskAlleleFreq) {
        this.riskAlleleFreq = riskAlleleFreq;
    }

    public double getpVal() {
        return pVal;
    }

    public void setpVal(double pVal) {
        this.pVal = pVal;
    }

    public double getpValMlog() {
        return pValMlog;
    }

    public void setpValMlog(double pValMlog) {
        this.pValMlog = pValMlog;
    }

    public String getpValTxt() {
        return pValTxt;
    }

    public void setpValTxt(String pValTxt) {
        this.pValTxt = pValTxt;
    }

    public String getOrBeta() {
        return orBeta;
    }

    public void setOrBeta(String orBeta) {
        this.orBeta = orBeta;
    }

    public String getCi95() {
        return ci95;
    }

    public void setCi95(String ci95) {
        this.ci95 = ci95;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getCnv() {
        return cnv;
    }

    public void setCnv(String cnv) {
        this.cnv = cnv;
    }

    public String getMapTrait() {
        return mapTrait;
    }

    public void setMapTrait(String mapTrait) {
        this.mapTrait = mapTrait;
    }

    public String getMapTraitUri() {
        return mapTraitUri;
    }

    public void setMapTraitUri(String mapTraitUri) {
        this.mapTraitUri = mapTraitUri;
    }

    public String getStudyAcc() {
        return studyAcc;
    }

    public void setStudyAcc(String studyAcc) {
        this.studyAcc = studyAcc;
    }

    public String getGenotypeTech() {
        return genotypeTech;
    }

    public void setGenotypeTech(String genotypeTech) {
        this.genotypeTech = genotypeTech;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getReplicateSample() {
        return replicateSample;
    }

    public void setReplicateSample(String replicateSample) {
        this.replicateSample = replicateSample;
    }
}
