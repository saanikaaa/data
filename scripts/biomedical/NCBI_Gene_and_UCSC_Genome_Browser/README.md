# Importing NCBI Assembly Data

## Table of Contents

1. [About the Dataset](#about-the-dataset)
    1. [Download URL](#download-url)
    2. [Database Overview](#database-overview)
    3. [Schema Overview](#schema-overview)
    4. [Notes and Caveats](#notes-and-caveats)
    5. [License](#license)
    6. [Dataset Documentation and Relevant Links](#dataset-documentation-and-relevant-links)
2. [About the Import](#about-the-import)
    1. [Artifacts](#artifacts)
    2. [Import Procedure](#import-procedure)


## About the Dataset

### Download URL

Gene data can be downloaded from the National Center for Biotechnology Information (NCBI) Assembly database using their [Gene FTP Site](https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/). Thus far genes for the following species have been downloaded [Homo sapiens](https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz), [Mus musculus](https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Mus_musculus.gene_info.gz), [Danio rerio](https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Non-mammalian_vertebrates/Danio_rerio.gene_info.gz), [Gallus gallus](https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Non-mammalian_vertebrates/Gallus_gallus.gene_info.gz), [Xenopus laevis](https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Non-mammalian_vertebrates/Xenopus_laevis.gene_info.gz), [Caenorhabditis elegans](https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Invertebrates/Caenorhabditis_elegans.gene_info.gz), [Drosophila melanogaster](https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Invertebrates/Drosophila_melanogaster.gene_info.gz), and [Saccharomyces cerevisiae](https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Fungi/Saccharomyces_cerevisiae.gene_info.gz). These species were selected because they are common model organisms in biomedical research. The version included on Biomedical Data Commons is the Feburary 19, 2020 release.

Additional data on genes and RNA transcripts were downloaded for the two most recent genomoe assemblies for each species from the [UCSC Genome Browser](https://genome.ucsc.edu/). This was done by downloading the knownGene table using the [Table Browser](https://genome.ucsc.edu/cgi-bin/hgTables) on August 13, 2019. The following genome assemblies and builds were downloaded: galGal5 (dbSNP Build 147), hg19 (dbSNP Build 151), hg38 (dbSNP Build 151), mm9 (dbSNP Build 128), and mm10 (dbSNP Build 142). The data for all of these datasets are in tab delimited format.

### Database Overview

This directory stores the script used to import data on genes and RNA transcripts from NCBI Gene database and the UCSC Genome Browser. This contains information including the genomic coordinates of genes and RNA Transcripts, exon coordinates, names, alternative IDs, strand orientation, and type of gene (e.g. protein-coding, ncRNA, etc.). More on the nature of the data contained in [NCBI Gene](https://www.ncbi.nlm.nih.gov/books/NBK3841/#EntrezGene.Quick_Start) and the [USCSC Genome Browser](https://genome.ucsc.edu/goldenPath/newsarch.html) can be found on their respective websites.

### Schema Overview

The schema representing data from NCBI Gene and the UCSC Genome Browser is represented in [GenomeAnnotation.mcf](https://github.com/datacommonsorg/schema/blob/main/biomedical_schema/genome_annotation.mcf) and [GenomeAnnotationEnum.mcf](https://github.com/datacommonsorg/schema/tree/main/biomedical_schema/genome_annotation_enum.mcf).

The assembly reports contains instances of entities "Gene" and "RNATranscript". These are connected to entity "Species" by property "ofSpecies", "GenomeAssembly" by property "inGenomeAssembly", and "Chromosome" by property "inChromosome". "Gene" is connected to "RNATranscript" entities by property "hasRNATranscript". A list of all the properties for entities ["Gene"](https://datacommons.org/browser/Gene) and ["RNATranscript"](https://datacommons.org/browser/RNATranscript) can be viewed on the [Data Commons browser](https://datacommons.org/browser).

### Notes and Caveats

NCBI Gene includes gene annotations for viral, prokaryotic, and eukaryotic genomes. However, here we only include a the gene annotations from a subset of species. The raw data of additional gene annotations can be found in [this directory](https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/). In addition, UCSC Genome Browser contains an aggregate of publicly avaialble information primarily for human and mouse genomes, epigenomes, and transcriptomes. We include a susbest of the information avaialbe on the UCSC Genome Browser here pretaining to the gene and transcript annotation for select species. Additional data is available for download on their [browser](https://genome.ucsc.edu/goldenPath/help/ftp.html).

The formatting of the NCBI Gene files are different for mammal and non-mammal species. To account for these differences in the data processing seperate python notebooks were developed to handle mammal and non-mammal species. The appropriate notebook used to convert the data to mcf format should be determined by the species of interest.

### License
This data is from an NIH National Library of Medicine (NLM) genome unrestricted-access data repository and made accessible under the [NIH Genomic Data Sharing (GDS) Policy](https://osp.od.nih.gov/scientific-sharing/genomic-data-sharing/) and the [NLM Accessibility Policy](https://www.nlm.nih.gov/accessibility.html). Additional information on "NCBI Website and Data Usage Policies" can be found [here](https://www.ncbi.nlm.nih.gov/home/about/policies/).

As detailed by UCSC Genome Browser's [terms of use](https://genome.ucsc.edu/conditions.html), data from the UCSC Genome Browser is "free for academic, nonprofit, and personal use. A [license](https://genome.ucsc.edu/license/) is required for commercial download and installation of most Genome Browser binaries and source code, with some exceptions as detailed on the [license](https://genome.ucsc.edu/license/) page." UCSC Genome Browser states [how to cite](https://genome.ucsc.edu/cite.html) use of their data for journal publication.

### Dataset Documentation and Relevant Links

The database original documentation is accessible on [NCBI Assembly](https://www.ncbi.nlm.nih.gov/assembly/help/). NCBI assembly reports can be accessed using [FTP Download](https://ftp.ncbi.nlm.nih.gov/genomes/all/).

## About the import

### Artifacts

#### Python Notebooks

`format_mammal_gene_coordinates.ipynb`
`format_non_mammal_gene_coordinates.ipynb`

#### Files

`GCF_000001405.39_GRCh38.p13_assembly_report.txt`

### Import Procedure

#### Processing Steps 

To generate the data mcf from NCBI Gene reports and genome and transcriptome annotation from UCSC Genome Browser download and run the python notebooks. All of the files for import should be stored in the same directory and the filepath should be specified in the cell 3 `path` variable. The following variables should also be specified in cell 3 in accordance with the species and genome assembly being imported: `assembly`, `species`, and `species_abv`.