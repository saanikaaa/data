# Importing NCBI Assembly Summary Report

## Table of Contents

1. [About the Dataset](#about-the-dataset)
    1. [Download URL](#download-url)
    2. [Database Overview](#database-overview)
    3. [Schema Overview](#schema-overview)
       1. [dcid Generation](#dcid-generation)
       2. [Edges)(#edges)
    4. [Notes and Caveats](#notes-and-caveats)
    5. [License](#license)
    6. [Dataset Documentation and Relevant Links](#dataset-documentation-and-relevant-links)
2. [About the Import](#about-the-import)
    1. [Artifacts](#artifacts)
    2. [Import Procedure](#import-procedure)
    3. [Test](#test)


## About the Dataset

"NCBI provides stable accessions and data tracking for genome assemblies. It stores the names and identifiers for the sequences in each genome assembly as well as the associated metadata (such as assembly name, date of submission, name of submitter, details of sequenced organism), assembly statistics, and the organization of its component sequences into scaffolds and chromosomes. NCBI uses the Genome Reference Consortium (GRC) data model, reflecting the complexity of the modern genome assembly. The data model accounts for all sequences known to represent an organism’s genome, including those that are not yet assigned to chromosome assemblies. Assemblies may be of different levels (contig level, scaffold level, chromosome level, or complete genomes).

All assemblies contain a unit termed “primary assembly.” This includes non-redundant sequences (chromosomes and/or scaffolds) that represent an organism’s haploid genome. Additional assembly units that may be included are organelle genomes (mitochondria, plastids), alternate loci (sequences aligned to the primary assembly that provide alternate representations of corresponding loci found in the primary assembly), and genome patches that are sequences representing assembly updates."

### Download URL

1. [assembly_summary_genbank.txt](https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt).
2. [assembly_summary_refseq.txt](https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt).

### Database Overview

"The [NCBI Assembly database](www.ncbi.nlm.nih.gov/assembly/) provides stable accessioning and data tracking for genome assembly data. The model underlying the database can accommodate a range of assembly structures, including sets of unordered contig or scaffold sequences, bacterial genomes consisting of a single complete chromosome, or complex structures such as a human genome with modeled allelic variation. The database provides an assembly accession and version to unambiguously identify the set of sequences that make up a particular version of an assembly, and tracks changes to updated genome assemblies. The Assembly database reports metadata such as assembly names, simple statistical reports of the assembly (number of contigs and scaffolds, contiguity metrics such as contig N50, total sequence length and total gap length) as well as the assembly update history. The Assembly database also tracks the relationship between an assembly submitted to the International Nucleotide Sequence Database Consortium (INSDC) and the assembly represented in the NCBI RefSeq project" (Kitts et al. 2016).

In this import we include the metadata for all genome assemblies documented in `assembly_summary_genbank.txt` and `assembly_summary_refseq.txt`. Assemblies are stored in GenomeAssembly nodes whose information is integrated from both the GenBank and RefSeq datasets.

### Schema Overview

#### New Schema

##### Properties

* BiologicalEntity
    * biologicalIsolate
    * breed
    * cultivar
    * ecotype
    * infraspecificName
    * strain
* GenomeAnnotation
    *  geneCount
    *  geneticScaffoldCount
    *  gcContent
    *  nonCodingGeneCount
    *  proteinCodingGeneCount
* GenomeAssembly
    * contigCount
    * genBankNucleotideAccession
    * geneticRepliconCount
    * geneticScaffoldingCount
    * geneticScaffoldingCount
    * genomeAnnotatedBy
    * genomeAssemblyDerivedFrom
    * genomeSize
    * genomeSizeUngapped
* Taxon
    * ofStrain 
* Thing
    * isLatestVersion  

##### Enumerations

* BiologicalTaxonomyGroupEnum
    * BiologicalTaxonomyGroupArchaea
    * BiologicalTaxonomyGroupBacteria
    * BiologicalTaxonomyGroupFungi
    * BiologicalTaxonomyGroupInvertebrate
    * BiologicalTaxonomyGroupMetagenomes
    * BiologicalTaxonomyGroupOther
    * BiologicalTaxonomyGroupPlant
    * BiologicalTaxonomyGroupProtozoa
    * BiologicalTaxonomyGroupVertebrateMammalian
    * BiologicalTaxonomyGroupVertebrateOther
    * BiologicalTaxonomyGroupViral
* GenomeAssemblyDerivedFromEnum
    * GenomeAssemblyDerivedFromEnum
    * GenomeAssemblyDerivedFromTypeMaterial
    * GenomeAssemblyDerivedFromSynonymTypeMaterial
    * GenomeAssemblyDerivedFromPathotypeMaterial
    * GenomeAssemblyDerivedFromCladeExemplar
    * GenomeAssemblyDerivedFromNeotype
    * GenomeAssemblyDerivedFromReftype
    * GenomeAssemblyDerivedFromIctvSpeciesExemplar
    * GenomeAssemblyDerivedFromIctvAdditionalIsolate

#### dcid Generation

The data from this import was stored as GenomeAssembly entities. The dcid for these entites are generated by adding the prefix 'bio/' to the GenBank accession. We used GenBank accessions to generate the dcid because they are more comprehensive in terms of the genome assemblies represented. RefSeq only includes a subset of assemblies that are represented in GenBank and there are a whole host of [reasons why an assembly may be excluded from RefSeq](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/policies-annotation/genome-processing/genome_notes/). RefSeq accessions were mapped to the corresponding entry in GenBank and this information was stored in the "refSeqAccession" property and this data was preferentially retrieved from the `assembly_summary_refseq.txt` file.

Quantity nodes were generated to represent the genome size, genome size ungapped, and GC content properties. Quantity node dcids are generated by the unit of measurement in pascal case followed directly by the quantity. For example the dcid for a Quantity node represented 1000 base pairs would be "BasePairs1000". The unit of measurement for genome size and genome size ungapped are both Base Pairs, whereas the unit of measurement for gc content is percent.

#### Edges

GenomeAssembly nodes are linked to corresponding Taxon nodes. This is done through the properties "ofSpecies" and "ofStrain", which indicate the corresponding nodes representing species or virus strain - when appropriate - from which the genome assembly was derived. The dcids for these links were generated by converting the "species_tax_id" - and when distinct from the "species_tax_id" the "tax_id" as well - to the corresponding Taxon dcid using the `tax_id_dcid_mapping.txt` file that contains the key for converting NCBI Tax IDs to corresponding dcids and is generated by the NCBI Taxonomy import.

In addition, the following properties for GenomeAssemblies contain links to corresponding Quantity nodes: "genomeSize", "genomeSizeUngapped", and "gcContent".

### Notes and Caveats

When reprocessing this dataset it must be run on the same day as the NCBI Taxonomy import because this import is reliant on the `tax_id_dcid_mapping.txt` generated by the NCBI Taxonomy import. This dataset and NCBI Taxonomy are both updated daily.  Therefore, these must be run on the same day to ensure that this mapping file used for this import is up to date. Furtheremore, in the infraspecific name field contains information on breed, cultivar, ecotype, and strain in addition to the infraspecific name. In the cases of these additional variables they are noted by the property followed by '=' (e.g. 'breed=', 'cultivar=', etc). Therefore, these properties were seperated out into their own column in which the value is the corresponding string stripped of the property (e.g. "ecotype=Kro-0" is assigned to the new ecotype column with the value "Kro-0").

### License

This data is from an NIH human genome unrestricted-access data repository and made accessible under the [NIH Genomic Data Sharing (GDS) Policy](https://osp.od.nih.gov/scientific-sharing/genomic-data-sharing/).

### Dataset Documentation and Relevant Links

The NCBI Assembly resource is described in [Kitts et al. 2016](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4702866/). More about the NCBI Genome Assembly data model can be found [here](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/policies-annotation/data-model/). Please note that effective June 2024 NCBI Datasets replaces legacy NCBI Genome and Assembly web resources. More about this transition can be found [here](https://ncbiinsights.ncbi.nlm.nih.gov/2023/10/18/ncbi-datasets-access-sequence-data/). Data can still be accessed through the [Genomes FTP site](https://ftp.ncbi.nlm.nih.gov/genomes/). A glossary of terms used by NCBI Assembly can be found [here](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/glossary/).

## About the import

### Artifacts

#### Scripts

##### Bash Scripts

- [download.sh](scripts/download.sh) downloads the most recent release of the NCBI Assembly Summary Report data.
- [run.sh](scripts/run.sh) creates ncbi_assembly_summary.csv.
- [tests.sh](scripts/tests.sh) runs standard tests to check for proper formatting.

##### Python Scripts

- [process.py](scripts/process.py) creates the NCBI Assembly Summary Report formatted CSV files.
- [process_test.py](scripts/process_test.py) unittest script to test standard test cases on NCBI Assembly Summary Report.

#### tMCFs

- [ncbi_assembly_summary_report.tmcf](tMCF/ncbi_assembly_summary_report.tmcf) contains the tmcf mapping to the csv of NCBI Assembly Summary Report.



### Import Procedure

Download the most recent versions of NCBI Assembly Summary Report data:

```bash
sh download.sh
```

Generate the formatted CSV:

```bash
sh run.sh
```


### Test 

To run tests:

```bash
sh tests.sh
```