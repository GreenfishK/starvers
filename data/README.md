# Data Acquisition
Create a directory .BEAR in your home directory and copy the content of this data directory including all sub-directories into ~/.BEAR . Give all user groups full permission (read, write, execute) to this directory and all subdirectories (chmod 777). 

## Datasets
### Download and prepare BEAR-B datasets
* Download the BEAR-B hour-datasets from the [BEAR webpage](https://aic.ai.wu.ac.at/qadlod/bear.html) (4 files in section 3.BEAR-B/description of the dataset/get the dataset) and copy them into ~/home/.BEAR/rawdata/bearb/hour. If you followed the steps, this directory should exist by now.
* Extract the datasets from their .gz packages. Also extract all sub .gz packages.
* Rename allData.nq to alldata.TB.nq.

#### Option 1: Create RDF-star datasets from BERA-B ICs and CBs
Create the alldata.TB_star_flat.ttl  and alldata_TB_star_hierarchical.ttl datasets by using the script provided [here](https://github.com/GreenfishK/BEAR/blob/master/scripts/build_tb_rdf_star_datasets.py). This script first computes changesets from the ICs and stores them into the alldata.CB_computed.nt directory. From the initial IC alldata.IC.nt/000001.nt and the change sets it creates two single-filed RDF*-based TB dataset. These datasets include all of the 1299 versions where each version is a set of triples with the same creation timestamp (valid_from). The timestamps do not reflect the original timestamps but are artificially constructed with one second difference between each version. Triples that were deleted and therefore not existent in a specific version anymore are annotated with a deletion timestamp (valid_until).

#### Option 2: Download RDF-star datasets from Zenodo
Download the alldata_TB_star_hierarchical.ttl and alldata_TB_star_flat.ttl datasets from [![Zenodo](https://zenodo.org/badge/DOI/10.5281/zenodo.5877503.svg)](https://doi.org/10.5281/zenodo.5877503) and copy them into ~/home/.BEAR/rawdata/bearb/hour.


Following datasets were not used from this framework:
* BEAR-A
* BEAR-B day
* BEAR-B instant
* BEAR-C

## Queries
Queries are already provided in the [queries_bearb](https://github.com/GreenfishK/BEAR/tree/master/data/queries/queries_bearb) folder. 
See [readme](https://github.com/GreenfishK/BEAR/tree/master/data/queries/queries_bearb/README.md)

