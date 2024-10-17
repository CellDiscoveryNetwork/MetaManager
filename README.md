# HCA Metadata Manager

## Description
A set of google drive API functions in python for collecting and harmonizing experimental metadata. Input is a google sheet defined by a bionetwork or experimenter that defines metadata fields and allowed values for those fields. The functions here can help you generate empty metadata entry google sheets formatted with dropdown menus and descriptions of metadata to distribute to authors of experiments (as we do in the HCA). Also included are functions to help prefill these metadata entry sheets. 

## Installation

### Standard Installation
To install the package, run the following command:
```bash
pip install git+https://github.com/CellDiscoveryNetwork/HCA-MetaManager.git
```

### Development Installation
For developers who want to contribute to the package, clone the repository and then install it using:
```bash
git clone https://github.com/CellDiscoveryNetwork/MetaManager
cd MetaManager
pip install -e .
```

## Usage
We wrote a blog post on the CDN website with a code tutorial for how to use this. The code for the tutorial is here, in docs/metadata_collection_vignette.rmd
You can also see the tutorial on the github page: https://celldiscoverynetwork.github.io/MetaManager/metadata_collection_vignette.html

## Contributing
Reach out to kkimler@broadinstitute.org or any champions of HCA Bionetworks' integrated atlas projects to contribute!

## License
MIT License
