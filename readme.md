## Activate environment for entrez and smorf dependencies
``` source env_marine/bin/activate ``` 

## Download prokaryotic genome with filter "marine" in isolation source
``` bash download_marine_prokaryotes_fna "marine" 10 ```

## Run smORFinder on genome
### On one genome
```  smorf single marine_prokaryote_fna/GCF_016622455.1_ASM1662245v1_genomic.fna -o new ```
### On all genomes
```bash
./run_smorf_all.sh                     # uses ./marine_prokaryote_fna -> ./smorf_output
# or override directories:
# ./run_smorf_all.sh /path/to/fna /path/to/output_root
```
