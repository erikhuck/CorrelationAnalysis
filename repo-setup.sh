cd data/
ln -s /private/storage/bioinfstg3/2018/erikpetr/AD_SubTypes/AD_SubTypesCluster/prepared-data/adni/phenotypes-col-types.csv adnimerge-col-types.csv
ln -s /private/storage/bioinfstg3/2018/erikpetr/AD_SubTypes/AD_SubTypesCluster/prepared-data/adni/combined-col-types.csv col-types.csv
ln -s /private/storage/bioinfstg3/2018/erikpetr/AD_SubTypes/AD_SubTypesCluster/prepared-data/adni/combined.csv data.csv
cd ..
bash jobs/col-types.sh
bash jobs/debug-data.sh

IDX1=0
IDX2=9

for IDX in $(seq ${IDX1} ${IDX2})
do
    bash jobs/debug-col-comparison-dict.sh $IDX
done
