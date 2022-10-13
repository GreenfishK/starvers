docker run \
    -it \
    --rm \
    -v ${datasetdir}:/var/data/dataset/ \
    -v ${querydir}:/var/data/queries/ \
    -v ${outputdir}:/var/data/output/ \
    bear-rdfstarstores \
    java -cp target/rdfstoreQuery-0.8.jar org/ai/wu/ac/at/rdfstarArchive/tools/GenerateQueries \
        -w ~/Uni/PhD/Semantic_Technologies/RDFVersioningEvaluation/rdfostrich_BEAR/data/queries/final_queries/IC/queries_beara/high \
        -i ts \
        -o ic \
        -v 1299 \

#-r ~/Uni/PhD/Semantic_Technologies/RDFVersioningEvaluation/rdfostrich_BEAR/data/queries/raw_queries/queries_beara/high/lookup_queries_o_high.txt 
