#!/bin/bash

# Check if a directory argument is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <directory>"
    exit 1
fi

# Assign the directory argument to a variable
input_dir="$1"

# Step 1: Run metaphlan for each pair of forward and reverse files
for file in "${input_dir}"/*.1.fastq.gz; do
    if [ -e "$file" ]; then
        sample_name=$(basename "$file" .1.fastq.gz)
        merged_file="${input_dir}/${sample_name}.fastq"
        zcat "${input_dir}/${sample_name}.1.fastq.gz" "${input_dir}/${sample_name}.2.fastq.gz" > "$merged_file"
        mkdir -p "${input_dir}/bowtie2" "${input_dir}/sams"
        metaphlan "$merged_file" --input_type fastq -s "${input_dir}/sams/${sample_name}.sam.bz2" --bowtie2out "${input_dir}/bowtie2/${sample_name}.bowtie2.bz2" -o "${input_dir}/${sample_name}_profiled.tsv"
        rm "$merged_file"
    fi
done

# Step 2: Generate consensus marker
mkdir -p "${input_dir}/consensus_marker"
sample2markers.py -i "${input_dir}/sams"/*.sam.bz2 -o "${input_dir}/consensus_marker" -n 25

# Step 3: Print clades for further process
strainphlan -s "${input_dir}/consensus_marker"/*.pkl -o "${input_dir}/consensus_marker" --print_clades_only --marker_in_n_samples 10 -d /home/omprakash/miniconda3/envs/mpa3/lib/python3.6/site-packages/metaphlan/metaphlan_databases/*.pkl > "${input_dir}/clades.txt"

# Extracting clades info from text file
grep -o "s__[^:]*" "${input_dir}/clades.txt" > "${input_dir}/new_clades.txt"
mv "${input_dir}/clades.txt" "${input_dir}/old_clades.txt"
mv "${input_dir}/new_clades.txt" "${input_dir}/clades.txt"

# Step 4: Extract markers for each clade
mkdir -p "${input_dir}/db_marker"
mkdir -p "${input_dir}/output"
while IFS= read -r clade; do
    new_clade_name=${clade#*__}
    extract_markers.py -c "$clade" -o "${input_dir}/db_marker/" -d /home/omprakash/miniconda3/envs/mpa3/lib/python3.6/site-packages/metaphlan/metaphlan_databases/*.pkl
    strainphlan -s "${input_dir}/consensus_marker"/*.pkl -m "${input_dir}/db_marker/${clade}.fna" --marker_in_n_samples 10 -o output -n 25 -c "$new_clade_name" --mutation_rates -d /home/omprakash/miniconda3/envs/mpa3/lib/python3.6/site-packages/metaphlan/metaphlan_databases/
done < "${input_dir}/clades.txt"
