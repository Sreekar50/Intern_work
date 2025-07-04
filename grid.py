import os
import pandas as pd
import requests
import gzip
import shutil
import subprocess
import concurrent.futures

base_dir = os.getcwd()
PROCESSED_SAMPLES_FILE = "processed_samples.txt"

LAST_WORKED_SAMPLES_COUNT = 5

# Function to get the URLs of .fastq.gz files for a given accession ID using the ENA API.
def get_fastq_urls(accession_id):
    ena_api_base_url = "https://www.ebi.ac.uk/ena/portal/api/search?"
    params = {
        'result': 'read_run',
        'query': f'accession={accession_id}',
        'fields': 'fastq_ftp'
    }
    response = requests.get(ena_api_base_url, params=params)
    if response.status_code == 200:
        data = response.text.strip().split('\n')
        if len(data) > 1:
            fastq_ftp_field = data[1].split('\t')[0]
            fastq_urls = fastq_ftp_field.split(';')
            fastq_urls = [f"https://{url}" for url in fastq_urls]  # Add https:// prefix
            fastq_files = [url.split('/')[-1] for url in fastq_urls]  # Extract file names
            return list(zip(fastq_files, fastq_urls))
    else:
        print(f"Failed to get fastq URLs for {accession_id}: {response.status_code}")
    return []

# Function to download a .fastq.gz file from a given URL and save to the specified path.
def download_fastq(fastq_url, download_path):
    file_name = fastq_url.split('/')[-1]
    gz_path = os.path.join(download_path, file_name)
        
    # Check if the file already exists
    if os.path.exists(gz_path):
        print(f"File {file_name} already exists in {download_path}, skipping download.")
        return
        
    response = requests.get(fastq_url)
    if response.status_code == 200:
        with open(gz_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded {file_name} to {gz_path}")
    else:
        print(f"Failed to download {fastq_url}: {response.status_code}")

# Function to clean and concatenate fastq.gz files for a single sample.
def clean_and_concatenate_sample(sample_id, sample_files, output_file):
    with open(output_file, 'w') as outfile:
        for input_file in sample_files:
            print(f"Processing {input_file}...")
            with gzip.open(input_file, 'rt') as infile:
                complete = True
                header, seq, plus, qual = '', '', '', ''
                for i, line in enumerate(infile):
                    if i % 4 == 0:
                        if not complete:
                            print("Incomplete sequence found and removed.")
                        header = line.strip()
                        complete = False
                    elif i % 4 == 1:
                        seq = line.strip()
                    elif i % 4 == 2:
                        plus = line.strip()
                    elif i % 4 == 3:
                        qual = line.strip()
                        complete = True
                        if len(seq) == len(qual):
                            outfile.write(f"{header}\n{seq}\n{plus}\n{qual}\n")
                        else:
                            print("Mismatched sequence and quality score lengths found and removed.")
                if not complete:
                    print("Incomplete sequence found and removed.")
            print(f"Finished processing {input_file}, output saved to {output_file}.")
    return output_file

# Function to process a single sample
def process_sample(sample_data, study_dir, merged_dir):
    sample_id, accession_ids, sample_col, accession_col = sample_data
    sample_folder = os.path.join(study_dir, "samples")
    sample_dir = os.path.join(sample_folder, sample_id)
    single_files_dir = os.path.join(sample_dir, 'single_files')
    paired_files_dir = os.path.join(sample_dir, 'paired_files')
    os.makedirs(single_files_dir, exist_ok=True)
    os.makedirs(paired_files_dir, exist_ok=True)
    save_processed_sample(sample_id)
    fastq_files = []

    for accession_id in accession_ids.split(';'):
        accession_id = accession_id.strip()
        if accession_id:
            print(f"Processing {accession_id} in {sample_dir}")
            fastq_data = get_fastq_urls(accession_id)

            paired_urls = []
            single_urls = []

            # Logic to handle the specific conditions
            if len(fastq_data) == 3:
                for file_name, fastq_url in fastq_data:
                    if f"{accession_id}.fastq.gz" not in file_name:
                        paired_urls.append(fastq_url)
            elif len(fastq_data)==2:
                paired_urls.append(fastq_data[0][1])
                paired_urls.append(fastq_data[1][1])
            elif len(fastq_data) == 1:
                single_urls.append(fastq_data[0][1])

            for fastq_url in paired_urls:
                download_fastq(fastq_url, paired_files_dir)
                fastq_files.append(os.path.join(paired_files_dir, fastq_url.split('/')[-1]))

            for fastq_url in single_urls:
                download_fastq(fastq_url, single_files_dir)
                fastq_files.append(os.path.join(single_files_dir, fastq_url.split('/')[-1]))

    output_file = os.path.join(merged_dir, f"{sample_id}.fastq")
    if os.path.exists(output_file):
        print(f"Output file for {sample_id} already exists, skipping concatenation.")
    else:
        clean_and_concatenate_sample(sample_id, fastq_files, output_file)

    return sample_id

# Function to execute GRiD on the merged folder
def execute_grid(grid_dir, merged_dir, study_name):
    grid_command = (
        f"grid multiplex -r {merged_dir} "
        f"-d /storage32Tb/sourav/CMPA/GRiD-1.3/Stool/ -p -m -c 0.2 "
        f"-o {grid_dir} -n 10 "
    )
    print(f"Executing GRiD command: {grid_command}")                       
    subprocess.run(grid_command, shell=True)

# Function to load processed samples from file
def load_processed_samples():
    with open(PROCESSED_SAMPLES_FILE, 'r') as file:
        return [line.strip() for line in file.readlines()]

# Function to save processed samples to file
def save_processed_sample(sample_id):
    with open(PROCESSED_SAMPLES_FILE, 'a') as file:
        file.write(f"{sample_id}\n")

# Function to delete last N worked samples
def delete_last_n_samples(study_dir, merged_dir, n):
    processed_samples = load_processed_samples()
    if len(processed_samples) >= n:
        last_n_samples = processed_samples[-n:]
        for sample_id in last_n_samples:
            sample_folder = os.path.join(study_dir, "samples", sample_id)
            if os.path.exists(sample_folder):
                shutil.rmtree(sample_folder)
            output_file = os.path.join(merged_dir, f"{sample_id}.fastq")
            if os.path.exists(output_file):
                os.remove(output_file)
        # Remove the last n samples from the processed samples file
        with open(PROCESSED_SAMPLES_FILE, 'w') as file:
            file.writelines([sample + "\n" for sample in processed_samples[:-n]])
        return last_n_samples
    return []

# Function to clear the processed_samples.txt file
def clear_processed_samples_file():
    open(PROCESSED_SAMPLES_FILE, 'w').close()
    print("Cleared the processed_samples.txt file.")

# Main script to process all samples in the study.
def main():
    excel_path = 'workflow_testing.xlsx'  # Path to the uploaded Excel file

    if not os.path.exists(excel_path):
        print(f"Error: The file {excel_path} does not exist.")
        return

    df = pd.read_excel(excel_path)

    study_col = df.columns[1]  # Study names in column 2 (index 1)
    sample_col = df.columns[0]  # Sample IDs in column 1 (index 0)
    accession_col = df.columns[30]  # Accession IDs in column 31 (index 30)
    study_name = df.iloc[1, 1]

    study_folder = os.path.join(base_dir, "output")
    os.makedirs(study_folder, exist_ok=True)
    study_dir = os.path.join(study_folder, study_name)
    os.makedirs(study_dir, exist_ok=True)
    merged_dir = os.path.join(study_dir, f"{study_name}_merged")
    os.makedirs(merged_dir, exist_ok=True)
    grid_dir = os.path.join(study_dir, f"{study_name}_GRiD")
    os.makedirs(grid_dir, exist_ok=True)

    # Delete last 5 recently worked samples
    last_worked_samples = delete_last_n_samples(study_dir, merged_dir, LAST_WORKED_SAMPLES_COUNT)

    processed_samples = load_processed_samples()
    sample_data_list = [
        (row[sample_col], str(row[accession_col]), sample_col, accession_col) 
        for idx, row in df.iterrows()
        if row[sample_col] not in processed_samples or row[sample_col] in last_worked_samples
    ]

    # Process samples concurrently, 5 at a time
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_sample, sample_data, study_dir, merged_dir) for sample_data in sample_data_list]
        for future in concurrent.futures.as_completed(futures):
            sample_id = future.result()
    
    # Execute GRiD on the merged folder
    execute_grid(grid_dir, merged_dir, study_name)

    # Clear the processed_samples.txt file after execution
    clear_processed_samples_file()

if __name__ == "__main__":
    main()
