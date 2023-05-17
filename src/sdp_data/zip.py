import os
import py7zr

def create_7z_archive(source_dir, output_file):
     with py7zr.SevenZipFile(output_file, 'w') as archive:
        archive.writeall(source_dir)

# Usage example
source_directory = "data/_raw"
output_archive = "data/_raw.7z"
create_7z_archive(source_directory, output_archive)