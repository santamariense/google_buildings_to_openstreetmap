#!/bin/bash

# Create "decompressed" directory if it doesn't exist
mkdir -p decompressed

# Loop over all .gz files in the current directory
for file in originals/*.gz; do
    if [ -f "$file" ]; then
        # Decompress each file
        gunzip -c "$file" > "decompressed/$(basename "$file" .gz)"
        echo "Decompressed $file to decompressed/$(basename "$file" .gz)"
    fi
done

