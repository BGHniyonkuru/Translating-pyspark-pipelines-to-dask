# cases/extract_pyspark.py
import json
import os
import glob

notebook_dir = "kaggle_notebooks"
output_dir = "extracted_scripts"

os.makedirs(output_dir, exist_ok=True)

for ipynb_file in glob.glob(f"{notebook_dir}/*.ipynb"):
    with open(ipynb_file, 'r') as f:
        notebook = json.load(f)
    
    # Filtre cellules code PySpark (contenant 'spark' ou 'pyspark')
    pyspark_code = []
    for cell in notebook['cells']:
        if cell['cell_type'] == 'code':
            source = ''.join(cell['source'])
            if 'spark' in source.lower() or 'pyspark' in source.lower():
                pyspark_code.append(source)
    
    if pyspark_code:
        # Nom du fichier output
        base_name = os.path.basename(ipynb_file).replace('.ipynb', '')
        output_file = f"{output_dir}/{base_name}_pyspark.py"
        
        with open(output_file, 'w') as f:
            f.write('# Extrait de : ' + ipynb_file + '\n\n')
            for code_block in pyspark_code:
                f.write(code_block + '\n\n')
        
        print(f"Extrait → {output_file}")

print("Extraction terminée !")