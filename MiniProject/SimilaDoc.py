# This is a Python application called "SimilaDoc" designed 
# to analyze the similarity between two text documents using MinHashLSH algorithm
# Written by Aryan Sai Arvapelly and M. Sai Gopal

import tkinter as tk
from tkinter import filedialog
from pyspark.sql import SparkSession
from pyspark.ml.feature import MinHashLSH
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col

def read_txt(file_path):
    with open(file_path, 'r') as file:
        text = file.read()
    return text

def give_label(jaccard_similarity):
    if jaccard_similarity == 0:
        return "100% Similarity"
    elif jaccard_similarity >= 0.8:
        return "High Similarity"
    elif 0.6 <= jaccard_similarity < 0.8:
        return "Moderate Similarity"
    elif 0.4 <= jaccard_similarity < 0.6:
        return "Low Similarity"
    elif 0.2 <= jaccard_similarity < 0.4:
        return "Very Low Similarity"
    else:
        return "No Similarity"

def display(value, percentage_label, label):
    percentage = value * 100
    percentage_label.config(text="{:.2f}%".format(percentage))
    similarity_label = give_label(value) 
    label.config(text=similarity_label)

def upload_file(label, file_var):
    filename = filedialog.askopenfilename()
    if filename:
        label.config(text="Selected File {}".format(filename))
        with open(filename, 'r') as file:
            contents = file.read()
            # print("Contents of File : \n{}".format(contents))
            file_var.set(contents)
    else:
        label.config(text="No file selected")
        file_var.set("")

def load_ui():
    # create the main window
    root = tk.Tk()
    root.title("SimilaDoc")

    root.geometry("800x480")

    title_label = tk.Label(root, text="SimilaDoc - Document Similarity Analysis", font=("Helvetica", 25, "bold"))
    title_label.pack(pady=20)
    
    # Variables to hold file contents
    file1_content = tk.StringVar()
    file2_content = tk.StringVar()
    
    # labels to display the selected files
    label1 = tk.Label(root, text="No file selected")
    label1.pack(pady=10)
    button1 = tk.Button(root, text="Upload File 1", command=lambda: upload_file(label1, file1_content))
    button1.pack(pady=5)
    label2 = tk.Label(root, text="No file selected")
    label2.pack(pady=10)
    button2 = tk.Button(root, text="Upload File 2", command=lambda: upload_file(label2, file2_content))
    button2.pack(pady=5)
    
    percentage_label = tk.Label(root, text="0 %")
    percentage_label.pack(pady=5)
    label = tk.Label(root, text="")
    label.pack(pady=5)
    
    button = tk.Button(root, text="Check Similarity", command=lambda: calculate_similarity(root,file1_content.get(), file2_content.get(), percentage_label, label))
    button.pack(pady=5)
    
    return root

def calculate_similarity(root, text1, text2, percentage_label, label):
    
    label.config(text="Performing similarity analysis...")
    root.update()
    
    if text1 and text2:
        spark = SparkSession.builder.appName("Similarity").getOrCreate()

        # convert strings to sets of shingles
        shingle_size = 3
        shingles1 = set(text1[i:i + shingle_size] for i in range(len(text1)-shingle_size-1))
        shingles2 = set(text2[i:i + shingle_size] for i in range(len(text2)-shingle_size-1))

        # create DataFrame with shingles
        data = [(0, Vectors.dense([int(shingle in shingles1) for shingle in shingles1])),
                (1, Vectors.dense([int(shingle in shingles2) for shingle in shingles2]))]

        df = spark.createDataFrame(data, ["id", "features"])

        # minHashing
        mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=10)
        model = mh.fit(df)
        hashed_df = model.transform(df)

        # Locality Sensitive Hashing
        # approxSimilarityJoin- Join two datasets to approximately find all pairs of rows whose distance
        # are smaller than the threshold.
        similar_pairs = model.approxSimilarityJoin(hashed_df, hashed_df, 0.9, distCol="JaccardDistance") \
            .filter(col("datasetA.id") < col("datasetB.id")) \
            .select(col("datasetA.id").alias("id1"),
                    col("datasetB.id").alias("id2"),
                    col("JaccardDistance"))


        # Extract Jaccard distance values
        jaccard_distance_values = similar_pairs.select("JaccardDistance").rdd.map(lambda row: row[0]).collect()

        if jaccard_distance_values:
            similarity_value = jaccard_distance_values[0]
            display(similarity_value, percentage_label, label)
        else:
            label.config(text="No similarity found")

        spark.stop()
    else:
        label.config(text="Please upload both files")

def main():
    root = load_ui()
    root.mainloop()

main()