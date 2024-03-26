import tkinter as tk
from tkinter import filedialog

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

def upload_file(label):
    filename = filedialog.askopenfilename()
    if filename:
        label.config(text="Selected File {}".format(filename))
        with open(filename, 'r') as file:
            contents = file.read()
            print("Contents of File : \n{}".format(contents))
    else:
        label.config(text="No file selected")

def load_ui():
    # create the main window
    root = tk.Tk()
    root.title("Document Similarity")

    # Make the window full screen
    root.geometry("1400x7800")
    
    # labels to display the selected files
    label1 = tk.Label(root, text="No file selected")
    label1.pack(pady=10)
    button1 = tk.Button(root, text="Upload File 1", command=lambda: upload_file(label1))
    button1.pack(pady=5)
    label2 = tk.Label(root, text="No file selected")
    label2.pack(pady=10)
    button2 = tk.Button(root, text="Upload File 2", command=lambda: upload_file(label2))
    button2.pack(pady=5)
    
    percentage_label = tk.Label(root, text="0 %")
    percentage_label.pack(pady=5)
    label = tk.Label(root, text="")
    label.pack(pady=5)
    
    button = tk.Button(root, text="Check Similarity", command=lambda: display(0.343,percentage_label, label))
    button.pack(pady=5)
    
    return root

def main():
    root = load_ui()
    root.mainloop()
    
main()