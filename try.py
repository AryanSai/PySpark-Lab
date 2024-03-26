def interpret_similarity(jaccard_similarity):
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

# Example usage:
similarity_value = 0.72
label = interpret_similarity(similarity_value)
print("Jaccard Similarity Label:", label)
