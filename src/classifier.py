from transformers import pipeline

# Load the zero-shot classification model
classifier = pipeline("zero-shot-classification","facebook/bart-large-mnli")

# Define your categories
categories = ["terrorism / riot /violence", "uplifting / positive", "natural disaster", "politics", "others"]

def classify(input):
    prediction = classifier(input, candidate_labels=categories)
    return prediction