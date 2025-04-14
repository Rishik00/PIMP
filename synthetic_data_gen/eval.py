import nltk
import mmap
import math, time
from collections import Counter

from sklearn.feature_extraction.text import TfidfVectorizer
from typing import List

nltk.download('punkt')

class SyntheticURLEvaluator:
    def __init__(self, original: str, synthetic: List[str]):
        pass

    def levenshtein(self):
        pass

    def cosine_similarity(self):
        pass

    def custom_similarity(self):
        pass

    def cross_entropy(self):
        pass

    def log_metrics(self):
        pass


def output_parser(ifile: str):
    
    with open(ifile, 'r') as f:
        for line in f[:10]:
            print(line)
