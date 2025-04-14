import pandas as pd
import numpy as np
import lightgbm as lgb
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, classification_report, roc_curve, auc


class TreeTrainer:
    def __init__(self, model_type: str='gbm'):
        pass
    
    def train(self, X_train, y_train):
        pass

class TreeInference:
    def __init__(self):
        pass

    def predict(self, X):
        pass