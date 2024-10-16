# Quantum SVM Classification for Heart Disease Detection

This repository contains code for implementing and comparing various quantum embedding methods alongside classical Support Vector Machine (SVM) classification for heart disease detection.

## Introduction

Heart disease is a prevalent health condition globally, and early detection is crucial for effective treatment. In this project, we explore the potential of quantum computing techniques in enhancing the accuracy and efficiency of heart disease detection using machine learning algorithms.

## Dataset

I utilize the Heart Disease dataset, which includes features such as general health status, age, cholesterol levels, and more, along with the presence or absence of heart disease or heart attack.

## Quantum Embedding Methods

We investigate four quantum embedding methods:
- Angle Embedding
- Amplitude Embedding
- Basis Embedding
- IQP Embedding

These methods encode the dataset into quantum states, which are then used to compute kernel matrices for SVM classification.

## SVM Classification

Support Vector Machine (SVM) classifiers are trained using the computed kernel matrices. SVM is a powerful algorithm for binary classification tasks, known for its ability to find optimal hyperplanes that separate different classes.

