import pickle
import os

print(os.getcwd())  
# Replace 'file_path.pkl' with the path to your pickle file
file_path = os.getcwd() + '/evl_streaming_src/datasets/scargc_dataset_JITC_svm.pkl'

# Open the pickle file in read-binary mode
with open(file_path, 'rb') as file:
    data = pickle.load(file)

# Now, 'data' holds the contents of the pickle file
print(data)

# get unique values out of label column
unique_labels = data['label'].unique()

print(unique_labels)