import pandas as pd
import glob

files = glob.glob('data/raw/youtube/*.csv')
files = [f for f in files if 'all_keywords' not in f]
df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
df.to_csv('data/raw/youtube/all_keywords.csv', index=False)
print('done:', len(df), 'rows')
