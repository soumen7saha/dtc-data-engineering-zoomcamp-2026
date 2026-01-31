import sys
import pandas as pd

# print(f'arguments: {sys.argv}')
month = int(sys.argv[1])
print(f'month={month}')

df = pd.DataFrame({'day': [1,2], 'num_passengers':[3,4]})
df['month'] = month
print(df)

df.to_parquet(f'output_{month}.parquet')