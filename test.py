import pandas as pd
from datetime import datetime, timedelta
import os
import sys

def process_logs(input_dir, output_dir, target_date_str):
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    start_date = target_date - timedelta(days=6)
    user_actions = {}
    
    for file_name in os.listdir(input_dir):
        if file_name.endswith(".csv"):
            file_path = os.path.join(input_dir, file_name)
            df = pd.read_csv(file_path)

            df['dt'] = pd.to_datetime(df['dt'])
            df_filtered = df[(df['dt'] >= start_date) & (df['dt'] <= target_date)]
            
            grouped = df_filtered.groupby(['email', 'action']).size().unstack(fill_value=0)
            
            grouped = grouped.rename(columns={
                'CREATE': 'create_count',
                'READ': 'read_count',
                'UPDATE': 'update_count',
                'DELETE': 'delete_count'
            })
            
            grouped = grouped.reindex(columns=['create_count', 'read_count', 'update_count', 'delete_count'], fill_value=0)
            
            for email, actions in grouped.iterrows():
                if email not in user_actions:
                    user_actions[email] = {'create_count': 0, 'read_count': 0, 'update_count': 0, 'delete_count': 0}
                
                for action in ['create_count', 'read_count', 'update_count', 'delete_count']:
                    user_actions[email][action] += actions[action]

    output_file_path = os.path.join(output_dir, f"{target_date_str}.csv")
    result_df = pd.DataFrame.from_dict(user_actions, orient='index')
    result_df.to_csv(output_file_path, index_label='email')

    print(f"Сохранено в: {output_file_path}")

if len(sys.argv) != 2:
    print("Дата должна быть в формате: python script.py <YYYY-mm-dd>")
    sys.exit(1)

target_date_str = sys.argv[1]

input_dir = "input"
output_dir = "output"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

process_logs(input_dir, output_dir, target_date_str)
