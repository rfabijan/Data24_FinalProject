import os
import pandas as pd
import json
from pprint import pprint as pp

# Create the directory if it does not exist
save_to_dir = '../../datafiles'
combined_path = f'{save_to_dir}/combined'
if not os.path.exists(combined_path):
    os.makedirs(combined_path)

# Go through the downloaded files and sort accordingly
subdirectory = {
    'Academy': [],
    'Talent': {
        'Applicants': [],
        'SpartaDay': [],
        'Trainee': []
    }
}

for filename in os.listdir(save_to_dir):
    path = f'{save_to_dir}/{filename}'

    if filename.startswith('Academy'):
        # subdirectory['Academy'].append(pd.read_csv(path))
        df = pd.read_csv(path)
        df['course'] = filename.split('-')[1][:-5].replace('_', ' ')
        df['date'] = filename.split('_')[-1].split('.')[0]
        subdirectory['Academy'].append(df)

    elif filename.startswith('Talent'):
        if filename.endswith('Applicants.csv'):
            df = pd.read_csv(path)
            date = list(filename.split('-')[1].split('Appl')[0])
            date.insert(-4, ' ')
            date = "".join(date)
            df['date'] = date
            subdirectory['Talent']['Applicants'].append(df)

        # elif filename.endswith('.json'):
        #     with open(path) as json_data:
        #         data = json.load(json_data)
        #
        #     pp(data)

            # df = pd.read_json(path)
            # appl_id = filename.split('-')[-1]
            # print(appl_id)


# Now combine files:
pd.concat(subdirectory['Academy'], ignore_index=True).to_csv(f'{combined_path}/Academy.csv', index=False)
pd.concat(subdirectory['Talent']['Applicants'], ignore_index=True).to_csv(f'{combined_path}/TalentApplicants.csv', index=False)

