
import json



# The JSON data investigation
# File name
integer = 10383

# Loop through JSON FILES
while True:
  # If end of reading break
  if integer == 13488:
    break
  # Open JSON
  with open(f'D:\\Documents\\Sparta Global\\Data24_FinalProject\\datafiles\\Talent-{str(integer)}.json') as f:
    data = json.load(f)
    keys = data.keys()
    values = data.values()

    if data["name"] == "Eryn Speers":
        print(data["date"])
  integer += 1







