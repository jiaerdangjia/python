import re

# Define a function to check if an item matches the pattern
def matches_pattern(item):
    return re.match(r'^[A-Z]', item)

# Filter the items in __builtins__ using the matches_pattern function
filtered_items = filter(matches_pattern, dir(__builtins__))

# Print the filtered items
for item in filtered_items:
    print(item)
