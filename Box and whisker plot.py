import matplotlib.pyplot as plt

# Sample data
data = [25,30,25,40,45,50,55,60,65,70,75]

# Create a box and whisker plot with custom colors
box_color = 'dodgerblue'
median_color = 'red'
whisker_color = 'green'
flier_color = 'purple'

plt.boxplot(data,boxprops={'color': box_color},medianprops={'color':median_color},whiskerprops={'color':whisker_color},flierprops={'markerfacecolor':flier_color})

# Add labels and title
plt.xlabel('Data')
plt.ylabel('Value')
plt.title('Colored Box and whisker plot')

# Display the plot
plt.show()