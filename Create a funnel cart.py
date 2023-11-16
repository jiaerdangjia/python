import matplotlib.pyplot as plt

# define the data for the funnel chart 
labels = ['Step1','Step2','Step3','Step4','Step5']
values = [100,75,50,30,10]

# calculate the cumulative values for plotting
cumulative_values = [sum(values[:i+1]) for i in range(len(values))]

# define colors for each segment
colors = ['blue','green','orange','red','purple']

# create teh funnel chart
fig, ax = plt.subplots()
for i in range(len(labels)):
    ax.fill_betweenx([i ,i+1],0, cumulative_values[i],step = 'mid',alpha = 0.7, color = colors[i])

ax.set_yticks(range(len(labels)))
ax.set_yticklabels(labels)
ax.set_xlabel('Conversion Rate')

# addlabels to the bars
for i, value in enumerate(cumulative_values):
    ax.annotate(str(value),xy = (value,i),xytext = (5,5),textcoords = 'offset points')

plt.title('Funnel Chart')
plt.show()
