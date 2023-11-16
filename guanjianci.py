import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import pandas as pd
import matplotlib

# 设置 Matplotlib 使用的字体，确保可以显示中文
# 这里以“微软雅黑”为例，您可以替换为您系统中的其他字体
matplotlib.rcParams['font.sans-serif'] = ['Microsoft YaHei']

# 现在您可以继续您的绘图代码


# Load your data
keywords_data = pd.read_excel(r'C:\Users\mille\OneDrive\桌面\keywords.xlsx')  # Replace with the path to your file

# Splitting the keywords and counting their occurrences
keywords = keywords_data['关键词'].dropna()
keyword_list = [word.strip() for sublist in keywords.str.split(',') for word in sublist]
keyword_counts = Counter(keyword_list)

# Selecting the top 20 most common keywords for visualization
top_keywords = dict(keyword_counts.most_common(20))

# Creating a bar plot for the top keywords
plt.figure(figsize=(12, 8))
sns.barplot(x=list(top_keywords.values()), y=list(top_keywords.keys()))
plt.title('Top 20 Most Common Keywords')
plt.xlabel('Frequency')
plt.ylabel('Keywords')
plt.show()

