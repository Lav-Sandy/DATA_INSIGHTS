# STREAMLIT APP for Data Exploration

Welcome to the Streamlit App for Data Exploration! This application provides a user-friendly interface for analyzing and comparing datasets. It includes multiple features that enable users to explore their data effectively.

## Features

- **Data Compare:** 
  - Compares two files using the DataCompy library and produces detailed output.
  - Built using Python and DataCompy.

- **Spark Compare:**
  - Compares two larger files efficiently using DataCompy and Apache Spark.

- **Profiling:**
  - Performs basic profiling by fetching total null columns, total NA or missing values, and their percentages.
  - Utilizes the Pandas Profiling utility for comprehensive exploratory data analysis (EDA).

- **EDA Tool:**
  - Conducts Exploratory Data Analysis with interactive visualizations using Plotly.

## Requirements

- Python 3.7 or higher
- Streamlit
- DataCompy
- Apache Spark (for Spark Compare functionality)
- Pandas Profiling
- Plotly
- Other dependencies listed in `requirements.txt`

Examples
Data Compare:

Upload two datasets to see differences in values, data types, and more.
Spark Compare:

Efficiently compare large files with Spark integration.
Profiling:

View total missing values and get detailed reports.
EDA Tool:

Generate interactive plots for visual data analysis.

Contributing
Contributions are welcome! If you have suggestions for improvements or want to report issues, please open an issue or submit a pull request.

Acknowledgments
Thanks to the DataCompy library for simplifying data comparisons.
Special thanks to the Pandas Profiling and Plotly communities for their valuable tools and resources.
Thanks to the Streamlit community for creating an amazing framework.

