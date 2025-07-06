import matplotlib.pyplot as plt

def plot_bar_chart(data, x_column, y_column, chart_title):
    """
    This function takes data in the form of a list of dictionaries,
    and plots a bar chart based on the specified x and y columns.

    Args:
        data (list of dict): Data to plot, each dictionary represents a data point.
        x_column (str): The key for the data on the x-axis (e.g., 'year').
        y_column (str): The key for the data on the y-axis (e.g., 'total_works_count_per_year').
        chart_title (str): The title of the chart.
    """
    # Extract x and y values
    x_values = [item[x_column] for item in data]
    y_values = [item[y_column] for item in data]

    # Create the bar chart
    plt.bar(x_values, y_values)

    # Add labels and title
    plt.xlabel(x_column)
    plt.ylabel(y_column)
    plt.title(chart_title)

    # Display the chart
    plt.show()