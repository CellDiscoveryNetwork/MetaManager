import matplotlib.pyplot as plt
import seaborn as sns
import re
import pandas as pd
import numpy as np

def check_completeness(df, ignore_values=None):
    """
    Calculate the percentage of non-missing values for each column in a DataFrame,
    ignoring specified 'empty' values. 
    
    Parameters:
        df (pd.DataFrame): The dataframe to analyze.
        ignore_values (list, optional): A list of values to ignore as 'empty' in the completeness calculation.
        
    Returns:
        pd.Series: A series with the percentage of real completeness for each column.
    """
    if ignore_values is None:
        ignore_values = ['', ' ', 'not applicable', 'NA', 'n/a', None]  # Default ignore values; can be customized

    # Apply a mask where data is not in ignore_values
    mask = df.map(lambda x: x not in ignore_values)
    
    # Calculate the percentage of 'real' non-missing values
    real_completeness = mask.mean() * 100

    # for consistent plotting based on HCA guidelines
    real_completeness = pd.DataFrame(real_completeness)
    real_completeness.columns = ['Percentage']
    real_completeness = real_completeness.iloc[1:,:]
    real_completeness['Required'] = real_completeness.index.map(meta_col_dict)
    real_completeness = real_completeness.fillna('not defined')
    real_completeness['Required'] = real_completeness['Required'].astype('category')
    if len(real_completeness['Required'].unique())==2:
        real_completeness['Required'] = real_completeness['Required'].cat.reorder_categories(['not defined', 'MUST'])
    elif len(real_completeness['Required'].unique())==3:
        try:
            real_completeness['Required'] = real_completeness['Required'].cat.reorder_categories(['not defined', 'MUST', 'RECOMMENDED'])
        except:
            real_completeness['Required'] = real_completeness['Required'].cat.reorder_categories(['not defined', 'MUST', 'GUTSPECIFIC'])
    elif len(real_completeness['Required'].unique())==4:
        real_completeness['Required'] = real_completeness['Required'].cat.reorder_categories(['not defined', 'MUST', 'RECOMMENDED', 'GUTSPECIFIC'])

    return real_completeness

def check_consistency(df, field_name, allowed_values):
    """Check for values that are not in the allowed set, indicating inconsistency."""
    if field_name in df.columns:
        inconsistency_report = ~df[field_name].isin(allowed_values)
        return df[inconsistency_report]
    else:
        return pd.DataFrame()  # Return an empty DataFrame if field doesn't exist

def statistical_summary(df):
    """Provide a statistical summary for both numeric and categorical columns."""
    return df.describe(include='all')

def visualize_completeness(real_completeness_report, title):
    """Visualize real completeness for each column in a DataFrame."""
    fig, ax = plt.subplots(figsize=(8, int(real_completeness_report.shape[0]/3)))
    sns.barplot(data=real_completeness_report,
                x='Percentage',
                y=real_completeness_report.index,
                hue='Required'
               )
    plt.title(title)
    plt.xlabel('Percentage Complete (Adjusted for Non-Informative Values)')
    plt.ylabel('Fields')
    plt.show()

def validate_pattern(df, column_name, pattern):
    """
    Validates a column in a DataFrame against a provided regex pattern.
    
    Parameters:
        df (pd.DataFrame): The dataframe containing the data.
        column_name (str): The name of the column to validate.
        pattern (str): The regex pattern to validate against.
    
    Returns:
        pd.Series: A boolean series indicating whether each value matches the pattern.
    """
    if column_name in df.columns:
        # Compiling the regular expression pattern for efficiency in matching
        regex = re.compile(pattern)
        # Applying the regex to the column
        valid_mask = df[column_name].astype(str).apply(lambda x: bool(regex.match(x)))
        return valid_mask
    else:
        raise ValueError(f"The column '{column_name}' does not exist in the DataFrame.")

def validate_columns_with_patterns(df, patterns):
    """
    Validates columns in a DataFrame that are specified in the patterns dictionary.
    
    Parameters:
        df (pd.DataFrame): The dataframe to validate.
        patterns (dict): A dictionary of column names and their corresponding regex patterns.
    
    Returns:
        dict: A dictionary with column names as keys and Series of boolean values indicating validity.
    """
    validation_results = {}
    for column, pattern in patterns.items():
        if column in df.columns:  # Check if the column exists in the DataFrame
            regex = re.compile(pattern)
            validation_results[column] = df[column].astype(str).apply(lambda x: bool(regex.match(x)))
        else:
            validation_results[column] = pd.Series([], dtype=bool)  # Return empty series if column not in df
    return validation_results

def validate_allowed_values(df, allowed_values):
    """
    Validates the DataFrame columns based on the allowed values provided in the dictionary.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing the data to validate.
        allowed_values (dict): Dictionary with column names as keys and lists of allowed values as values.
    
    Returns:
        dict: Dictionary with validation results; each key is a column name, and the value is a Series of booleans.
    """
    validation_results = {}
    for column, values in allowed_values.items():
        if column in df.columns:
            validation_results[column] = df[column].isin(values)
        else:
            validation_results[column] = pd.Series([], dtype=bool)  # Return empty series if column not in df
    return validation_results

def report_invalid_entries(df, validation_results):
    """
    Prints and returns invalid entries for columns in the DataFrame based on validation results.

    # this will work with both permitted patterns and with permitted values
    
    Parameters:
        df (pd.DataFrame): The DataFrame that was validated.
        validation_results (dict): The validation results returned by validate_columns_with_patterns.
    
    Returns:
        dict: A dictionary containing dataframes of invalid entries for each column.
    """
    invalid_entries = {}
    for column, valid_series in validation_results.items():
        if not valid_series.empty and not valid_series.all():
            invalid_df = df[~valid_series]
            print(f"Invalid entries found in column '{column}':")
            display(invalid_df)
            invalid_entries[column] = invalid_df
    return invalid_entries

def completeness_by_dataset(df, grouping_var):
    """ Group the dataframe by grouping_var and calculate completeness for each group. """
    grouped = df.groupby(grouping_var)
    completeness = grouped.apply(check_completeness)
    return completeness


def plot_completeness(completeness_df, title):
    """ Plot the completeness for each worksheet in a dataframe. """
    ax = completeness_df.T.plot(kind='bar', figsize=(14, 7), title=title)
    ax.set_ylabel('Completeness (%)')
    ax.set_xlabel('Metadata Fields')
    plt.show()


def calculate_correctness(df, permitted_values, permitted_patterns):
    correctness = {}
    for column in df.columns:
        if column in permitted_values:
            correctness[column + '_value_correctness'] = (df[column].isin(permitted_values[column]).mean()) * 100
        if column in permitted_patterns:
            pattern = re.compile(permitted_patterns[column])
            correctness[column + '_pattern_correctness'] = (df[column].astype(str).apply(lambda x: bool(pattern.match(x))).mean()) * 100
    correctness_df = pd.DataFrame([correctness])
    return correctness_df.fillna(0)  # Fill NaN with 0 where there's no data

def calculate_correctness_per_group(df, permitted_values, permitted_patterns, group_by='worksheet'):
    # Create an empty DataFrame to store aggregated correctness data
    all_correctness = pd.DataFrame()
    # Group the DataFrame by the specified 'worksheet' column
    groups = df.groupby(group_by)
    for group_key, group_data in groups:
        correctness = {}
        for column in group_data.columns:
            if column in permitted_values:
                correctness[column] = (group_data[column].isin(permitted_values[column]).mean()) * 100
            if column in permitted_patterns:
                pattern = re.compile(permitted_patterns[column])
                correctness[column] = max(correctness.get(column, 0), (group_data[column].astype(str).apply(lambda x: bool(pattern.match(x))).mean()) * 100)
        # Transpose and rename index to include group_key for clarity
        correctness_df = pd.DataFrame(correctness, index=[group_key])
        all_correctness = pd.concat([all_correctness, correctness_df], axis=0)
    return all_correctness

def plot_correctness_heatmap(correctness_df, title):
    correctness_df = correctness_df.round().astype(int)
    plt.figure(figsize=(15, 8))  # Adjusted figure size
    sns.set_theme(style="whitegrid", font_scale=0.7)  # Using set_theme with style and font scale
    ax = sns.heatmap(correctness_df, annot=False, fmt="d", cmap='viridis', linewidths=.5,
                     cbar_kws={'label': 'Percent of Field Correctly Filled'})
    plt.title(title, pad=20)
    plt.xticks(rotation=60, ha='right')  # Adjust rotation slightly if needed
    plt.yticks(rotation=0)
    plt.tight_layout()  # Ensures everything fits without overlap or cut-off
    plt.show()