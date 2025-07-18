#Function
import pandas as pd
import numpy as np
from typing import Union, Optional
from sklearn.preprocessing import PowerTransformer
import matplotlib.pyplot as plt

def to_dataframe(X: Union[np.ndarray, pd.DataFrame]) -> pd.DataFrame:
    """
    Convert the input data into a pandas DataFrame.

    Parameters:
    X (Union[np.ndarray, pd.DataFrame]): The input data to be converted to a DataFrame. It can be a NumPy array or a pandas DataFrame.

    Returns:
    pd.DataFrame: A pandas DataFrame representation of the input data.
    """
    return pd.DataFrame(X)


def generate_symmetry_graphs(dataframe: pd.DataFrame, graph: str = 'boxplot', figsize: tuple = (16, 8), colors: tuple = ('red', 'green'), 
                             fontsize: int = 10, x_max_input: float = 0.7, y_max_input: float = 1, skew_threshold: float = 0.5, 
                             return_skew_dic: bool = False) -> Optional[dict]:
     
      """
      Generate symmetry graphs for the numerical columns in the given DataFrame.

      Parameters:
      dataframe (pd.DataFrame): The input DataFrame containing the data.
      graph (str): The type of graph to generate ('boxplot' or 'hist'). Default is 'boxplot'.
      figsize (tuple): The size of the figure. Default is (16, 8).
      colors (tuple): The colors to use for skewness indication. Default is ('red', 'green').
      fontsize (int): The font size for text in the plots. Default is 10.
      x_max_input (float): The position for skewness text on the x-axis. Default is 0.7.
      y_max_input (float): The position for skewness text on the y-axis. Default is 1.
      skew_threshold (float): The skewness threshold for color indication. Default is 0.5.
      return_skew_dic (bool): Whether to return the skewness dictionary. Default is False.

      Returns:
      Optional[dict]: A dictionary of skewness values for each column if return_skew_dic is True, otherwise None.
      """
      df_numerical = dataframe.select_dtypes(exclude = ['object'])
      df_numerical_columns = df_numerical.columns

      n = len(df_numerical_columns)  # Number of numerical columns
      cols = 5  # Number of subplot columns
      rows = (n + cols - 1) // cols  # Number of subplot rows, calculated to contain all plots

      fig, axes = plt.subplots(rows, cols, figsize=figsize)  # Create the subplot layout

      # Flatten the array of axes for easy iteration
      axes = axes.flatten()

      skew_dic = {}

      for col in df_numerical:
          skew_dic[col] = df_numerical[col].skew()

      # Crea un boxplot per ogni colonna numerica
      for i, col in enumerate(df_numerical_columns):

          if graph == 'boxplot':
              axes[i].boxplot(df_numerical[col].dropna())
          elif graph == 'hist':
              axes[i].hist(df_numerical[col].dropna(), bins=20)
          else:
              print('Graph not supported, choose boxplot or hist.')

          axes[i].set_title(f'{col}')
          y_max = axes[i].get_ylim()[1]
          x_max = axes[i].get_xlim()[1]
          color = colors[0] if abs(skew_dic[col]) > skew_threshold else colors[1]
          axes[i].text(x_max*x_max_input, y_max *y_max_input, f'Skewness: {skew_dic[col]:.2f}', color=color, fontsize=fontsize, ha='left')

      # Rimuove gli assi vuoti
      for j in range(i + 1, len(axes)):
          fig.delaxes(axes[j])

      plt.tight_layout()
      plt.show()

      if return_skew_dic: 
          return {col: round(skewness, 2) for col, skewness in skew_dic.items()}


# Class for conditional symmetrization based on skewness

class ConditionalSymmetrizer:
    """
    A custom transformer that applies symmetrization to columns with skewness
    exceeding a specified threshold using the Yeo-Johnson power transformation.

    Attributes:
    threshold (float): The skewness threshold above which the transformation is applied.
    """

    def __init__(self, threshold: float = 0.5) -> None:
        """
        Initialize the ConditionalSymmetrizer with a specified skewness threshold.

        Parameters:
        threshold (float): The skewness threshold for applying the power transformation. Default is 0.5.
        """
        self.threshold = threshold

    def fit(self, X: Union[np.ndarray, pd.DataFrame], y: Optional[pd.Series] = None) -> 'ConditionalSymmetrizer':
        """
        Fit the transformer on the input data. This method does not perform any operations
        as fitting is not required for this transformer.

        Parameters:
        X (Union[np.ndarray, pd.DataFrame]): The input data.
        y (Optional[pd.Series]): The target variable (not used in this transformer).

        Returns:
        ConditionalSymmetrizer: The fitted transformer object.
        """
        return self

    def transform(self, X: Union[np.ndarray, pd.DataFrame]) -> pd.DataFrame:
        """
        Apply symmetrization to columns in the input data that have skewness
        exceeding the specified threshold.

        Parameters:
        X (Union[np.ndarray, pd.DataFrame]): The input data to be transformed. It can be a NumPy array or a pandas DataFrame.

        Returns:
        pd.DataFrame: The symmetrized DataFrame with transformations applied to skewed columns.
        """
        if isinstance(X, np.ndarray):
            X = pd.DataFrame(X)

        X_symmetrized = X.copy()
        for col in X.columns:
            skewness = X[col].skew()
            if abs(skewness) > self.threshold:
                pt = PowerTransformer(method='yeo-johnson')
                X_symmetrized[col] = pt.fit_transform(X[[col]])
        return X_symmetrized






  