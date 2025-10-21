import io
import logging
import sys
import warnings

import pandas as pd


class ParquetDataLoader:
    def __init__(self, data_filename, column_types=None):
        self.data_filename = data_filename
        self.column_types = column_types or {}  # Column types for post-load conversion
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.failed_columns = set()  # Track columns that failed type conversion

    def _smart_datetime_conversion(self, series, column_name="unknown"):
        """
        Convert series to datetime, handling timezone info strictly.

        Strategy:
        - If mixed timezones detected: mark column as failed (data quality issue)
        - If single timezone: preserve original timezone
        - If no timezone (naive): default to UTC

        Args:
            series: pandas Series to convert
            column_name: name of the column for logging

        Returns:
            pandas Series with datetime values, or None if mixed timezones detected
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            warnings.simplefilter("ignore", FutureWarning)

            # First convert to datetime - this may result in mixed timezones
            converted = pd.to_datetime(series, errors="coerce")

            # Check if we got object dtype (indicates mixed timezones)
            if converted.dtype == "object":
                # Mixed timezones detected - this is a data quality issue
                self.log.warning(
                    "Mixed timezones detected in column '%s'. This indicates inconsistent "
                    "datetime formatting. Column will be excluded to prevent validation issues.",
                    column_name,
                )
                return None
            elif converted.dt.tz is None:
                # Timezone-naive data - default to UTC
                converted = converted.dt.tz_localize("UTC")
            # else: single timezone already present, preserve it

            return converted

    def _apply_column_types(self, df):
        """
        Apply column type conversions to loaded Parquet data with error handling.

        Args:
            df: pandas.DataFrame loaded from Parquet

        Returns:
            pandas.DataFrame with type conversions applied where possible
        """
        if not self.column_types:
            return df

        for col, target_type in self.column_types.items():
            if col not in df.columns:
                continue

            try:
                if target_type.startswith("datetime"):
                    # Convert to datetime, handling mixed timezones strictly
                    result = self._smart_datetime_conversion(df[col], col)
                    if result is not None:
                        df[col] = result
                    else:
                        # Mixed timezones detected - drop the column
                        df = df.drop(columns=[col])
                        self.log.warning(
                            "Dropped column '%s' due to mixed timezone data quality issue",
                            col,
                        )
                elif target_type == "string":
                    # Convert to string
                    df[col] = df[col].astype("string")
                elif target_type == "float64":
                    # Convert to float
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif target_type == "int64":
                    # Convert to int (with nullable int type to handle NaNs)
                    df[col] = df[col].astype("Int64")
                # Add more type conversions as needed

            except Exception as e:
                self.log.warning(
                    "Failed to convert column '%s' to type '%s': %s. Using original type.",
                    col,
                    target_type,
                    str(e),
                )
                self.failed_columns.add(col)

        return df

    def load(self):
        try:
            # Load Parquet data
            if self.data_filename == "-":
                # Handle stdin input for Parquet files
                # Read binary data from stdin into BytesIO buffer
                # This allows pandas to properly parse the Parquet format
                binary_data = sys.stdin.buffer.read()
                buffer = io.BytesIO(binary_data)
                df = pd.read_parquet(buffer)
            else:
                df = pd.read_parquet(self.data_filename)

            # Apply column type conversions if specified
            df = self._apply_column_types(df)

            # Log any failed columns for user awareness
            if self.failed_columns:
                self.log.warning(
                    "Failed to apply specified types to %d Parquet columns (using original types): %s",
                    len(self.failed_columns),
                    sorted(self.failed_columns),
                )

            return df

        except Exception as e:
            self.log.error("Failed to load Parquet file: %s", str(e))
            raise
