import io
import logging
import sys
from typing import Any, Optional

import polars as pl


class ParquetDataLoader:
    def __init__(self, data_filename, column_types=None):
        self.data_filename = data_filename
        self.column_types = column_types or {}  # Column types for post-load conversion
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.failed_columns = set()  # Track columns that failed type conversion

    def _smart_datetime_conversion(
        self, series: pl.Series, column_name: str = "unknown"
    ) -> Optional[pl.Series]:
        """
        Convert series to datetime, handling timezone info strictly using Polars.

        Strategy:
        - Try to convert to datetime with UTC timezone
        - If conversion fails, mark column as failed

        Args:
            series: Polars Series to convert
            column_name: name of the column for logging

        Returns:
            Polars Series with datetime values, or None if conversion fails
        """
        try:
            # Check if already datetime type
            if isinstance(series.dtype, pl.Datetime):
                # Already datetime, but validate that conversion was successful
                # If there are nulls, it means some values failed to convert
                if series.null_count() > 0:
                    self.log.warning(
                        "Column '%s' contains %d null values after datetime conversion, dropping column due to data quality issues",
                        column_name,
                        series.null_count(),
                    )
                    return None

                # All values converted successfully, ensure UTC timezone
                if series.dtype.time_zone is None:
                    return series.dt.replace_time_zone("UTC")
                elif series.dtype.time_zone != "UTC":
                    return series.dt.convert_time_zone("UTC")
                else:
                    return series  # Already UTC timezone

            # Handle string datetime conversion
            if series.dtype == pl.Utf8:
                # String or other type to datetime conversion
                try:
                    # Try multiple datetime parsing strategies
                    converted = None

                    # A strategy succeeds only if parsing introduces no nulls
                    # beyond those already present (so nullable columns convert),
                    # and the column has at least one real value (an all-null
                    # column carries no evidence of being a datetime, so it is
                    # left as a string rather than coerced).
                    original_null_count = series.null_count()
                    has_values = original_null_count < series.len()

                    # Strategy 1: Try ISO format with timezone
                    try:
                        candidate = series.str.to_datetime(
                            format="%Y-%m-%dT%H:%M:%S%z",  # ISO with timezone like -05:00
                            strict=False,
                        )
                        # Accept if parsing added no new nulls (nullable columns)
                        # and at least one value actually parsed
                        if has_values and candidate.null_count() == original_null_count:
                            converted = candidate
                    except Exception:
                        pass

                    # Strategy 2: Try ISO format with Z timezone
                    if converted is None:
                        try:
                            candidate = series.str.to_datetime(
                                format="%Y-%m-%dT%H:%M:%SZ",  # ISO with Z timezone
                                strict=False,
                            )
                            # Accept if parsing added no new nulls (nullable
                            # columns) and at least one value actually parsed
                            if has_values and candidate.null_count() == original_null_count:
                                converted = candidate
                        except Exception:
                            pass

                    # Strategy 3: Try space-separated format
                    if converted is None:
                        try:
                            candidate = series.str.to_datetime(
                                format="%Y-%m-%d %H:%M:%S",  # Space-separated format
                                strict=False,
                            )
                            # Accept if parsing added no new nulls (nullable
                            # columns) and at least one value actually parsed
                            if has_values and candidate.null_count() == original_null_count:
                                converted = candidate
                        except Exception:
                            pass

                    # Strategy 4: Try simple date format (YYYY-MM-DD)
                    if converted is None:
                        try:
                            candidate = series.str.to_datetime(
                                format="%Y-%m-%d", strict=False  # Simple date format
                            )
                            # Accept if parsing added no new nulls (nullable
                            # columns) and at least one value actually parsed
                            if has_values and candidate.null_count() == original_null_count:
                                converted = candidate
                        except Exception:
                            pass

                    # Strategy 5: Try mixed timezone handling - parse each row individually
                    if converted is None:
                        try:
                            # Create a list to hold converted timestamps
                            converted_values: list[Any] = []

                            for value in series:
                                if value is None:
                                    converted_values.append(None)
                                    continue

                                value_str = str(value)
                                parsed_value = None

                                # Try different timezone formats for this individual value
                                for fmt in [
                                    "%Y-%m-%dT%H:%M:%S%z",
                                    "%Y-%m-%dT%H:%M:%SZ",
                                    "%Y-%m-%d %H:%M:%S",
                                    "%Y-%m-%d",
                                ]:
                                    try:
                                        temp_series = pl.Series([value_str])
                                        temp_result = temp_series.str.to_datetime(
                                            format=fmt, strict=False
                                        )
                                        if temp_result[0] is not None:
                                            parsed_value = temp_result[0]
                                            break
                                    except Exception:
                                        continue

                                converted_values.append(parsed_value)

                            # Create new series from converted values
                            candidate = pl.Series(
                                series.name, converted_values, dtype=pl.Datetime("us")
                            )

                            # Accept if parsing added no new nulls (nullable
                            # columns) and at least one value actually parsed
                            if has_values and candidate.null_count() == original_null_count:
                                converted = candidate

                        except Exception:
                            pass

                    # Strategy 6: Let Polars infer the format (fallback for any
                    # single format strategies 1-5 did not match). Format inference
                    # cannot parse timezone-qualified ISO (trailing 'Z' or offsets),
                    # but strategies 1-2 already cover those.
                    if converted is None:
                        try:
                            candidate = series.str.to_datetime(
                                format=None,  # Let Polars infer
                                strict=False,  # Allow invalid dates to become null
                                exact=False,  # Allow partial matches
                                cache=True,  # Cache format inference
                            )
                            # Accept if parsing added no new nulls (nullable columns)
                            # and at least one value actually parsed
                            if has_values and candidate.null_count() == original_null_count:
                                converted = candidate
                        except Exception:
                            pass

                    if converted is not None:
                        # Convert to UTC timezone if not already
                        if isinstance(converted.dtype, pl.Datetime):
                            dt_dtype = converted.dtype  # mypy type narrowing
                            if dt_dtype.time_zone is None:
                                converted = converted.dt.replace_time_zone("UTC")
                            elif dt_dtype.time_zone != "UTC":
                                converted = converted.dt.convert_time_zone("UTC")

                        return converted
                    else:
                        self.log.warning(
                            f"Failed to convert {column_name} to datetime with all strategies"
                        )
                        return None

                except Exception as e:
                    self.log.warning(
                        f"Failed to convert {column_name} to datetime: {e}"
                    )
                    return None
            else:
                # For other types, cannot convert to datetime
                self.log.warning(
                    f"Cannot convert column '{column_name}' of type {series.dtype} to datetime"
                )
                return None

        except Exception as e:
            self.log.warning(
                f"Failed to convert column '{column_name}' to datetime: {e}. "
                f"Column will be excluded to prevent validation issues."
            )
            return None

    def _apply_column_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Apply column type conversions to loaded Parquet data with error handling using Polars.

        Args:
            df: Polars DataFrame loaded from Parquet

        Returns:
            Polars DataFrame with type conversions applied where possible
        """
        if not self.column_types:
            return df

        for col, target_type in self.column_types.items():
            if col not in df.columns:
                continue

            try:
                # Handle Polars datetime types directly
                if isinstance(target_type, pl.Datetime) or (
                    isinstance(target_type, str) and target_type.startswith("datetime")
                ):
                    # Convert to datetime, handling timezones strictly
                    result = self._smart_datetime_conversion(df[col], col)
                    if result is not None:
                        df = df.with_columns(result.alias(col))
                    else:
                        # Drop column with failed conversion (mixed timezones, etc.)
                        df = df.drop(col)
                        self.failed_columns.add(col)
                        self.log.warning(
                            f"Dropped column '{col}' due to datetime conversion issues"
                        )
                elif target_type == "string":
                    # Convert to string
                    df = df.with_columns(pl.col(col).cast(pl.Utf8, strict=False))
                elif target_type == "float64":
                    # Convert to float
                    df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
                elif target_type in ["int64", "Int64"]:
                    # Convert to int (Polars handles nulls natively)
                    df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))
                # Add more type conversions as needed

            except Exception as e:
                self.log.warning(
                    f"Failed to convert column '{col}' to type '{target_type}': {e}. Using original type."
                )
                self.failed_columns.add(col)

        return df

    def load(self):
        try:
            # Load Parquet data using Polars
            if self.data_filename == "-":
                # Handle stdin input for Parquet files
                # Read binary data from stdin into BytesIO buffer
                # This allows Polars to properly parse the Parquet format
                binary_data = sys.stdin.buffer.read()
                buffer = io.BytesIO(binary_data)
                df = pl.read_parquet(buffer)
            else:
                df = pl.read_parquet(self.data_filename)

            # Apply column type conversions if specified
            df = self._apply_column_types(df)

            # Log any failed columns for user awareness
            if self.failed_columns:
                self.log.warning(
                    f"Failed to apply specified types to {len(self.failed_columns)} Parquet columns "
                    f"(using original types): {sorted(self.failed_columns)}"
                )

            return df

        except Exception as e:
            self.log.error(f"Failed to load Parquet file: {e}")
            raise
