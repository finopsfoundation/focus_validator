import io
import logging
import sys
from typing import Any, Dict, Optional

import polars as pl


class CSVDataLoader:
    def __init__(self, data_filename, column_types=None):
        self.data_filename = data_filename
        self.column_types = column_types or {}
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")

        # Track failed columns for reporting
        self.failed_columns = set()

        # Common FOCUS columns that might need special handling
        self.common_string_columns = {
            "AccountId",
            "AccountName",
            "AvailabilityZone",
            "BillingAccountId",
            "BillingAccountName",
            "BillingCurrency",
            "BillingPeriodEnd",
            "BillingPeriodStart",
            "ChargeCategory",
            "ChargeClass",
            "ChargeDescription",
            "ChargeFrequency",
            "ChargePeriodEnd",
            "ChargePeriodStart",
            "ChargeType",
            "CommitmentDiscountCategory",
            "CommitmentDiscountId",
            "CommitmentDiscountName",
            "CommitmentDiscountStatus",
            "CommitmentDiscountType",
            "ConsumedService",
            "ConsumedUnit",
            "EffectiveCost",
            "InvoiceIssuerName",
            "ListCost",
            "ListUnitPrice",
            "PricingCategory",
            "PricingUnit",
            "ProviderName",
            "PublisherName",
            "RegionId",
            "RegionName",
            "ResourceId",
            "ResourceName",
            "ResourceType",
            "ServiceCategory",
            "ServiceName",
            "SkuId",
            "SkuPriceId",
            "SubAccountId",
            "SubAccountName",
            "Tags",
        }

    def _convert_pandas_to_polars_dtypes(
        self, dtype_dict: Dict[str, Any]
    ) -> Dict[str, pl.DataType]:
        """
        Convert pandas dtype specifications to Polars dtype specifications.

        Args:
            dtype_dict: Dictionary mapping column names to pandas dtype strings or Polars types

        Returns:
            Dictionary mapping column names to Polars DataType objects
        """
        polars_dtypes = {}
        for col, pandas_dtype in dtype_dict.items():
            # Handle Polars types directly
            if isinstance(pandas_dtype, pl.DataType):
                polars_dtypes[col] = pandas_dtype
            elif pandas_dtype == "string":
                polars_dtypes[col] = pl.Utf8()
            elif pandas_dtype == "float64":
                polars_dtypes[col] = pl.Float64()
            elif pandas_dtype in ["int64", "Int64"]:  # Both regular and nullable int
                polars_dtypes[col] = pl.Int64()
            elif isinstance(pandas_dtype, str) and pandas_dtype.startswith("datetime"):
                polars_dtypes[col] = pl.Datetime("us", "UTC")
            else:
                # For unknown types, default to string and log warning
                self.log.warning(
                    f"Unknown dtype '{pandas_dtype}' for column '{col}', defaulting to string"
                )
                polars_dtypes[col] = pl.Utf8()

        return polars_dtypes

    def _try_load_with_types(self, filename_or_buffer, dtype_dict, parse_dates_list):
        """
        Attempt to load CSV with specified types using Polars, with retry logic for problematic columns.

        Returns:
            pl.DataFrame: Loaded DataFrame with types applied
        """
        try:
            # Convert to Polars schema
            polars_dtypes = self._convert_pandas_to_polars_dtypes(dtype_dict)

            self.log.debug(f"Attempting to load with Polars dtypes: {polars_dtypes}")

            # Use schema_overrides instead of deprecated dtypes parameter
            df = pl.read_csv(
                filename_or_buffer,
                schema_overrides=polars_dtypes,
                try_parse_dates=bool(parse_dates_list),
                infer_schema_length=10000,  # Increased inference length
                null_values=[
                    "INVALID",
                    "INVALID_COST",
                    "BAD_DATE",
                    "INVALID_DECIMAL",
                    "INVALID_INT",
                    "NULL",
                ],  # Common invalid values
            )

            # Apply datetime conversions for columns specified in parse_dates_list
            for col in parse_dates_list:
                if col in df.columns:
                    converted = self._smart_datetime_conversion(df[col], col)
                    if converted is not None:
                        df = df.with_columns(converted.alias(col))
                    else:
                        # Drop failed datetime columns (mixed timezones, etc.)
                        df = df.drop(col)
                        self.failed_columns.add(col)
                        self.log.warning(
                            f"Dropped column {col} due to datetime conversion failure"
                        )

            return df

        except Exception as e:
            self.log.warning(
                "Initial Polars load with all column types failed: %s", str(e)
            )
            # Reset failed columns since we're trying a different approach
            self.failed_columns = set()
            # Try alternative approach: load without types, then apply conversions with coercion
            return self._load_and_convert_with_coercion(
                filename_or_buffer, dtype_dict, parse_dates_list
            )

    def _load_and_convert_with_coercion(
        self, filename_or_buffer, dtype_dict, parse_dates_list
    ):
        """
        Load the CSV with Polars and apply type conversions with coercion for error handling.

        Args:
            filename_or_buffer: File path or buffer to read from
            dtype_dict: Dictionary of column types to apply
            parse_dates_list: List of columns to parse as dates

        Returns:
            pl.DataFrame: Loaded DataFrame with best-effort type conversions
        """
        try:
            self.log.debug("Starting load with coercion for failed columns...")

            # Load CSV without any type specifications - let Polars infer
            df = pl.read_csv(
                filename_or_buffer,
                infer_schema_length=10000,
                null_values=[
                    "INVALID",
                    "INVALID_COST",
                    "BAD_DATE",
                    "INVALID_DECIMAL",
                    "INVALID_INT",
                    "NULL",
                ],
            )

            # Apply datetime conversions with coercion
            for col in parse_dates_list:
                if col in df.columns:
                    try:
                        result = self._smart_datetime_conversion(df[col], col)
                        if result is not None:
                            df = df.with_columns(result.alias(col))
                            self.log.debug(
                                "Successfully converted %s to datetime (invalid values -> null)",
                                col,
                            )
                        else:
                            # Mixed timezones detected - drop column
                            df = df.drop(col)
                            self.failed_columns.add(col)
                            self.log.warning(
                                "Dropped column '%s' due to mixed timezone data quality issue",
                                col,
                            )
                    except Exception as e:
                        df = df.drop(col)
                        self.failed_columns.add(col)
                        self.log.warning(
                            "Failed to convert %s to datetime: %s", col, str(e)
                        )

            # Apply other type conversions with coercion
            for col, target_type in dtype_dict.items():
                if col in df.columns and col not in parse_dates_list:
                    try:
                        # Convert to Polars type
                        if isinstance(target_type, pl.DataType):
                            polars_type = target_type
                        else:
                            polars_type = self._convert_pandas_to_polars_dtypes(
                                {col: target_type}
                            )[col]

                        # Apply conversion with error handling
                        if polars_type == pl.Float64:
                            # Convert to float with coercion (handle int/string/float)
                            if df[col].dtype in [pl.Int64, pl.Int32, pl.Float32]:
                                # Already numeric, just cast to Float64
                                df = df.with_columns(
                                    pl.col(col)
                                    .cast(pl.Float64, strict=False)
                                    .alias(col)
                                )
                            else:
                                # String to float conversion
                                df = df.with_columns(
                                    pl.col(col)
                                    .str.replace_all(r"[^\d.-]", "")
                                    .cast(pl.Float64, strict=False)
                                    .alias(col)
                                )
                        elif polars_type == pl.Int64:
                            # Convert to int with coercion
                            if df[col].dtype in [pl.Float64, pl.Float32]:
                                # Float to int (will truncate)
                                df = df.with_columns(
                                    pl.col(col).cast(pl.Int64, strict=False).alias(col)
                                )
                            else:
                                # String to int conversion
                                df = df.with_columns(
                                    pl.col(col)
                                    .str.replace_all(r"[^\d-]", "")
                                    .cast(pl.Int64, strict=False)
                                    .alias(col)
                                )
                        elif polars_type == pl.Utf8:
                            # Convert to string
                            df = df.with_columns(
                                pl.col(col).cast(pl.Utf8, strict=False).alias(col)
                            )

                        self.log.debug(f"Successfully converted {col} to {polars_type}")

                    except Exception as e:
                        self.log.warning(
                            "Failed to convert %s to %s: %s", col, target_type, str(e)
                        )
                        self.failed_columns.add(col)

            return df

        except Exception as e:
            self.log.error(
                "All enhanced loading attempts failed, falling back to basic CSV loading: %s",
                str(e),
            )
            # Last resort: basic CSV loading with resilience options
            return pl.read_csv(
                filename_or_buffer,
                null_values=[
                    "INVALID",
                    "INVALID_COST",
                    "BAD_DATE",
                    "INVALID_DECIMAL",
                    "INVALID_INT",
                    "NULL",
                ],
                truncate_ragged_lines=True,  # Handle inconsistent column counts
                ignore_errors=True,  # Skip problematic rows
            )

    def _smart_datetime_conversion(
        self, series: pl.Series, column_name: str = "unknown"
    ) -> Optional[pl.Series]:
        """
        Convert series to datetime using Polars, handling timezone info strictly.

        Strategy:
        - If mixed timezones detected: return None (data quality issue)
        - If single timezone: preserve original timezone
        - If no timezone (naive): default to UTC

        Returns:
            pl.Series with datetime dtype, or None if conversion failed
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
                    return series  # Already UTC

            # Handle string datetime conversion
            if series.dtype == pl.Utf8:
                # String or other type to datetime conversion
                try:
                    # Try multiple datetime parsing strategies
                    converted = None

                    # Strategy 1: Try ISO format with timezone
                    try:
                        candidate = series.str.to_datetime(
                            format="%Y-%m-%dT%H:%M:%S%z",  # ISO with timezone like -05:00
                            strict=False,
                        )
                        # Check if conversion was successful (all values converted)
                        if candidate.null_count() == 0:
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
                            # Check if conversion was successful (all values converted)
                            if candidate.null_count() == 0:
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
                            # Check if conversion was successful (all values converted)
                            if candidate.null_count() == 0:
                                converted = candidate
                        except Exception:
                            pass

                    # Strategy 4: Try simple date format (YYYY-MM-DD)
                    if converted is None:
                        try:
                            candidate = series.str.to_datetime(
                                format="%Y-%m-%d", strict=False  # Simple date format
                            )
                            # Check if conversion was successful (all values converted)
                            if candidate.null_count() == 0:
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

                            # Check if we successfully converted all values
                            if candidate.null_count() == 0:
                                converted = candidate

                        except Exception:
                            pass

                    # Strategy 6: Let Polars infer format (for fallback cases)
                    if converted is None:
                        try:
                            candidate = series.str.to_datetime(
                                format=None,  # Let Polars infer
                                strict=False,  # Allow invalid dates to become null
                                exact=False,  # Allow partial matches
                                cache=True,  # Cache format inference
                            )
                            # For auto-inference, allow some nulls but require most values to convert
                            if candidate.null_count() < len(candidate):
                                converted = candidate
                        except Exception:
                            pass

                    if converted is not None:
                        # Convert to UTC timezone if not already
                        if isinstance(converted.dtype, pl.Datetime):
                            datetime_dtype = converted.dtype  # type: pl.Datetime
                            if datetime_dtype.time_zone is None:
                                converted = converted.dt.replace_time_zone("UTC")
                            elif datetime_dtype.time_zone != "UTC":
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

            # Handle other data types (not datetime or string)
            self.log.warning(
                f"Cannot convert column '{column_name}' with dtype {series.dtype} to datetime"
            )
            return None

        except Exception as e:
            self.log.warning(
                f"Failed to convert column '{column_name}' to datetime: {e}. "
                f"Column will be excluded to prevent validation issues."
            )
            return None

    def _get_parse_dates_list(self):
        """
        Get list of columns that should be parsed as dates based on column_types.

        Returns:
            List of column names to parse as dates
        """
        parse_dates = []
        if self.column_types:
            for col, col_type in self.column_types.items():
                # Check for datetime types
                if isinstance(col_type, pl.DataType) and isinstance(
                    col_type, pl.Datetime
                ):
                    parse_dates.append(col)
                elif isinstance(col_type, str) and col_type.startswith("datetime"):
                    parse_dates.append(col)
        return parse_dates

    def load(self):
        """
        Load CSV data with enhanced error handling and type coercion.

        Returns:
            pl.DataFrame: Loaded DataFrame
        """
        # Reset failed columns tracking
        self.failed_columns = set()

        # Determine parse_dates list from column_types
        parse_dates_list = self._get_parse_dates_list()

        try:
            if self.data_filename == "-":
                # Handle stdin
                csv_content = sys.stdin.read()
                filename_or_buffer = io.StringIO(csv_content)

                if self.column_types or parse_dates_list:
                    # For stdin, apply column types and parse_dates with retry logic
                    return self._try_load_with_types(
                        filename_or_buffer, self.column_types, parse_dates_list
                    )
                else:
                    # Basic loading for stdin without column types
                    return pl.read_csv(
                        filename_or_buffer,
                        truncate_ragged_lines=True,
                        ignore_errors=True,
                        null_values=[
                            "INVALID",
                            "INVALID_COST",
                            "BAD_DATE",
                            "INVALID_DECIMAL",
                            "INVALID_INT",
                            "NULL",
                        ],
                    )
            else:
                # Handle file
                if self.column_types or parse_dates_list:
                    return self._try_load_with_types(
                        self.data_filename, self.column_types, parse_dates_list
                    )
                else:
                    # Basic file loading without column types
                    return pl.read_csv(
                        self.data_filename,
                        truncate_ragged_lines=True,  # Handle inconsistent column counts
                        ignore_errors=True,  # Skip problematic rows
                        null_values=[
                            "INVALID",
                            "INVALID_COST",
                            "BAD_DATE",
                            "INVALID_DECIMAL",
                            "INVALID_INT",
                            "NULL",
                        ],
                    )

        except Exception as e:
            self.log.error(f"Failed to load CSV data: {e}")
            raise Exception(f"Failed to load CSV data: {e}") from e

    def get_failed_columns(self):
        """
        Get set of column names that failed type conversion.

        Returns:
            set: Column names that failed conversion
        """
        return self.failed_columns.copy()
