import logging
import warnings

import pandas as pd


class CSVDataLoader:
    def __init__(self, data_filename, column_types=None):
        self.data_filename = data_filename
        self.column_types = column_types or {}
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.failed_columns = set()  # Track columns that failed type conversion

        # FOCUS specification string columns to ensure proper typing
        # This prevents DuckDB type inference issues when columns contain only NULLs
        self.STRING_COLUMNS = {
            "AvailabilityZone",
            "BillingAccountId",
            "BillingAccountName",
            "BillingCurrency",
            "ChargeCategory",
            "ChargeClass",
            "ChargeDescription",
            "ChargeFrequency",
            "CommitmentDiscountCategory",
            "CommitmentDiscountId",
            "CommitmentDiscountName",
            "CommitmentDiscountStatus",
            "CommitmentDiscountType",
            "ConsumedUnit",
            "InvoiceIssuerName",
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

    def _try_load_with_types(self, filename_or_buffer, dtype_dict, parse_dates_list):
        """
        Attempt to load CSV with specified types, with retry logic for problematic columns.

        Returns:
            pandas.DataFrame: Loaded data
        """
        try:
            # First attempt: try with all specified types
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                return pd.read_csv(
                    filename_or_buffer, dtype=dtype_dict, parse_dates=parse_dates_list
                )

        except (ValueError, TypeError, pd.errors.DtypeWarning) as e:
            self.log.warning("Initial load with all column types failed: %s", str(e))

            # Try alternative approach: load without types, then apply conversions with coercion
            return self._load_and_convert_with_coercion(
                filename_or_buffer, dtype_dict, parse_dates_list
            )

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

    def _load_and_convert_with_coercion(
        self, filename_or_buffer, dtypes_dict, parse_dates_list
    ):
        """
        Load the CSV and apply type conversions with coercion for error handling.

        Returns:
            pandas.DataFrame or None if loading fails
        """
        try:
            self.log.debug("Starting load with coercion for failed columns...")

            # Load CSV without any type specifications
            df = pd.read_csv(filename_or_buffer)

            # Apply datetime conversions with coercion
            for col in parse_dates_list:
                if col in df.columns:
                    try:
                        result = self._smart_datetime_conversion(df[col], col)
                        if result is not None:
                            df[col] = result
                            self.log.debug(
                                "Successfully converted %s to datetime (invalid values -> NaT)",
                                col,
                            )
                        else:
                            # Mixed timezones detected - drop the column
                            df = df.drop(columns=[col])
                            self.failed_columns.add(col)
                            self.log.warning(
                                "Dropped column '%s' due to mixed timezone data quality issue",
                                col,
                            )
                    except Exception as e:
                        self.log.warning(
                            "Failed to convert %s to datetime: %s", col, str(e)
                        )
                        self.failed_columns.add(col)

            # Apply other type conversions with coercion
            for col, target_type in dtypes_dict.items():
                if col not in df.columns:
                    continue

                try:
                    if target_type == "string":
                        df[col] = df[col].astype("string")
                        self.log.debug("Successfully converted %s to string", col)
                    elif target_type == "float64":
                        df[col] = pd.to_numeric(
                            df[col], errors="coerce", downcast=None
                        ).astype("float64")
                        self.log.debug(
                            "Successfully converted %s to float64 (invalid values -> NaN)",
                            col,
                        )
                    elif target_type == "int64":
                        # Convert to nullable int type to handle NaNs properly
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype(
                            "Int64"
                        )
                        self.log.debug(
                            "Successfully converted %s to int64 (invalid values -> NaN)",
                            col,
                        )
                    else:
                        # For other types, try direct conversion
                        df[col] = df[col].astype(target_type)
                        self.log.debug(
                            "Successfully converted %s to %s", col, target_type
                        )

                except Exception as e:
                    self.log.warning(
                        "Failed to convert column '%s' to type '%s': %s",
                        col,
                        target_type,
                        str(e),
                    )
                    self.failed_columns.add(col)

            return df

        except Exception as e:
            self.log.error("Failed to load with coercion: %s", str(e))
            return None

    def load(self):
        try:
            # Determine dtype mapping from extracted column types or fallback to hardcoded set
            dtype_dict = {}
            parse_dates_list = []

            if self.column_types:
                # Use dynamically extracted column types from rules
                for col, col_type in self.column_types.items():
                    if col_type.startswith("datetime"):
                        # For datetime columns, use parse_dates instead of dtype
                        parse_dates_list.append(col)
                    else:
                        # For non-datetime columns, use dtype
                        dtype_dict[col] = col_type
            else:
                # Fallback to hardcoded STRING_COLUMNS for backward compatibility
                for col in self.STRING_COLUMNS:
                    dtype_dict[col] = "string"

            # Handle stdin input
            if self.data_filename == "-":
                import sys

                # For stdin, apply column types and parse_dates with retry logic
                result = self._try_load_with_types(
                    sys.stdin, dtype_dict, parse_dates_list
                )

            # For regular file paths, use two-pass approach to set proper types
            elif isinstance(self.data_filename, str):
                # Read CSV with first pass to get columns
                df_peek = pd.read_csv(self.data_filename, nrows=0)

                # Build final dtype mapping and parse_dates list for columns that exist in the file
                final_dtype_dict = {}
                final_parse_dates = []

                for col in df_peek.columns:
                    if col in dtype_dict:
                        final_dtype_dict[col] = dtype_dict[col]
                    if col in parse_dates_list:
                        final_parse_dates.append(col)

                # Load with proper types and datetime parsing using robust method
                result = self._try_load_with_types(
                    self.data_filename, final_dtype_dict, final_parse_dates
                )

                # Check if any columns need type coercion due to pandas auto-inference
                needs_coercion = False

                # Check datetime columns that ended up as object type or without timezone
                for col in final_parse_dates:
                    if col in result.columns:
                        col_dtype_str = str(result[col].dtype)
                        if col_dtype_str == "object" or (
                            col_dtype_str.startswith("datetime64")
                            and "UTC" not in col_dtype_str
                        ):
                            needs_coercion = True
                            break

                # Check other columns that don't match expected types
                if not needs_coercion:
                    for col, expected_type in final_dtype_dict.items():
                        if (
                            col in result.columns
                            and str(result[col].dtype) != expected_type
                        ):
                            needs_coercion = True
                            break

                if needs_coercion:
                    self.log.info(
                        "Detected columns with incorrect types, applying coercion"
                    )
                    result = self._load_and_convert_with_coercion(
                        self.data_filename, final_dtype_dict, final_parse_dates
                    )
            else:
                # For file-like objects (BytesIO, etc.), fall back to single-pass approach
                # This is mainly for testing scenarios
                result = self._try_load_with_types(
                    self.data_filename, dtype_dict, parse_dates_list
                )

            # Log any failed columns for user awareness
            if self.failed_columns:
                invalid_count = sum(
                    result[col].isna().sum()
                    for col in self.failed_columns
                    if col in result.columns
                )
                self.log.warning(
                    "Applied type conversions with coercion to %d columns. %d invalid values converted to NaN/NaT: %s",
                    len(self.failed_columns),
                    invalid_count,
                    sorted(self.failed_columns),
                )

            return result

        except Exception as e:
            # Ultimate fallback: basic CSV loading without any type specifications
            self.log.error(
                "All enhanced loading attempts failed, falling back to basic CSV loading: %s",
                str(e),
            )
            return pd.read_csv(self.data_filename)
