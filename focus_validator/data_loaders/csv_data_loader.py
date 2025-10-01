import pandas as pd


class CSVDataLoader:
    def __init__(self, data_filename):
        self.data_filename = data_filename
        
        # FOCUS specification string columns to ensure proper typing
        # This prevents DuckDB type inference issues when columns contain only NULLs
        self.STRING_COLUMNS = {
            'AvailabilityZone', 'BillingAccountId', 'BillingAccountName', 'BillingCurrency',
            'ChargeCategory', 'ChargeClass', 'ChargeDescription', 'ChargeFrequency',
            'CommitmentDiscountCategory', 'CommitmentDiscountId', 'CommitmentDiscountName',
            'CommitmentDiscountStatus', 'CommitmentDiscountType', 'ConsumedUnit',
            'InvoiceIssuerName', 'PricingCategory', 'PricingUnit', 'ProviderName',
            'PublisherName', 'RegionId', 'RegionName', 'ResourceId', 'ResourceName',
            'ResourceType', 'ServiceCategory', 'ServiceName', 'SkuId', 'SkuPriceId',
            'SubAccountId', 'SubAccountName', 'Tags'
        }

    def load(self):
        try:
            # For regular file paths, use two-pass approach to set proper string types
            if isinstance(self.data_filename, str):
                # Read CSV with first pass to get columns
                df_peek = pd.read_csv(self.data_filename, nrows=0)
                
                # Build dtype mapping for string columns that exist in the file
                dtype_dict = {}
                for col in df_peek.columns:
                    if col in self.STRING_COLUMNS:
                        dtype_dict[col] = 'string'
                
                # Load with proper string types to avoid DuckDB type inference issues
                return pd.read_csv(self.data_filename, dtype=dtype_dict)
            else:
                # For file-like objects (BytesIO, etc.), fall back to single-pass approach
                # This is mainly for testing scenarios
                return pd.read_csv(self.data_filename)
        except Exception as e:
            # If the two-pass approach fails for any reason, fall back to regular loading
            return pd.read_csv(self.data_filename)
