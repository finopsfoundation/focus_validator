from concurrent.futures import ThreadPoolExecutor
import csv
import functools
import io
import logging
import random
import time
import pytz
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

def log_execution_time(func):
    """Decorator to log the execution time of a function."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"{func.__name__} executed in {end_time - start_time:.2f} seconds")
        return result

    return wrapper

class FakeFocus:
    
    def __init__(self):
        self._cache = {}
    
    # Current time in UTC
    now = datetime.now(pytz.utc)

    # 30 days ago from now
    thirty_days_ago = now - timedelta(days=30)

    @property
    def InvoiceIssuer(self):
        return self._cached_property('InvoiceIssuer', self.get_aws_invoice_issuer)

    @property
    def ResourceID(self):
        return self._cached_property('ResourceID', fake.uuid4)

    @property
    def ChargeType(self):
        return self._cached_property('ChargeType', self.get_charge_type)
    
    @property
    def Provider(self):
        return self._cached_property('Provider', fake.company)
    
    @property
    def BillingAccountName(self):
        return self._cached_property('BillingAccountName', fake.company)
    
    @property
    def SubAccountName(self):
        # Only generate a new datetime when it's not in cache
        if 'SubAccountName' not in self._cache:
            # Generate a random datetime object within the last 30 days
            random_datetime = fake.date_time_between(start_date=self.thirty_days_ago, end_date=self.now, tzinfo=pytz.utc)
            formatted_date = datetime.strftime(random_datetime, '%Y-%m-%dT%H:%M:%SZ')
            self._cache['SubAccountName'] = formatted_date

        return self._cache['SubAccountName']

    @property
    def BillingAccountId(self):
        return self._cached_property('BillingAccountId', fake.uuid4)

    @property
    def Publisher(self):
        return self._cached_property('Publisher', self.get_aws_publisher)

    @property
    def ResourceName(self):
        return self._cached_property('ResourceName', self.get_aws_resource_name)

    @property
    def ServiceName(self):
        return self._cached_property('ServiceName', self.get_aws_service_name)

    @property
    def BilledCurrency(self):
        return self._cached_property('BilledCurrency', lambda: 'USD')

    @property
    def BillingPeriodEnd(self):
        # Only generate a new datetime when it's not in cache
        if 'BillingPeriodEnd' not in self._cache:
            # Generate a random datetime object within the last 30 days
            random_datetime = fake.date_time_between(start_date=self.thirty_days_ago, end_date=self.now, tzinfo=pytz.utc)
            formatted_date = datetime.strftime(random_datetime, '%Y-%m-%dT%H:%M:%SZ')
            self._cache['BillingPeriodEnd'] = formatted_date

        return self._cache['BillingPeriodEnd']

    @property
    def BillingPeriodStart(self):
        # Only generate a new datetime when it's not in cache
        if 'BillingPeriodStart' not in self._cache:
            # Generate a random datetime object within the last 30 days
            random_datetime = fake.date_time_between(start_date=self.thirty_days_ago, end_date=self.now, tzinfo=pytz.utc)
            formatted_date = datetime.strftime(random_datetime, '%Y-%m-%dT%H:%M:%SZ')
            self._cache['BillingPeriodStart'] = formatted_date

        return self._cache['BillingPeriodStart']

    @property
    def Region(self):
        return self._cached_property('Region', self.get_aws_region)

    @property
    def ServiceCategory(self):
        return self._cached_property('ServiceCategory', self.get_aws_service_category)

    @property
    def ChargePeriodStart(self):
        # Only generate a new datetime when it's not in cache
        if 'ChargePeriodStart' not in self._cache:
            # Generate a random datetime object within the last 30 days
            random_datetime = fake.date_time_between(start_date=self.thirty_days_ago, end_date=self.now, tzinfo=pytz.utc)
            formatted_date = datetime.strftime(random_datetime, '%Y-%m-%dT%H:%M:%SZ')
            self._cache['ChargePeriodStart'] = formatted_date

        return self._cache['ChargePeriodStart']

    @property
    def ChargePeriodEnd(self):
        # Only generate a new datetime when it's not in cache
        if 'ChargePeriodEnd' not in self._cache:
            # Generate a random datetime object within the last 30 days
            random_datetime = fake.date_time_between(start_date=self.thirty_days_ago, end_date=self.now, tzinfo=pytz.utc)
            formatted_date = datetime.strftime(random_datetime, '%Y-%m-%dT%H:%M:%SZ')
            self._cache['ChargePeriodEnd'] = formatted_date

        return self._cache['ChargePeriodEnd']

    @property
    def BilledCost(self):
        return self._cached_property('BilledCost', lambda: fake.pyfloat(left_digits=3, right_digits=2, positive=True))

    @property
    def AmortizedCost(self):
        return self._cached_property('AmortizedCost', lambda: fake.pyfloat(left_digits=3, right_digits=2, positive=True))

    def _cached_property(self, prop_name, generator_func):
        if prop_name not in self._cache:
            self._cache[prop_name] = generator_func()
        return self._cache[prop_name]
    
    def to_dict(self):
        return {
            'InvoiceIssuer': self.InvoiceIssuer,
            'ResourceID': self.ResourceID,
            'ChargeType': self.ChargeType,
            'Provider': self.Provider,
            'BillingAccountName': self.BillingAccountName,
            'SubAccountName': self.SubAccountName,
            'BillingAccountId': self.BillingAccountId,
            'Publisher': self.Publisher,
            'ResourceName': self.ResourceName,
            'ServiceName': self.ServiceName,
            'BilledCurrency': self.BilledCurrency,
            'BillingPeriodEnd': self.BillingPeriodEnd,
            'BillingPeriodStart': self.BillingPeriodStart,
            'Region': self.Region,
            'ServiceCategory': self.ServiceCategory,
            'ChargePeriodStart': self.ChargePeriodStart,
            'ChargePeriodEnd': self.ChargePeriodEnd,
            'BilledCost': self.BilledCost,
            'AmortizedCost': self.AmortizedCost
        }

    
    def get_aws_invoice_issuer(self):
            aws_entities = [
                'AWS Inc.', 'Amazon Web Services', 'AWS Marketplace', 
                'Amazon Data Services', 'AWS CloudFront', 'Amazon S3 Billing', 
                'Amazon EC2 Billing', 'AWS Lambda Billing'
            ]
            return str(random.choice(aws_entities))
    
    def get_charge_type(self):
            aws_entities = [
                'Adjustment', 'Purchase', 'Tax', 
                'Usage'
            ]
            return str(random.choice(aws_entities))

    def get_aws_publisher(self):
            publisher_types = ['Software', 'Service', 'Platform']
            publisher_suffix = random.choice(['Inc.', 'LLC', 'Ltd.', 'Group', 'Technologies', 'Solutions'])
            return f"{fake.company()} {random.choice(publisher_types)} {publisher_suffix}"

    def get_aws_resource_name(self):
            resource_types = ['i-', 'vol-', 'snap-', 'ami-', 'bucket-', 'db-']
            resource_prefix = random.choice(resource_types)
            resource_id = fake.hexify(text='^^^^^^^^', upper=False)
            return f'{resource_prefix}{resource_id}'

    def get_aws_service_category(self):
            aws_service_categories = [
                'AI and Machine Learning', 'Analytics', 'Business Applications', 'Compute', 'Databases', 'Developer Tools', 'Multicloud',
                'Identity', 'Integration', 'Internet of Things', 'Management and Governance', 'Media', 'Migration', 'Mobile', 'Networking',
                'Security', 'Storage', 'Web', 'Other'
            ]
            return random.choice(aws_service_categories)

    def get_aws_service_name(self):
            aws_services = [
                'Amazon EC2', 'Amazon S3', 'AWS Lambda', 'Amazon RDS', 
                'Amazon DynamoDB', 'Amazon VPC', 'Amazon Route 53', 
                'Amazon CloudFront', 'AWS Elastic Beanstalk', 'Amazon SNS', 
                'Amazon SQS', 'Amazon Redshift', 'AWS CloudFormation', 
                'AWS IAM', 'Amazon EBS', 'Amazon ECS', 'Amazon EKS', 
                'Amazon ElastiCache', 'AWS Fargate', 'AWS Glue'
            ]
            return random.choice(aws_services)

    def get_aws_region(self):
            aws_regions = [
                'us-east-1', 'us-west-1', 'us-west-2', 'eu-west-1', 'eu-central-1',
                'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1', 'ap-northeast-2',
                'ap-south-1', 'sa-east-1', 'ca-central-1', 'eu-north-1', 'eu-west-2',
                'eu-west-3', 'ap-east-1', 'me-south-1', 'af-south-1', 'eu-south-1'
            ]
            return random.choice(aws_regions)

def generate_fake_focus():
    return FakeFocus()

def write_focus_to_csv(focus, csv_writer):
    csv_writer.writerow(focus.to_dict())

@log_execution_time
def generate_and_write_fake_focuses(csv_filename, num_records):
    headers = FakeFocus().to_dict().keys()

    with open(csv_filename, 'w', newline='') as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=headers)
        csv_writer.writeheader()

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(generate_fake_focus) for _ in range(num_records)]
            for future in futures:
                focus = future.result()
                write_focus_to_csv(focus, csv_writer)