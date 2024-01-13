import functools
import polars as pl
from faker import Faker
import random
from datetime import datetime, timedelta
import pytz
import logging
import time

fake = Faker()

def get_aws_invoice_issuer(num_records):
    aws_entities = [
        'AWS Inc.', 'Amazon Web Services', 'AWS Marketplace', 
        'Amazon Data Services', 'AWS CloudFront', 'Amazon S3 Billing', 
        'Amazon EC2 Billing', 'AWS Lambda Billing'
    ]
    return [random.choice(aws_entities) for _ in range(num_records)]

# ... similar functions for other non-date attributes ...

def get_random_datetimes(num_records, start_date, end_date):
    return [fake.date_time_between(start_date=start_date, end_date=end_date, tzinfo=pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ') for _ in range(num_records)]

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

@log_execution_time
def generate_and_write_fake_focuses(csv_filename, num_records):
    now = datetime.now(pytz.utc)
    thirty_days_ago = now - timedelta(days=30)

    df = pl.DataFrame({
        'InvoiceIssuer': [random.choice([ 'AWS Inc.', 'Amazon Web Services', 'AWS Marketplace', 'Amazon Data Services', 
                                         'AWS CloudFront', 'Amazon S3 Billing', 'Amazon EC2 Billing', 'AWS Lambda Billing']) for _ in range(num_records)],
        'ResourceID': [fake.uuid4() for _ in range(num_records)],
        'ChargeType': [random.choice(['Adjustment', 'Purchase', 'Tax', 'Usage']) for _ in range(num_records)],
        'Provider': [fake.company() for _ in range(num_records)],
        'BillingAccountName': [fake.company() for _ in range(num_records)],
        'SubAccountName': get_random_datetimes(num_records, thirty_days_ago, now),
        'BillingAccountId': [fake.uuid4() for _ in range(num_records)],
        'Publisher': [f"{fake.company()} {random.choice(['Software', 'Service', 'Platform'])} {random.choice(['Inc.', 'LLC', 'Ltd.', 'Group', 'Technologies', 'Solutions'])}" for _ in range(num_records)],
        'ResourceName': [f"{random.choice(['i-', 'vol-', 'snap-', 'ami-', 'bucket-', 'db-'])}{fake.hexify(text='^^^^^^^^', upper=False)}" for _ in range(num_records)],
        'ServiceName': [random.choice([
            'Amazon EC2', 'Amazon S3', 'AWS Lambda', 'Amazon RDS', 
            'Amazon DynamoDB', 'Amazon VPC', 'Amazon Route 53', 
            'Amazon CloudFront', 'AWS Elastic Beanstalk', 'Amazon SNS', 
            'Amazon SQS', 'Amazon Redshift', 'AWS CloudFormation', 
            'AWS IAM', 'Amazon EBS', 'Amazon ECS', 'Amazon EKS', 
            'Amazon ElastiCache', 'AWS Fargate', 'AWS Glue'
        ]) for _ in range(num_records)],
        'BilledCurrency': ['USD' for _ in range(num_records)],
        'BillingPeriodEnd': get_random_datetimes(num_records, thirty_days_ago, now),
        'BillingPeriodStart': get_random_datetimes(num_records, thirty_days_ago, now),
        'Region': [random.choice([
            'us-east-1', 'us-west-1', 'us-west-2', 'eu-west-1', 'eu-central-1',
            'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1', 'ap-northeast-2',
            'ap-south-1', 'sa-east-1', 'ca-central-1', 'eu-north-1', 'eu-west-2',
            'eu-west-3', 'ap-east-1', 'me-south-1', 'af-south-1', 'eu-south-1'
        ]) for _ in range(num_records)],
        'ServiceCategory': [random.choice([
            'AI and Machine Learning', 'Analytics', 'Business Applications', 'Compute', 'Databases', 'Developer Tools', 'Multicloud',
            'Identity', 'Integration', 'Internet of Things', 'Management and Governance', 'Media', 'Migration', 'Mobile', 'Networking',
            'Security', 'Storage', 'Web', 'Other'
        ]) for _ in range(num_records)],
        'ChargePeriodStart': get_random_datetimes(num_records, thirty_days_ago, now),
        'ChargePeriodEnd': get_random_datetimes(num_records, thirty_days_ago, now),
        'BilledCost': [fake.pyfloat(left_digits=3, right_digits=2, positive=True) for _ in range(num_records)],
        'AmortizedCost': [fake.pyfloat(left_digits=3, right_digits=2, positive=True) for _ in range(num_records)]
    })

    df.write_csv(csv_filename)