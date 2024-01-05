import csv
import io
import random
from faker import Faker

fake = Faker()

class FakeFocus:
    def __init__(self):
       self.InvoiceIssuer = self.get_aws_invoice_issuer()
       self.ResourceID = fake.uuid4()
       self.ChargeType = fake.word()
       self.Provider = fake.company()
       self.BillingAccountName = fake.company()
       self.BillingAccountId = fake.uuid4()
       self.Publisher = self.get_aws_publisher()
       self.ResourceName = self.get_aws_resource_name()
       self.ServiceName = self.get_aws_service_name()
       self.BilledCurrency = fake.currency_code()
       self.BillingPeriodEnd = fake.date_time_this_month(before_now=True, after_now=False, tzinfo=None).isoformat()
       self.BillingPeriodStart = fake.date_time_this_month(before_now=True, after_now=False, tzinfo=None).isoformat()
       self.Region = self.get_aws_region()
       self.ServiceCategory = self.get_aws_service_category()
       self.ChargePeriodStart = fake.date_time_this_month(before_now=True, after_now=False, tzinfo=None).isoformat()
       self.ChargePeriodEnd = fake.date_time_this_month(before_now=True, after_now=False, tzinfo=None).isoformat()
       self.BilledCost = fake.pyfloat(left_digits=3,right_digits=2, positive=True)
       self.AmortizedCost = fake.pyfloat(left_digits=3,right_digits=2, positive=True)

    def get_aws_invoice_issuer(self):
            aws_entities = [
                'AWS Inc.', 'Amazon Web Services', 'AWS Marketplace', 
                'Amazon Data Services', 'AWS CloudFront', 'Amazon S3 Billing', 
                'Amazon EC2 Billing', 'AWS Lambda Billing'
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
                'Compute', 'Storage', 'Database', 'Networking & Content Delivery',
                'Machine Learning', 'Analytics', 'Security, Identity, & Compliance',
                'Developer Tools', 'Management & Governance', 'Media Services',
                'Machine Learning', 'AR & VR', 'Application Integration', 'Customer Engagement',
                'Business Applications', 'End User Computing', 'Internet of Things', 
                'Game Development', 'Blockchain', 'Robotics', 'Satellite'
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

def generate_fake_focus(num_records):
    fake_focuses = [FakeFocus() for _ in range(num_records)]
    return fake_focuses

def write_fake_focuses_to_csv (fake_focuses, csv_filename):
    headers = vars(fake_focuses[0]).keys()

    with open(csv_filename, 'w', newline='') as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=headers)
        csv_writer.writeheader()

        for focus in fake_focuses:
            csv_writer.writerow(vars(focus))


#Generate 1000 fake focuses to a CSV file
fake_focuses = generate_fake_focus(1000)

#Output to a CSV
csv_filename = 'fake_focuses.csv'
write_fake_focuses_to_csv(fake_focuses, csv_filename)

print(f"Fake focuses have been generated to {csv_filename}")