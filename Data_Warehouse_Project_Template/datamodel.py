import configparser
import boto3
import pandas as pd
import os
import csv
import json

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
config.read_file(open('aws.cfg'))

#CONFIGURATIONS
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

IAM_ROLE               = config.get('IAM_ROLE','ARN')
DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

#-----------------------------------------------------------------------------------
#CREATING A CLIENT FOR THE AWS RESOURCE
s3 = boto3.resource('s3',
    region_name = "us-east-2",
    aws_access_key_id = KEY,
    aws_secret_access_key = SECRET
)

redshift = boto3.client('redshift',
                        region_name = "us-east-2",
                        aws_access_key_id = KEY,
                        aws_secret_access_key = SECRET
                       )

iam = boto3.client('iam',
                    region_name = "us-east-2",
                    aws_access_key_id = KEY,
                    aws_secret_access_key = SECRET
    
                    )

ec2 = boto3.resource('ec2',
                     region_name = "us-east-2",
                     aws_access_key_id = KEY,
                     aws_secret_access_key = SECRET
                    )

#CHECKING OUT S3 BUCKET CALLED "udacity-dend"
# udaBucket = s3.Bucket('udacity-dend')
# print(udaBucket)

#-----------------------------------------------------------------------------------------------
# function that returns the object summary of an object in a bucket
def getObjectSummary(bucket,objectNumber):
    i = 0

    for each in bucket.objects.filter(Prefix = "song_data"):
        i += 1
        
        if (i == objectNumber):
            obj = each
            print(obj)
            break
    
    return obj
#print(getObjectSummary(udaBucket,6))

#getting bucket object
#udaObject = s3.Object("udacity-dend","song_data/A/A/A/TRAAACN128F9355673.json")
# print(udaObject)

#downloading object content to a file 
files = os.path.join(os.getcwd(),"songs.py")
# with open(files,"wb") as myFile:
#     udaObject.download_fileobj(myFile)



# #--------------------------------------------------------------------------------------------
# csvFile = os.path.join(os.getcwd(),"csvFile.csv")
# csv.register_dialect('noQuotes',quoting = csv.QUOTE_NONE)
# # with open (files,"r") as logFile:
# #     filed = []
# #     reader = csv.reader(logFile,dialect = 'noQuotes')
    
#     #for line in reader:
#         # filed.append(line)
#         # print(line)
#         # break
    

# df = pd.read_json(files, lines = True)
# print(df.head())
#------------------------------------------------------------------------------------------------------



#---------------CREATING AN IAM ROLE----------------------

#Function creates an iam role when called
def createIamRole():
    '''
    Creates an aws Iam role
    '''

    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement':{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {"Service": "redshift.amazonaws.com"}},
                'Version': '2012-10-17'})
        )
    

    except Exception as e:
        print(e)

#Function attaches policy to iam role when called 
def attachPolicy():
    iam.attach_role_policy(
    RoleName = DWH_IAM_ROLE_NAME,
    PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    print('Attaching Policy')

# Function gets ARN of iamrole
def getARN():
    # print('IAM role ARN:')
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']


    print(roleArn)
    return roleArn



#-------------CREATING AN AWS CLUSTER-----------------------
#funtion creates redshift cluster when called
def createCluster ():
    '''
    Creates a redshift cluster

    '''
    try:
        response = redshift.create_cluster(
            #add parameters for hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # add parameters for identifiers & credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            # add parameter for role (to allow s3 access)
            IamRoles=[getARN()]

        )
    except Exception as e:
        print(e)
# createCluster()

#Function returns a dataframe of the details of a cluster
def prettyRedshiftProps(props):
    '''
    Returns a DataFrame of details of an aws cluster

    '''
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print(prettyRedshiftProps(myClusterProps))


#Function opens an incoming TCP port   to allow access to the cluster endpoint
def openIncomingPort():
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        
        defaultSg.authorize_ingress(
            GroupName='default',
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
# openIncomingPort()




