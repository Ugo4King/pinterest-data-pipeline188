
**Pinterest Data Pipeline**

**Project Overview:**

The Pinterest Data Pipeline project is a meticulously crafted system aimed at extracting, storing, transforming, and analyzing emulated Pinterest post data. With an emphasis on robustness and efficiency, this project encompasses the creation of two intricately designed data pipelines: one tailored for batch processing and the other fine-tuned for real-time processing of streaming data. The core objective is to provide a hands-on learning experience while mastering a suite of cutting-edge data engineering tools and services.

**Key Components and Technologies:**
**Programming Language:**

**Python:** The project harnesses the power of Python for executing Pinterest posting emulation via AWS RDS queries. Additionally, it facilitates seamless interactions with Kafka and AWS Kinesis through meticulously crafted API requests.

**Data Ingestion:**

**Kafka (Using Amazon MSK):** Kafka stands at the forefront of data ingestion, gracefully handling the raw Pinterest data influx and proficiently routing it to designated topics within an S3 bucket. This setup ensures streamlined batch processing in Databricks.

**Step-by-Step Setup:**

1.  Provision Amazon MSK cluster through the AWS Management Console.
2.  Configure your MSK cluster settings, including the number of brokers, instance types, security settings, etc.
3.  Create topics within your MSK cluster to organize incoming data.
4.  Update Kafka configurations in your application code to connect to the provisioned MSK cluster.

**Infrastructure:**

**Amazon EC2:** The backbone of the Kafka client machine, Amazon EC2 plays a pivotal role in ensuring the seamless operation and integration of Kafka within the data pipeline ecosystem.

**Workflow Orchestration:**

**Apache Airflow (Using Amazon MWAA):** Task scheduling and orchestration are seamlessly managed by Apache Airflow, powered by Amazon MWAA. This dynamic duo ensures the timely execution of batch processing tasks, thereby orchestrating a symphony of data workflows with unparalleled precision.

**Real-time Data Processing:**

**Amazon Kinesis:** Real-time data processing reaches new heights with Amazon Kinesis, serving as the bedrock for ingesting raw Pinterest data as data streams. This enables lightning-fast real-time processing in Databricks, empowering stakeholders with actionable insights in the blink of an eye.

**API Gateway:**
**Amazon API Gateway:** The deployment of a robust REST API through Amazon API Gateway facilitates seamless communication between Amazon MSK and Kinesis. This ensures frictionless data flow and fosters a cohesive ecosystem conducive to innovation.

**Data Transformation and Analysis:**

**Databricks:** Databricks emerges as the undisputed champion for data cleaning, transformation, and analysis. With its prowess in both batch and real-time processing modes, Databricks executes SQL queries with finesse, unraveling key metrics and unveiling profound insights hidden within the vast expanse of processed data.

**Project Structure:**

1. **Data Extraction:**
Emulates Pinterest post data using meticulously crafted Python scripts, leveraging the power of AWS RDS queries to extract mission-critical information.

2. **Data Ingestion:**

**Batch Processing:** Harnesses the robust capabilities of Kafka (Amazon MSK) to ingest raw Pinterest data and seamlessly route it to designated topics within an S3 bucket, setting the stage for streamlined batch processing in Databricks.

**Real-time Processing:** Leverages the unparalleled efficiency of Amazon Kinesis for ingesting raw Pinterest data as data streams, fueling lightning-fast real-time processing in Databricks.

3. **Data Transformation:**

**Batch Processing:** Executes intricate data cleaning and transformation tasks in Databricks, laying the foundation for meticulous batch processing of extracted data.

**Real-time Processing:** Embarks on a journey of real-time data cleaning and transformation within Databricks, ensuring seamless processing of streaming data with unparalleled efficiency.

4. **Analysis and Insights:**
Unveils the power of SQL queries in Databricks, unraveling key metrics and unveiling profound insights from the processed data. This facilitates data-driven decision-making and empowers stakeholders with actionable insights.

5. **Task Scheduling and Orchestration:**
Seamlessly orchestrated by Apache Airflow (Amazon MWAA), batch processing tasks unfold with unparalleled precision, ensuring the timely execution of data workflows and fostering an environment of operational excellence.

**Setup and Deployment:**
Embarking on the journey to replicate and deploy the Pinterest Data Pipeline project entails a meticulous approach:

**Python Environment Setup:** Lay the groundwork by setting up a Python environment with the necessary dependencies for executing Pinterest posting emulation and facilitating interactions with Kafka and AWS Kinesis.

**Kafka (Using Amazon MSK):**
Kafka serves as a powerful data ingestion tool, and setting it up with Amazon Managed Streaming for Apache Kafka (Amazon MSK) is straightforward:

**Provision Amazon MSK Cluster:**

1. Navigate to the AWS Management Console and search for "MSK".
2. Click on "Create cluster" and follow the setup wizard.
3. Choose a suitable deployment mode (Single-broker, Multi-broker, or Dedicated Zookeeper).
4. Configure your cluster settings, including the number of brokers, instance types, security settings, etc.
5. Review and confirm your settings, then create the cluster.

**Create Kafka Topics:**

Once the cluster is provisioned, 
1. Navigate to the Amazon MSK console.
2. Select your cluster and navigate to the "Kafka settings" tab.
3. Click on "Create topic" and specify the topic name, number of partitions, replication factor, etc.
4. Create topics as needed to organize incoming data.

**Update Kafka Configurations:**

Update Kafka configurations in your application code to connect to the provisioned MSK cluster.
Configure producers to write data to the specified topics and consumers to read data from these topics.

**Amazon EC2:**

Amazon EC2 is used to set up the Kafka client machine for seamless operation and integration with Kafka:

***Launch EC2 Instance:***

1. Navigate to the AWS Management Console and search for "EC2".
2. Click on "Launch Instance" and choose an Amazon Machine Image (AMI) suitable for your requirements.
3. Select an instance type, configure instance details, add storage, configure security groups, etc.
4. Review and launch the instance.
(**NB:** for the purpose of this project we are using already provisioned EC2 so we only SSH into it)

**Install Kafka Client:**

1. SSH into the EC2 instance using your preferred SSH client.
2. Install Kafka client dependencies using package managers like apt or yum.
3. Configure Kafka client properties such as bootstrap servers, security settings, etc., in the server.properties file.

**Connect to Kafka Cluster:**

Use command-line tools like kafka-console-producer and kafka-console-consumer to produce and consume messages, respectively.
Connect to the Kafka cluster using the bootstrap servers provided in the Amazon MSK console.

**Apache Airflow (Using Amazon MWAA):**
Apache Airflow setup with Amazon Managed Workflows for Apache Airflow (MWAA) simplifies task scheduling and orchestration:

**Create MWAA Environment:**

1. Navigate to the Amazon MWAA console.
2. Click on "Create environment" and follow the setup wizard.
3. Choose a name for your environment, select a VPC and subnet, configure networking settings, etc.
4. Review and create the environment.

**Configure DAGs:**

1. Upload your DAG (Directed Acyclic Graph) Python scripts to the MWAA environment.
2. Define tasks and dependencies within your DAG scripts.
3. Use MWAA environment variables to pass configuration settings to your DAGs.

**Trigger DAG Runs:**

Once your DAGs are configured, trigger DAG runs manually or set up schedule intervals for automatic execution.
Monitor DAG runs and view logs through the MWAA console.

**Amazon Kinesis:**

Amazon Kinesis is used for real-time data ingestion, and setting it up involves a few simple steps:

**Create Kinesis Data Stream:**

1. Navigate to the Amazon Kinesis console.
2. Click on "Create data stream" and specify the stream name, number of shards, etc.
3. Create the data stream to start ingesting data.

**Amazon API Gateway:**

Amazon API Gateway simplifies the deployment of REST APIs, enabling seamless communication between different components:

**Create API Gateway API:**

1. Navigate to the Amazon API Gateway console.
2. Click on "Create API" and choose the REST API type.
3. Define your API resources, methods, request/response models, etc.

**Deploy API:**

Once your API is defined, 
1. deploy it to a stage (e.g., "dev", "prod") for testing and production use.
2. Generate an API endpoint URL that can be used to interact with your API.

**Integrate with Other Services:**

1. Integrate your API with other AWS services like Amazon MSK and Amazon Kinesis using AWS Lambda functions, HTTP integrations, etc.

2. Configure permissions and security settings to control access to your API resources.

**Databricks:**

Databricks is utilized for data cleaning, transformation, and analysis tasks in both batch and real-time processing modes:

**Provision Databricks Workspace:**

1. Navigate to the Azure Databricks portal.
2. Create a new workspace and configure the workspace settings, including the Azure region, pricing tier, etc.
3. Invite collaborators and set up access controls as needed.

**Create Clusters:**

1. Create one or more Databricks clusters with the desired configurations (e.g., instance type, auto-scaling settings, etc.).
2. Configure cluster access controls and permissions.
(**NB:** we are using an already created cluster)

**Develop and Execute Jobs:**

Develop notebooks for data cleaning, transformation, and analysis tasks using languages like Python.
Create jobs to schedule and execute these notebooks as batch processing tasks or set up streaming jobs for real-time processing.

Usage:
Once deployed, the Pinterest Data Pipeline project stands ready to revolutionize data processing and analysis:

**Data Emulation and Extraction:** Emulate Pinterest post data and extract mission-critical information using meticulously crafted Python scripts.

**Data Ingestion and Processing:** Ingest the extracted data into the system using either Kafka (for batch processing) or Amazon Kinesis (for real-time processing), fueling seamless processing and analysis.

**Data Transformation and Analysis:** Embark on a journey of data transformation and analysis within Databricks, executing intricate SQL queries to unravel key metrics and unveil profound insights from the processed data, empowering stakeholders with actionable insights and facilitating data-driven decision-making.

**Contributors:**


**License:**
This project is licensed under [License]. For more details, please refer to the LICENSE.md file included in the project repository.

Acknowledgments:
The Pinterest Data Pipeline project owes its success and innovation to the invaluable contributions and support from the following sources:


